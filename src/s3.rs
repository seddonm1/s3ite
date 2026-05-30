use std::{collections::HashSet, fmt::Write as _, ops::Not};

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use md5::{Digest, Md5};
use s3s::{
    S3, S3Error,
    S3ErrorCode::InternalError,
    S3Request, S3Response, S3Result, StdError,
    auth::Credentials,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput,
        CopyObjectOutput, CopyObjectResult, CopySource, CreateBucketInput, CreateBucketOutput,
        CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
        DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
        DeleteObjectsOutput, DeletedObject, ETag, EncodingType, GetBucketLocationInput,
        GetBucketLocationOutput, GetObjectAclInput, GetObjectAclOutput, GetObjectInput,
        GetObjectOutput, Grant, Grantee, HeadBucketInput, HeadBucketOutput, HeadObjectInput,
        HeadObjectOutput, ListBucketsInput, ListBucketsOutput, ListObjectVersionsInput,
        ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
        ListObjectsV2Output, ListPartsInput, ListPartsOutput, MetadataDirective, Object,
        ObjectVersion, ObjectVersionStorageClass, Owner, Part, Permission, PutObjectInput,
        PutObjectOutput, StreamingBlob, Timestamp, Type, UploadPartInput, UploadPartOutput,
    },
    s3_error,
};
use time::OffsetDateTime;
use tokio::{
    fs,
    io::{AsyncWrite, AsyncWriteExt},
};
use tracing::debug;
use uuid::Uuid;

use crate::{
    error::S3ite,
    sqlite::{KeySize, KeyValue, Multipart, MultipartUploadState, Sqlite},
};

const LIST_BATCH_SIZE: usize = 1024;
const CONTINUATION_TOKEN_PREFIX: &str = "s3ite:v1:";
const OWNER_DISPLAY_NAME: &str = "s3ite";
const OWNER_ID: &str = "s3ite";

/// Copies a streaming S3 request body into an async writer.
async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> std::result::Result<u64, S3ite>
where
    S: Stream<Item = std::result::Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = result.map_err(|_| S3ite::Copy)?;
        writer.write_all(&bytes).await?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await?;
    Ok(nwritten)
}

/// Encodes bytes as lowercase hexadecimal text.
fn hex(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let input = input.as_ref();
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Encodes bytes as standard base64 text.
fn base64(input: impl AsRef<[u8]>) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let input = input.as_ref();
    let mut encoded = String::with_capacity(input.len().div_ceil(3) * 4);

    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };

        encoded.push(char::from(TABLE[usize::from(b0 >> 2)]));
        encoded.push(char::from(
            TABLE[usize::from(((b0 & 0x03) << 4) | (b1 >> 4))],
        ));

        if chunk.len() > 1 {
            encoded.push(char::from(
                TABLE[usize::from(((b1 & 0x0f) << 2) | (b2 >> 6))],
            ));
        } else {
            encoded.push('=');
        }

        if chunk.len() > 2 {
            encoded.push(char::from(TABLE[usize::from(b2 & 0x3f)]));
        } else {
            encoded.push('=');
        }
    }

    encoded
}

/// Returns the common prefix produced by applying an S3 delimiter to a key.
fn common_prefix_for_key(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter.filter(|delimiter| delimiter.is_empty().not())?;
    let rest = key.strip_prefix(prefix)?;
    let delimiter_index = rest.find(delimiter)?;
    Some(format!(
        "{}{}",
        prefix,
        &rest[..delimiter_index + delimiter.len()]
    ))
}

/// Encodes a listing continuation token from the last scanned key.
fn encode_continuation_token(key: &str) -> String {
    format!("{CONTINUATION_TOKEN_PREFIX}{}", hex(key.as_bytes()))
}

/// Decodes a hexadecimal string into bytes.
fn decode_hex(value: &str) -> Option<Vec<u8>> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }

    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        decoded.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Some(decoded)
}

/// Decodes and validates a listing continuation token.
fn decode_continuation_token(token: &str) -> S3Result<String> {
    let encoded = token
        .strip_prefix(CONTINUATION_TOKEN_PREFIX)
        .ok_or_else(|| s3_error!(InvalidToken))?;
    let decoded = decode_hex(encoded).ok_or_else(|| s3_error!(InvalidToken))?;
    String::from_utf8(decoded).map_err(|_| s3_error!(InvalidToken))
}

/// Decodes a part `ETag` into its binary MD5 digest.
fn decode_md5_hex(value: &str) -> S3Result<[u8; 16]> {
    let decoded = decode_hex(value).ok_or_else(|| s3_error!(InvalidPart))?;
    decoded
        .try_into()
        .map_err(|_| s3_error!(InvalidPart, "invalid part etag"))
}

/// Returns the canonical owner used by s3ite ACL and listing responses.
fn owner() -> Owner {
    Owner {
        display_name: Some(OWNER_DISPLAY_NAME.to_string()),
        id: Some(OWNER_ID.to_string()),
    }
}

/// URL-encodes a list response value when requested by the client.
fn encode_list_value(value: String, encode: bool) -> String {
    if encode.not() {
        return value;
    }

    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(char::from(byte));
            }
            _ => {
                let _ = write!(&mut encoded, "%{byte:02X}");
            }
        }
    }
    encoded
}

/// Decodes S3 list input values that clients encode for XML-safe control bytes.
fn decode_list_input_value(value: String) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Some(hex) = decode_hex(&value[index + 1..index + 3])
            && matches!(hex.as_slice(), [b'\n' | b'\r' | b'\t'])
        {
            decoded.push(hex[0]);
            index += 3;
            continue;
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(decoded).unwrap_or(value)
}

/// Returns whether list output should use S3 URL encoding.
fn is_url_encoding(encoding_type: Option<&EncodingType>) -> bool {
    encoding_type.is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL)
}

/// Returns whether a query string contains a specific key-value pair.
fn has_query_value(query: Option<&str>, name: &str, value: &str) -> bool {
    query.is_some_and(|query| {
        query.split('&').any(|pair| {
            pair.split_once('=')
                .is_some_and(|(key, found)| key == name && found == value)
        })
    })
}

/// Normalizes a delimiter for list output.
fn output_delimiter(delimiter: Option<String>, encode: bool) -> Option<String> {
    delimiter.and_then(|delimiter| {
        delimiter
            .is_empty()
            .not()
            .then(|| encode_list_value(delimiter, encode))
    })
}

/// Validates that multipart state belongs to the request credentials.
fn validate_multipart_access(
    stored_access_key: Option<&String>,
    credentials: Option<&Credentials>,
) -> S3Result<()> {
    if stored_access_key == credentials.map(|credentials| &credentials.access_key) {
        Ok(())
    } else {
        Err(s3_error!(AccessDenied))
    }
}

/// Converts stored object metadata into an S3 list object.
fn object_from_key_size(
    key_size: KeySize,
    encode_keys: bool,
    owner: Option<&Owner>,
) -> S3Result<Object> {
    Ok(Object {
        key: Some(encode_list_value(key_size.key, encode_keys)),
        last_modified: Some(key_size.last_modified.into()),
        size: Some(i64::try_from(key_size.size).map_err(S3Error::internal_error)?),
        e_tag: key_size.md5.map(ETag::Strong),
        owner: owner.cloned(),
        ..Default::default()
    })
}

/// Converts stored object metadata into an S3 object-version entry.
fn object_version_from_key_size(
    key_size: KeySize,
    encode_keys: bool,
    owner: Option<&Owner>,
) -> S3Result<ObjectVersion> {
    Ok(ObjectVersion {
        e_tag: key_size.md5.map(ETag::Strong),
        is_latest: Some(true),
        key: Some(encode_list_value(key_size.key, encode_keys)),
        last_modified: Some(key_size.last_modified.into()),
        owner: owner.cloned(),
        size: Some(i64::try_from(key_size.size).map_err(S3Error::internal_error)?),
        storage_class: Some(ObjectVersionStorageClass::from_static(
            ObjectVersionStorageClass::STANDARD,
        )),
        version_id: Some("null".to_string()),
        ..Default::default()
    })
}

struct KeyListing {
    keys: Vec<KeySize>,
    common_prefixes: Vec<CommonPrefix>,
    key_count: usize,
    next_marker: Option<String>,
    is_truncated: bool,
}

impl Sqlite {
    /// Lists keys and common prefixes using S3 prefix, delimiter, and marker semantics.
    async fn list_keys(
        &self,
        bucket: &str,
        prefix_filter: &str,
        delimiter_filter: Option<&str>,
        mut after_key: Option<String>,
        max_keys: usize,
    ) -> S3Result<KeyListing> {
        let connection = self.try_get_connection(bucket).await?;
        let mut keys = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_common_prefixes = HashSet::new();
        let mut key_count = 0usize;
        let marker_filter = after_key.clone();
        let mut next_marker = None;
        let mut is_truncated = false;

        if max_keys > 0 {
            'pages: loop {
                let prefix = prefix_filter.to_string();
                let after_key_query = after_key.clone();
                let key_sizes = connection
                    .read(move |connection| {
                        let transaction = connection.transaction()?;
                        Ok(Self::try_list_objects(
                            &transaction,
                            Some(&prefix),
                            after_key_query.as_deref(),
                            LIST_BATCH_SIZE,
                        )?)
                    })
                    .await?;

                if key_sizes.is_empty() {
                    break;
                }

                let key_sizes_len = key_sizes.len();
                for key_size in key_sizes {
                    if prefix_filter.is_empty().not()
                        && key_size.key.starts_with(prefix_filter).not()
                    {
                        break 'pages;
                    }

                    if let Some(common_prefix) =
                        common_prefix_for_key(&key_size.key, prefix_filter, delimiter_filter)
                    {
                        if marker_filter
                            .as_ref()
                            .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                        {
                            after_key = Some(key_size.key);
                            continue;
                        }

                        if seen_common_prefixes.contains(&common_prefix) {
                            after_key = Some(key_size.key);
                            continue;
                        }

                        if key_count == max_keys {
                            is_truncated = true;
                            break 'pages;
                        }

                        seen_common_prefixes.insert(common_prefix.clone());
                        next_marker = Some(common_prefix.clone());
                        common_prefixes.push(CommonPrefix {
                            prefix: Some(common_prefix),
                        });
                        key_count += 1;
                        after_key = Some(key_size.key);
                        continue;
                    }

                    if key_count == max_keys {
                        is_truncated = true;
                        break 'pages;
                    }

                    after_key = Some(key_size.key.clone());
                    next_marker = Some(key_size.key.clone());
                    keys.push(key_size);
                    key_count += 1;
                }

                if key_sizes_len < LIST_BATCH_SIZE {
                    break;
                }
            }
        }

        Ok(KeyListing {
            keys,
            common_prefixes,
            key_count,
            next_marker,
            is_truncated,
        })
    }
}

#[async_trait::async_trait]
impl S3 for Sqlite {
    /// Creates a bucket database if it does not already exist.
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let CreateBucketInput { bucket, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        if self.buckets.read().await.contains_key(&bucket) {
            return Ok(S3Response::new(CreateBucketOutput::default()));
        }

        let file_path = self.get_bucket_path(&bucket)?;
        if file_path.exists() {
            return Ok(S3Response::new(CreateBucketOutput::default()));
        }

        self.try_create_bucket(&bucket, file_path)
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        debug!(bucket = %bucket, "created bucket");

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    /// Copies an object between buckets while applying S3 metadata-directive rules.
    #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let CopyObjectInput {
            bucket: tgt_bucket,
            key: tgt_key,
            cache_control,
            content_type,
            copy_source,
            expires,
            metadata,
            metadata_directive,
            ..
        } = req.input;

        self.validate_mutable_bucket(&tgt_bucket)?;

        let (src_bucket, src_key) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { bucket, key, .. } => (bucket, key),
        };
        let replace_metadata = metadata_directive
            .as_ref()
            .is_some_and(|directive| directive.as_str() == MetadataDirective::REPLACE);
        if src_bucket.as_ref() == tgt_bucket.as_str()
            && src_key.as_ref() == tgt_key.as_str()
            && replace_metadata.not()
        {
            return Err(s3_error!(
                InvalidRequest,
                "copy source must be different from target unless replacing metadata"
            ));
        }

        // verify source and target buckets exist
        let connection = self.try_get_connection(&src_bucket).await?;

        let mut object = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Ok(Self::try_get_object(&transaction, &src_key)?
                    .ok_or_else(|| s3_error!(NoSuchKey))?)
            })
            .await?;

        // replace key with target key
        object.key = tgt_key.clone();
        if replace_metadata {
            object.metadata = metadata;
            object.content_type = content_type;
            object.cache_control = cache_control;
            object.expires = expires.map(Into::into);
        }

        let copy_object_result = CopyObjectResult {
            e_tag: object.md5.clone().map(ETag::Strong),
            last_modified: Some(object.last_modified.into()),
            ..Default::default()
        };

        let connection = self.try_get_connection(&tgt_bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_put_object(&transaction, object)?;
                Ok(transaction.commit()?)
            })
            .await?;

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Deletes an empty bucket database and its `SQLite` sidecar files.
    #[tracing::instrument]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let DeleteBucketInput { bucket, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let connection = self.try_get_connection(&bucket).await?;
        let has_objects = connection
            .read(|connection| {
                let transaction = connection.transaction()?;
                Ok(Self::try_has_objects(&transaction)?)
            })
            .await?;
        if has_objects {
            return Err(s3_error!(BucketNotEmpty));
        }

        let connection = self
            .buckets
            .write()
            .await
            .remove(&bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;

        connection.close().await?;

        let bucket_path = self.get_bucket_path(&bucket)?;
        fs::remove_file(&bucket_path)
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
        fs::remove_file(format!("{}-wal", bucket_path.to_string_lossy()))
            .await
            .ok();
        fs::remove_file(format!("{}-shm", bucket_path.to_string_lossy()))
            .await
            .ok();

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    /// Deletes an object by key.
    #[tracing::instrument]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let DeleteObjectInput { bucket, key, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let connection = self.try_get_connection(&bucket).await?;

        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_delete_object(&transaction, &key)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                Ok(transaction.commit()?)
            })
            .await?;

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    /// Deletes a batch of objects and returns S3 multi-delete results.
    #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let quiet = delete.quiet.unwrap_or(false);
        let delete_keys = delete
            .objects
            .into_iter()
            .map(|object| object.key)
            .collect::<Vec<_>>();
        if delete_keys.len() > 1000 {
            return Err(s3_error!(
                MalformedXML,
                "DeleteObjects supports at most 1000 keys"
            ));
        }

        let connection = self.try_get_connection(&bucket).await?;

        let deleted_keys = delete_keys.clone();
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_delete_objects(&transaction, &delete_keys)?;
                transaction.commit()?;
                Ok(())
            })
            .await?;

        let output = DeleteObjectsOutput {
            deleted: quiet.not().then(|| {
                deleted_keys
                    .into_iter()
                    .map(|key| DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>()
            }),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Returns the default bucket location for an existing bucket.
    #[tracing::instrument]
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let GetBucketLocationInput { bucket, .. } = req.input;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let file_path = self.get_bucket_path(&bucket)?;
        if file_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    /// Returns an object body and metadata, applying byte-range requests when present.
    #[tracing::instrument]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let GetObjectInput {
            bucket, key, range, ..
        } = req.input;

        let connection = self.try_get_connection(&bucket).await?;

        let object =
            connection
                .read(move |connection| {
                    let transaction = connection.transaction()?;
                    Ok(Self::try_get_object(&transaction, &key)?
                        .ok_or_else(|| s3_error!(NoSuchKey))?)
                })
                .await?;

        let object_range = match range.as_ref() {
            None => 0..object.size,
            Some(range) => {
                let object_range = range.check(object.size)?;
                if object_range.is_empty() {
                    return Err(s3_error!(InvalidRange));
                }
                object_range
            }
        };
        let content_length = object_range.end - object_range.start;
        let content_length_i64 = try_!(i64::try_from(content_length));

        let value = match object.value {
            Some(value) => value,
            None if object.size == 0 => Vec::new(),
            None => {
                return Err(S3Error::with_message(
                    InternalError,
                    "object metadata has nonzero size but no value",
                ));
            }
        };

        let start = try_!(usize::try_from(object_range.start));
        let end = try_!(usize::try_from(object_range.end));
        let Some(value) = value.get(start..end) else {
            return Err(S3Error::with_message(
                InternalError,
                "object metadata size exceeds stored value length",
            ));
        };
        let value = Bytes::copy_from_slice(value);
        let body = stream::once(async { Ok(value) });

        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap::<_, S3Error>(body)),
            accept_ranges: Some("bytes".to_string()),
            cache_control: object.cache_control,
            content_range: range.is_some().then(|| {
                format!(
                    "bytes {}-{}/{}",
                    object_range.start,
                    object_range.end - 1,
                    object.size
                )
            }),
            content_length: Some(content_length_i64),
            content_type: object.content_type,
            expires: object.expires.map(Into::into),
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            e_tag: object.md5.map(ETag::Strong),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Checks whether a bucket exists.
    #[tracing::instrument]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let HeadBucketInput { bucket, .. } = req.input;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let file_path = self.get_bucket_path(&bucket)?;
        if file_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }

        Ok(S3Response::new(HeadBucketOutput {
            access_point_alias: None,
            bucket_location_name: None,
            bucket_location_type: None,
            bucket_region: None,
        }))
    }

    /// Returns object metadata without reading the object body.
    #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let HeadObjectInput { bucket, key, .. } = req.input;

        let connection = self.try_get_connection(&bucket).await?;

        let object = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Ok(Self::try_get_metadata(&transaction, &key)?
                    .ok_or_else(|| s3_error!(NoSuchKey))?)
            })
            .await?;

        // TODO: detect content type
        let content_type = object
            .content_type
            .unwrap_or_else(|| mime::APPLICATION_OCTET_STREAM.to_string());

        let output = HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            cache_control: object.cache_control,
            content_length: Some(try_!(i64::try_from(object.size))),
            content_type: Some(content_type),
            e_tag: object.md5.map(ETag::Strong),
            expires: object.expires.map(Into::into),
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Returns the fixed owner ACL for an existing object.
    #[tracing::instrument]
    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        let GetObjectAclInput { bucket, key, .. } = req.input;

        let connection = self.try_get_connection(&bucket).await?;
        connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_get_metadata(&transaction, &key)?.ok_or_else(|| s3_error!(NoSuchKey))?;
                Ok(())
            })
            .await?;

        let owner = owner();
        let output = GetObjectAclOutput {
            grants: Some(vec![Grant {
                grantee: Some(Grantee {
                    display_name: owner.display_name.clone(),
                    email_address: None,
                    id: owner.id.clone(),
                    type_: Type::from_static(Type::CANONICAL_USER),
                    uri: None,
                }),
                permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
            }]),
            owner: Some(owner),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    /// Lists all open bucket databases.
    #[tracing::instrument]
    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let ListBucketsInput {
            bucket_region: _,
            continuation_token: _,
            max_buckets: _,
            prefix: _,
        } = req.input;

        let mut buckets: Vec<Bucket> = Vec::new();

        for name in self.buckets.read().await.keys() {
            let file_path = self.get_bucket_path(name)?;
            let file_meta = fs::metadata(file_path)
                .await
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
            let created_or_modified_date =
                Timestamp::from(try_!(file_meta.created().or(file_meta.modified())));

            let bucket = Bucket {
                bucket_region: None,
                creation_date: Some(created_or_modified_date),
                name: Some(name.clone()),
            };
            buckets.push(bucket);
        }

        let output = ListBucketsOutput {
            continuation_token: None,
            buckets: Some(buckets),
            owner: None,
            prefix: None,
        };
        Ok(S3Response::new(output))
    }

    /// Lists objects using the original `ListObjects` marker protocol.
    #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let allow_unordered = has_query_value(req.uri.query(), "allow-unordered", "true");
        let ListObjectsInput {
            bucket,
            mut prefix,
            max_keys,
            mut marker,
            mut delimiter,
            encoding_type,
            ..
        } = req.input;

        prefix = prefix.map(decode_list_input_value);
        marker = marker.map(decode_list_input_value);
        delimiter = delimiter.map(decode_list_input_value);

        let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));
        let marker_output = Some(marker.clone().unwrap_or_default());
        let prefix_filter = prefix.clone().unwrap_or_default();
        let encode_keys = is_url_encoding(encoding_type.as_ref());
        let delimiter_filter = delimiter
            .as_deref()
            .filter(|delimiter| delimiter.is_empty().not())
            .map(str::to_string);
        if allow_unordered && delimiter_filter.is_some() {
            return Err(s3_error!(
                InvalidArgument,
                "allow-unordered cannot be used with delimiter"
            ));
        }
        let delimiter = output_delimiter(delimiter, encode_keys);
        let owner = owner();

        let listing = self
            .list_keys(
                &bucket,
                &prefix_filter,
                delimiter_filter.as_deref(),
                marker.clone(),
                max_keys_usize,
            )
            .await?;
        let objects = listing
            .keys
            .into_iter()
            .map(|key_size| object_from_key_size(key_size, encode_keys, Some(&owner)))
            .collect::<S3Result<Vec<_>>>()?;
        let next_marker = listing
            .is_truncated
            .then(|| {
                listing
                    .next_marker
                    .clone()
                    .map(|marker| encode_list_value(marker, encode_keys))
            })
            .flatten();

        let output = ListObjectsOutput {
            contents: Some(objects),
            common_prefixes: delimiter.as_ref().map(|_| {
                listing
                    .common_prefixes
                    .into_iter()
                    .map(|common_prefix| CommonPrefix {
                        prefix: common_prefix
                            .prefix
                            .map(|prefix| encode_list_value(prefix, encode_keys)),
                    })
                    .collect::<Vec<_>>()
            }),
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix: prefix.map(|prefix| encode_list_value(prefix, encode_keys)),
            max_keys: Some(max_keys),
            is_truncated: Some(next_marker.is_some()),
            marker: marker_output,
            next_marker,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    /// Lists unversioned objects using the `ListObjectVersions` response shape.
    #[tracing::instrument]
    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            mut prefix,
            max_keys,
            mut key_marker,
            version_id_marker,
            mut delimiter,
            encoding_type,
            ..
        } = req.input;

        prefix = prefix.map(decode_list_input_value);
        key_marker = key_marker.map(decode_list_input_value);
        delimiter = delimiter.map(decode_list_input_value);

        let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));
        let key_marker_clone = key_marker.clone();
        let version_id_marker_clone = version_id_marker.clone();
        let prefix_filter = prefix.clone().unwrap_or_default();
        let encode_keys = is_url_encoding(encoding_type.as_ref());
        let delimiter_filter = delimiter
            .as_deref()
            .filter(|delimiter| delimiter.is_empty().not())
            .map(str::to_string);
        let delimiter = output_delimiter(delimiter, encode_keys);
        let owner = owner();

        let listing = self
            .list_keys(
                &bucket,
                &prefix_filter,
                delimiter_filter.as_deref(),
                key_marker.clone(),
                max_keys_usize,
            )
            .await?;
        let versions = listing
            .keys
            .into_iter()
            .map(|key_size| object_version_from_key_size(key_size, encode_keys, Some(&owner)))
            .collect::<S3Result<Vec<_>>>()?;

        let next_key_marker = listing
            .is_truncated
            .then(|| {
                listing
                    .next_marker
                    .clone()
                    .map(|marker| encode_list_value(marker, encode_keys))
            })
            .flatten();
        let next_version_id_marker = next_key_marker.as_ref().map(|_| "null".to_string());

        let output = ListObjectVersionsOutput {
            common_prefixes: delimiter.as_ref().map(|_| {
                listing
                    .common_prefixes
                    .into_iter()
                    .map(|common_prefix| CommonPrefix {
                        prefix: common_prefix
                            .prefix
                            .map(|prefix| encode_list_value(prefix, encode_keys)),
                    })
                    .collect::<Vec<_>>()
            }),
            delete_markers: Some(Vec::new()),
            delimiter,
            encoding_type,
            is_truncated: Some(next_key_marker.is_some()),
            key_marker: key_marker_clone,
            max_keys: Some(max_keys),
            name: Some(bucket),
            next_key_marker,
            next_version_id_marker,
            prefix: prefix.map(|prefix| encode_list_value(prefix, encode_keys)),
            version_id_marker: version_id_marker_clone,
            versions: Some(versions),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    /// Lists objects using the `ListObjectsV2` continuation-token protocol.
    #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let allow_unordered = has_query_value(req.uri.query(), "allow-unordered", "true");
        let ListObjectsV2Input {
            bucket,
            mut prefix,
            max_keys,
            mut start_after,
            mut delimiter,
            encoding_type,
            continuation_token,
            fetch_owner,
            ..
        } = req.input;

        prefix = prefix.map(decode_list_input_value);
        start_after = start_after.map(decode_list_input_value);
        delimiter = delimiter.map(decode_list_input_value);

        let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));
        let continuation_token_clone = continuation_token.clone();
        let prefix_filter = prefix.clone().unwrap_or_default();
        let encode_keys = is_url_encoding(encoding_type.as_ref());
        let delimiter_filter = delimiter
            .as_deref()
            .filter(|delimiter| delimiter.is_empty().not())
            .map(str::to_string);
        if allow_unordered && delimiter_filter.is_some() {
            return Err(s3_error!(
                InvalidArgument,
                "allow-unordered cannot be used with delimiter"
            ));
        }
        let delimiter = output_delimiter(delimiter, encode_keys);
        let owner = owner();
        let owner = fetch_owner.unwrap_or(false).then_some(owner);
        let after_key = match continuation_token
            .as_deref()
            .filter(|continuation_token| continuation_token.is_empty().not())
        {
            Some(continuation_token) => Some(decode_continuation_token(continuation_token)?),
            None => start_after.clone(),
        };

        let listing = self
            .list_keys(
                &bucket,
                &prefix_filter,
                delimiter_filter.as_deref(),
                after_key,
                max_keys_usize,
            )
            .await?;
        let objects = listing
            .keys
            .into_iter()
            .map(|key_size| object_from_key_size(key_size, encode_keys, owner.as_ref()))
            .collect::<S3Result<Vec<_>>>()?;

        let next_continuation_token = listing
            .is_truncated
            .then(|| {
                listing
                    .next_marker
                    .as_deref()
                    .map(encode_continuation_token)
            })
            .flatten();

        let key_count = try_!(i32::try_from(listing.key_count));

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(max_keys),
            continuation_token: continuation_token_clone,
            is_truncated: Some(next_continuation_token.is_some()),
            contents: Some(objects),
            common_prefixes: delimiter.as_ref().map(|_| {
                listing
                    .common_prefixes
                    .into_iter()
                    .map(|common_prefix| CommonPrefix {
                        prefix: common_prefix
                            .prefix
                            .map(|prefix| encode_list_value(prefix, encode_keys)),
                    })
                    .collect::<Vec<_>>()
            }),
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix: prefix.map(|prefix| encode_list_value(prefix, encode_keys)),
            start_after: start_after.map(|start_after| encode_list_value(start_after, encode_keys)),
            next_continuation_token,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    /// Stores an object body and metadata.
    #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let PutObjectInput {
            body,
            bucket,
            key,
            metadata,
            cache_control,
            content_length: _,
            content_md5,
            content_type,
            expires,
            storage_class,
            ..
        } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        if let Some(ref storage_class) = storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if is_valid.not() {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let connection = self.try_get_connection(&bucket).await?;

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        let size = copy_bytes(stream, &mut value).await?;
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);

        // if provided verify content_md5
        if let Some(content_md5) = content_md5
            && content_md5 != base64(md5_bytes)
        {
            return Err(s3_error!(BadDigest));
        }

        debug!(path = %key, ?size, %md5, "write file");

        let md5_clone = md5.clone();
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_put_object(
                    &transaction,
                    KeyValue {
                        key,
                        value: Some(value),
                        size,
                        metadata,
                        content_type,
                        cache_control,
                        expires: expires.map(Into::into),
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5_clone),
                    },
                )?;
                Ok(transaction.commit()?)
            })
            .await?;

        let output = PutObjectOutput {
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Creates multipart upload state for a future object.
    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput {
            bucket,
            key,
            cache_control,
            content_type,
            expires,
            metadata,
            ..
        } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let upload_id = Uuid::new_v4();

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_create_multipart_upload(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    MultipartUploadState {
                        access_key: req.credentials.map(|credentials| credentials.access_key),
                        metadata,
                        content_type,
                        cache_control,
                        expires: expires.map(Into::into),
                    },
                )?;
                Ok(transaction.commit()?)
            })
            .await?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    /// Stores or replaces a multipart upload part.
    #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            bucket,
            key,
            upload_id,
            part_number,
            content_md5,
            ..
        } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        copy_bytes(stream, &mut value).await?;
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);
        let size = try_!(i64::try_from(value.len()));

        // if provided verify content_md5
        if let Some(content_md5) = content_md5
            && content_md5 != base64(md5_bytes)
        {
            return Err(s3_error!(BadDigest));
        }

        let md5_clone = md5.clone();
        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;

                let upload_state =
                    Self::try_get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                        .ok_or_else(|| s3_error!(NoSuchUpload))?;
                validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials.as_ref(),
                )?;

                Self::try_put_multipart(
                    &transaction,
                    Multipart {
                        upload_id,
                        part_number,
                        last_modified: OffsetDateTime::now_utc(),
                        value,
                        size,
                        md5: Some(md5_clone),
                    },
                )?;

                Ok(transaction.commit()?)
            })
            .await?;

        let output = UploadPartOutput {
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Lists parts uploaded for an active multipart upload.
    #[tracing::instrument]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self.try_get_connection(&bucket).await?;
        let parts = connection
            .read(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                let upload_state = Self::try_get_multipart_upload_state(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .ok_or_else(|| s3_error!(NoSuchUpload))?;
                validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials.as_ref(),
                )?;

                let parts = Self::try_list_multipart(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                drop(transaction);

                Ok(parts)
            })
            .await?;

        let parts = parts
            .into_iter()
            .map(|part| Part {
                last_modified: Some(part.last_modified.into()),
                part_number: Some(part.part_number),
                size: Some(part.size),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id.to_string()),
            parts: Some(parts),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Aborts an active multipart upload and deletes its parts.
    #[tracing::instrument]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(NoSuchUpload))?;

        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let upload_state =
                    Self::try_get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                        .ok_or_else(|| s3_error!(NoSuchUpload))?;
                validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials.as_ref(),
                )?;

                let deleted = Self::try_delete_multipart(&transaction, upload_id)?;
                if deleted == 0 {
                    return Err(s3_error!(NoSuchUpload).into());
                }

                transaction.commit()?;
                Ok(())
            })
            .await?;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    /// Completes a multipart upload and records the result for idempotent retries.
    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let requested_parts = multipart_upload.parts.unwrap_or_default();
        if requested_parts.is_empty() {
            return Err(s3_error!(InvalidPart, "You must specify at least one part"));
        }

        let mut previous_part_number = 0;
        let requested_parts = requested_parts
            .into_iter()
            .map(|part| {
                let part_number = part.part_number.ok_or_else(|| s3_error!(InvalidPart))?;
                if part_number <= previous_part_number {
                    return Err(s3_error!(InvalidPartOrder));
                }
                previous_part_number = part_number;

                let e_tag = part
                    .e_tag
                    .and_then(ETag::into_strong)
                    .ok_or_else(|| s3_error!(InvalidPart))?;

                Ok((part_number, e_tag))
            })
            .collect::<S3Result<Vec<_>>>()?;

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self.try_get_connection(&bucket).await?;
        let md5 = connection
            .write(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                let Some(upload_state) = Self::try_get_multipart_upload_state(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                else {
                    let completed = Self::try_get_completed_multipart_upload(
                        &transaction,
                        upload_id,
                        &bucket_clone,
                        &key_clone,
                    )
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                    .ok_or_else(|| s3_error!(NoSuchUpload))?;
                    validate_multipart_access(
                        completed.access_key.as_ref(),
                        req.credentials.as_ref(),
                    )?;
                    return Ok(completed.md5);
                };
                validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials.as_ref(),
                )?;

                let parts = Self::try_get_multiparts(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                if parts.len() != requested_parts.len() {
                    return Err(s3_error!(InvalidPart).into());
                }

                let total_size = parts.iter().try_fold(0usize, |total, part| {
                    let part_size = usize::try_from(part.size)?;
                    total.checked_add(part_size).ok_or_else(|| {
                        S3ite::S3(S3Error::with_message(
                            InternalError,
                            "multipart object is too large",
                        ))
                    })
                })?;

                let mut value = Vec::with_capacity(total_size);
                let mut part_md5s = Vec::with_capacity(parts.len() * 16);
                let final_part_number = requested_parts.len();
                for (index, ((requested_part_number, requested_etag), part)) in
                    requested_parts.iter().zip(parts).enumerate()
                {
                    if part.part_number != *requested_part_number {
                        return Err(s3_error!(InvalidPart).into());
                    }

                    if index + 1 != final_part_number && part.size < 5 * 1024 * 1024 {
                        return Err(s3_error!(EntityTooSmall).into());
                    }

                    let Some(stored_md5) = part.md5.as_deref() else {
                        return Err(s3_error!(InvalidPart).into());
                    };
                    if requested_etag != stored_md5 {
                        return Err(s3_error!(InvalidPart).into());
                    }

                    part_md5s.extend_from_slice(&decode_md5_hex(stored_md5)?);
                    value.extend_from_slice(&part.value);
                }

                let mut md5_hash = Md5::new();
                md5_hash.update(&part_md5s);
                let md5 = format!("{}-{}", hex(md5_hash.finalize()), requested_parts.len());
                let size = try_!(u64::try_from(value.len()));

                Self::try_put_object(
                    &transaction,
                    KeyValue {
                        key: key_clone.clone(),
                        value: Some(value),
                        size,
                        metadata: upload_state.metadata,
                        content_type: upload_state.content_type,
                        cache_control: upload_state.cache_control,
                        expires: upload_state.expires,
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5.clone()),
                    },
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                Self::try_delete_multipart(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                Self::try_put_completed_multipart_upload(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials.map(|credentials| credentials.access_key),
                    &md5,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                transaction
                    .commit()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                Ok(md5)
            })
            .await?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
