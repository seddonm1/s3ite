use std::{collections::HashSet, ops::Not};

use bytes::Bytes;
use futures::{stream, TryStreamExt};
use md5::{Digest, Md5};
use s3s::{
    dto::*, s3_error, S3Error, S3ErrorCode::InternalError, S3Request, S3Response, S3Result, S3,
};
use time::OffsetDateTime;
use tokio::fs;
use tracing::debug;
use uuid::Uuid;

use crate::{
    error::*,
    sqlite::{KeySize, KeyValue, Multipart, Sqlite},
    utils::{base64, copy_bytes, hex},
};

const LIST_BATCH_SIZE: usize = 1024;
const CONTINUATION_TOKEN_PREFIX: &str = "s3ite:v1:";

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

fn encode_continuation_token(key: &str) -> String {
    format!("{CONTINUATION_TOKEN_PREFIX}{}", hex(key.as_bytes()))
}

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

fn decode_continuation_token(token: &str) -> S3Result<String> {
    let encoded = token
        .strip_prefix(CONTINUATION_TOKEN_PREFIX)
        .ok_or_else(|| s3_error!(InvalidToken))?;
    let decoded = decode_hex(encoded).ok_or_else(|| s3_error!(InvalidToken))?;
    String::from_utf8(decoded).map_err(|_| s3_error!(InvalidToken))
}

fn decode_md5_hex(value: &str) -> S3Result<[u8; 16]> {
    let decoded = decode_hex(value).ok_or_else(|| s3_error!(InvalidPart))?;
    decoded
        .try_into()
        .map_err(|_| s3_error!(InvalidPart, "invalid part etag"))
}

fn object_from_key_size(key_size: KeySize) -> S3Result<Object> {
    Ok(Object {
        key: Some(key_size.key),
        last_modified: Some(key_size.last_modified.into()),
        size: Some(i64::try_from(key_size.size).map_err(S3Error::internal_error)?),
        e_tag: key_size.md5.map(ETag::Strong),
        ..Default::default()
    })
}

fn object_version_from_key_size(key_size: KeySize) -> S3Result<ObjectVersion> {
    Ok(ObjectVersion {
        e_tag: key_size.md5.map(ETag::Strong),
        is_latest: Some(true),
        key: Some(key_size.key),
        last_modified: Some(key_size.last_modified.into()),
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
    next_after_key: Option<String>,
    is_truncated: bool,
}

impl Sqlite {
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
                        if seen_common_prefixes.contains(&common_prefix) {
                            after_key = Some(key_size.key);
                            continue;
                        }

                        if key_count == max_keys {
                            is_truncated = true;
                            break 'pages;
                        }

                        seen_common_prefixes.insert(common_prefix.clone());
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
            next_after_key: after_key,
            is_truncated,
        })
    }
}

#[async_trait::async_trait]
impl S3 for Sqlite {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let CreateBucketInput { bucket, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        if self.buckets.read().await.contains_key(&bucket) {
            return Err(s3_error!(BucketAlreadyExists));
        }

        let file_path = self.get_bucket_path(&bucket)?;
        if file_path.exists() {
            return Err(s3_error!(BucketAlreadyExists));
        }

        self.try_create_bucket(&bucket, file_path)
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        debug!(bucket = %bucket, "created bucket");

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let CopyObjectInput {
            bucket: tgt_bucket,
            key: tgt_key,
            copy_source,
            ..
        } = req.input;

        self.validate_mutable_bucket(&tgt_bucket)?;

        let (src_bucket, src_key) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { bucket, key, .. } => (bucket, key),
        };

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

    #[tracing::instrument]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let DeleteBucketInput { bucket, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

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

                // if is directory
                if key.ends_with('/') {
                    let rows_affected = Self::try_delete_objects_like(&transaction, &key)
                        .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                    if rows_affected > 1 {
                        return Err(s3_error!(BucketNotEmpty).into());
                    }
                } else {
                    let rows_affected = Self::try_delete_object(&transaction, &key)
                        .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                    if rows_affected != 1 {
                        return Err(s3_error!(NoSuchKey).into());
                    }
                }

                Ok(transaction.commit()?)
            })
            .await?;

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let delete_keys = delete
            .objects
            .into_iter()
            .map(|object| object.key)
            .collect::<Vec<_>>();

        let connection = self.try_get_connection(&bucket).await?;

        let affected_keys = connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let affected_keys = Self::try_delete_objects(&transaction, &delete_keys)?;
                transaction.commit()?;
                Ok(affected_keys)
            })
            .await?;

        let output = DeleteObjectsOutput {
            deleted: Some(
                affected_keys
                    .into_iter()
                    .map(|key| DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>(),
            ),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

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
            content_range: range.is_some().then(|| {
                format!(
                    "bytes {}-{}/{}",
                    object_range.start,
                    object_range.end - 1,
                    object.size
                )
            }),
            content_length: Some(content_length_i64),
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            e_tag: object.md5.map(ETag::Strong),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

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
        let content_type = mime::APPLICATION_OCTET_STREAM;

        let output = HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(try_!(i64::try_from(object.size))),
            content_type: Some(content_type.to_string()),
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

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

    #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let marker = req.input.marker.clone();

        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| {
            let next_marker = if v2.is_truncated.unwrap_or(false) {
                v2.contents
                    .as_ref()
                    .and_then(|contents| contents.last().and_then(|last| last.key.clone()))
                    .into_iter()
                    .chain(v2.common_prefixes.as_ref().and_then(|common_prefixes| {
                        common_prefixes
                            .last()
                            .and_then(|common_prefix| common_prefix.prefix.clone())
                    }))
                    .max()
            } else {
                None
            };

            ListObjectsOutput {
                contents: v2.contents,
                common_prefixes: v2.common_prefixes,
                delimiter: v2.delimiter,
                encoding_type: v2.encoding_type,
                name: v2.name,
                prefix: v2.prefix,
                max_keys: v2.max_keys,
                is_truncated: v2.is_truncated,
                marker,
                next_marker,
                ..Default::default()
            }
        }))
    }

    #[tracing::instrument]
    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            prefix,
            max_keys,
            key_marker,
            version_id_marker,
            delimiter,
            encoding_type,
            ..
        } = req.input;

        let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));
        let key_marker_clone = key_marker.clone();
        let version_id_marker_clone = version_id_marker.clone();
        let prefix_filter = prefix.clone().unwrap_or_default();
        let delimiter_filter = delimiter.clone();

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
            .map(object_version_from_key_size)
            .collect::<S3Result<Vec<_>>>()?;

        let next_key_marker = listing
            .is_truncated
            .then(|| listing.next_after_key.clone())
            .flatten();
        let next_version_id_marker = next_key_marker.as_ref().map(|_| "null".to_string());

        let output = ListObjectVersionsOutput {
            common_prefixes: delimiter.as_ref().map(|_| listing.common_prefixes),
            delete_markers: Some(Vec::new()),
            delimiter,
            encoding_type,
            is_truncated: Some(next_key_marker.is_some()),
            key_marker: key_marker_clone,
            max_keys: Some(max_keys),
            name: Some(bucket),
            next_key_marker,
            next_version_id_marker,
            prefix,
            version_id_marker: version_id_marker_clone,
            versions: Some(versions),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input {
            bucket,
            prefix,
            max_keys,
            start_after,
            delimiter,
            encoding_type,
            continuation_token,
            ..
        } = req.input;

        let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));
        let continuation_token_clone = continuation_token.clone();
        let prefix_filter = prefix.clone().unwrap_or_default();
        let delimiter_filter = delimiter.clone();
        let after_key = match continuation_token.as_deref() {
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
            .map(object_from_key_size)
            .collect::<S3Result<Vec<_>>>()?;

        let next_continuation_token = listing
            .is_truncated
            .then(|| {
                listing
                    .next_after_key
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
            common_prefixes: delimiter.as_ref().map(|_| listing.common_prefixes),
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix,
            start_after,
            next_continuation_token,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

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
            content_length,
            content_md5,
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

        // if is directory
        if key.ends_with('/') {
            if let Some(len) = content_length {
                if len > 0 {
                    return Err(s3_error!(
                        UnexpectedContent,
                        "Unexpected request body when creating a directory object."
                    ));
                }
            }

            connection
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    Self::try_put_object(
                        &transaction,
                        KeyValue {
                            key,
                            value: None,
                            size: 0,
                            metadata,
                            last_modified: OffsetDateTime::now_utc(),
                            md5: None,
                        },
                    )?;
                    Ok(transaction.commit()?)
                })
                .await?;

            let output = PutObjectOutput::default();
            return Ok(S3Response::new(output));
        }

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        let size = copy_bytes(stream, &mut value).await?;
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);

        // if provided verify content_md5
        if let Some(content_md5) = content_md5 {
            if content_md5 != base64(md5_bytes) {
                return Err(s3_error!(BadDigest));
            }
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

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput { bucket, key, .. } = req.input;

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
                    req.credentials,
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
        if let Some(content_md5) = content_md5 {
            if content_md5 != base64(md5_bytes) {
                return Err(s3_error!(BadDigest));
            }
        }

        let md5_clone = md5.clone();
        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket,
                    &key,
                    req.credentials,
                )?
                .not()
                {
                    return Err(s3_error!(AccessDenied).into());
                }

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

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .not()
                {
                    return Err(s3_error!(AccessDenied).into());
                }

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

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .not()
                {
                    return Err(s3_error!(AccessDenied).into());
                }

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
                for ((requested_part_number, requested_etag), part) in
                    requested_parts.iter().zip(parts)
                {
                    if part.part_number != *requested_part_number {
                        return Err(s3_error!(InvalidPart).into());
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
                        key: key_clone,
                        value: Some(value),
                        size,
                        metadata: None,
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5.clone()),
                    },
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                Self::try_delete_multipart(&transaction, upload_id)
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
