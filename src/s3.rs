use std::{
    collections::HashSet,
    fmt::Write as _,
    ops::{Not, Range as ByteRange},
};

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use md5::{Digest, Md5};
use s3s::{
    S3, S3Error, S3ErrorCode,
    S3ErrorCode::InternalError,
    S3Request, S3Response, S3Result, StdError,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput,
        CopyObjectOutput, CopyObjectResult, CopyPartResult, CopySource, CreateBucketInput,
        CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
        DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag, ETagCondition, EncodingType,
        Error as DeleteError, GetBucketLocationInput, GetBucketLocationOutput, GetObjectAclInput,
        GetObjectAclOutput, GetObjectInput, GetObjectOutput, Grant, Grantee, HeadBucketInput,
        HeadBucketOutput, HeadObjectInput, HeadObjectOutput, Initiator, ListBucketsInput,
        ListBucketsOutput, ListMultipartUploadsInput, ListMultipartUploadsOutput,
        ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
        ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput,
        MetadataDirective, MultipartUpload, Object, ObjectVersion, ObjectVersionStorageClass,
        Owner, Part, Permission, PutObjectInput, PutObjectOutput, Range as S3Range, StorageClass,
        StreamingBlob, Timestamp, Type, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput,
        UploadPartOutput,
    },
    header::CONTENT_MD5,
};
use time::OffsetDateTime;
use tokio::{
    fs,
    io::{AsyncWrite, AsyncWriteExt},
};
use tracing::debug;
use uuid::Uuid;

use crate::sqlite::{
    CompletedObjectPart, KeyMetadata, KeyValue, Multipart, MultipartCompletionPart,
    MultipartUploadState, MultipartUploadSummary, ObjectSummary, ObjectTagCondition,
    ObjectWriteConditions, Sqlite, SqliteError,
};

const LIST_BATCH_SIZE: i64 = 1024;
const CONTINUATION_TOKEN_PREFIX: &str = "s3ite:v1:";
const OWNER_DISPLAY_NAME: &str = "s3ite";
const OWNER_ID: &str = "s3ite";

/// Maps storage-layer failures into S3 protocol errors at the service boundary.
fn s3_error_from_sqlite(error: SqliteError) -> S3Error {
    match error {
        SqliteError::BucketNotFound => S3Error::new(S3ErrorCode::NoSuchBucket),
        SqliteError::BucketReadOnly => S3Error::with_message(
            S3ErrorCode::MethodNotAllowed,
            "database is in read-only mode",
        ),
        SqliteError::ObjectNotFound => S3Error::new(S3ErrorCode::NoSuchKey),
        SqliteError::InvalidObjectRange => S3Error::new(S3ErrorCode::InvalidRange),
        SqliteError::PreconditionFailed => S3Error::new(S3ErrorCode::PreconditionFailed),
        SqliteError::MultipartUploadNotFound => S3Error::new(S3ErrorCode::NoSuchUpload),
        SqliteError::MultipartAccessDenied => S3Error::new(S3ErrorCode::AccessDenied),
        SqliteError::InvalidMultipartPart => S3Error::new(S3ErrorCode::InvalidPart),
        SqliteError::MultipartPartTooSmall => S3Error::new(S3ErrorCode::EntityTooSmall),
        SqliteError::MultipartObjectTooLarge => {
            S3Error::with_message(InternalError, "multipart object is too large")
        }
        SqliteError::Configuration(message) => S3Error::with_message(InternalError, message),
        error => S3Error::internal_error(error),
    }
}

/// Builds an S3 protocol error with no custom message.
fn s3_error(code: S3ErrorCode) -> S3Error {
    S3Error::new(code)
}

/// Builds an S3 protocol error with a custom message.
fn s3_error_with_message(code: S3ErrorCode, message: &'static str) -> S3Error {
    S3Error::with_message(code, message)
}

/// Copies a streaming S3 request body into an async writer.
async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> S3Result<u64>
where
    S: Stream<Item = std::result::Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = result.map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
        writer
            .write_all(&bytes)
            .await
            .map_err(S3Error::internal_error)?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await.map_err(S3Error::internal_error)?;
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

/// Decodes standard base64 text.
fn decode_base64(value: &str) -> Option<Vec<u8>> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len().is_multiple_of(4).not() {
        return None;
    }

    let chunk_count = bytes.len() / 4;
    let mut decoded = Vec::with_capacity(chunk_count * 3);
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let is_last = index + 1 == chunk_count;
        let padding = match (chunk[2], chunk[3]) {
            (b'=', b'=') => 2,
            (b'=', _) => return None,
            (_, b'=') => 1,
            _ => 0,
        };

        if padding > 0 && is_last.not() {
            return None;
        }

        let b0 = sextet(chunk[0])?;
        let b1 = sextet(chunk[1])?;
        let b2 = if padding == 2 { 0 } else { sextet(chunk[2])? };
        let b3 = if padding > 0 { 0 } else { sextet(chunk[3])? };

        if (padding == 2 && (b1 & 0x0f) != 0) || (padding == 1 && (b2 & 0x03) != 0) {
            return None;
        }

        decoded.push((b0 << 2) | (b1 >> 4));
        if padding < 2 {
            decoded.push((b1 << 4) | (b2 >> 2));
        }
        if padding == 0 {
            decoded.push((b2 << 6) | b3);
        }
    }

    Some(decoded)
}

/// Verifies the optional `Content-MD5` header against the computed body digest.
fn validate_content_md5(
    header_present: bool,
    content_md5: Option<&str>,
    computed_md5: &[u8],
) -> S3Result<()> {
    let Some(content_md5) = content_md5 else {
        if header_present {
            return Err(s3_error(S3ErrorCode::InvalidDigest));
        }
        return Ok(());
    };

    let decoded = decode_base64(content_md5).ok_or_else(|| s3_error(S3ErrorCode::InvalidDigest))?;
    if decoded.len() != 16 {
        return Err(s3_error(S3ErrorCode::InvalidDigest));
    }
    if decoded != computed_md5 {
        return Err(s3_error(S3ErrorCode::BadDigest));
    }

    Ok(())
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
        .ok_or_else(|| s3_error(S3ErrorCode::InvalidToken))?;
    let decoded = decode_hex(encoded).ok_or_else(|| s3_error(S3ErrorCode::InvalidToken))?;
    String::from_utf8(decoded).map_err(|_| s3_error(S3ErrorCode::InvalidToken))
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

/// Parses a multipart upload ID into the internal UUID representation.
fn parse_upload_id(upload_id: &str) -> S3Result<Uuid> {
    Uuid::parse_str(upload_id).map_err(|_| s3_error(S3ErrorCode::NoSuchUpload))
}

/// Validates an S3 multipart part number.
fn validate_part_number(part_number: i32) -> S3Result<()> {
    if (1..=10_000).contains(&part_number) {
        Ok(())
    } else {
        Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "partNumber must be between 1 and 10000",
        ))
    }
}

/// Returns a bounded S3 listing limit.
fn list_limit(limit: Option<i32>, default: i32, max: i32) -> S3Result<i32> {
    let limit = limit.unwrap_or(default);
    if limit <= 0 {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "listing limit must be greater than zero",
        ));
    }
    Ok(limit.min(max))
}

/// Parses an upload-copy source byte range.
fn parse_copy_source_range(range: Option<String>) -> S3Result<Option<S3Range>> {
    range
        .map(|range| {
            S3Range::parse(&range)
                .map_err(|_| s3_error_with_message(S3ErrorCode::InvalidArgument, "invalid range"))
        })
        .transpose()
}

/// Normalizes an optional source range against object size.
fn checked_copy_source_range(
    range: Option<S3Range>,
    object_size: u64,
) -> Result<ByteRange<u64>, SqliteError> {
    match range {
        Some(range) => {
            if let S3Range::Int {
                last: Some(last), ..
            } = range
                && last >= object_size
            {
                return Err(SqliteError::InvalidObjectRange);
            }
            let range = range
                .check(object_size)
                .map_err(|_| SqliteError::InvalidObjectRange)?;
            if range.is_empty() {
                return Err(SqliteError::InvalidObjectRange);
            }
            Ok(range)
        }
        None => Ok(0..object_size),
    }
}

/// Returns an object byte slice for upload-copy storage.
fn copied_part_bytes(object: &KeyValue, range: ByteRange<u64>) -> Result<Vec<u8>, SqliteError> {
    let value = match object.value.as_ref() {
        Some(value) => value,
        None if object.key_metadata.size == 0 => return Ok(Vec::new()),
        None => {
            return Err(SqliteError::Configuration(
                "object metadata has nonzero size but no value".to_string(),
            ));
        }
    };

    let start = usize::try_from(range.start).map_err(|_| SqliteError::InvalidObjectRange)?;
    let end = usize::try_from(range.end).map_err(|_| SqliteError::InvalidObjectRange)?;
    value
        .get(start..end)
        .map(<[u8]>::to_vec)
        .ok_or(SqliteError::InvalidObjectRange)
}

/// Computes the MD5 `ETag` for part bytes.
fn part_md5(value: &[u8]) -> String {
    let mut md5_hash = Md5::new();
    md5_hash.update(value);
    hex(md5_hash.finalize())
}

/// Converts a stored completed part boundary into an object byte range.
fn completed_part_range(part: &CompletedObjectPart) -> S3Result<ByteRange<u64>> {
    let start = u64::try_from(part.start).map_err(S3Error::internal_error)?;
    let size = u64::try_from(part.size).map_err(S3Error::internal_error)?;
    let end = start
        .checked_add(size)
        .ok_or_else(|| S3Error::with_message(InternalError, "completed part boundary overflows"))?;
    Ok(start..end)
}

/// Resolves `partNumber` against recorded completed multipart boundaries.
fn requested_part_range(
    part_number: i32,
    completed_part: Option<&CompletedObjectPart>,
    completed_part_count: i32,
    object_size: u64,
) -> S3Result<ByteRange<u64>> {
    match completed_part {
        Some(part) => completed_part_range(part),
        None if completed_part_count == 0 && part_number == 1 => Ok(0..object_size),
        None => Err(s3_error(S3ErrorCode::InvalidPart)),
    }
}

#[derive(Clone, Debug)]
struct ObjectReadConditions {
    etag_matches: Option<ETagCondition>,
    modified_since: Option<OffsetDateTime>,
    etag_none_matches: Option<ETagCondition>,
    unmodified_since: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
struct ObjectDeleteConditions {
    etag_matches: Option<ETagCondition>,
    last_modified_time: Option<OffsetDateTime>,
    size: Option<i64>,
}

#[derive(Debug)]
struct CopiedPart {
    value: Vec<u8>,
    size: i64,
    md5: String,
}

/// Converts S3 write condition headers into storage-neutral object conditions.
#[must_use]
fn object_write_conditions(
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
) -> ObjectWriteConditions {
    ObjectWriteConditions {
        if_match: if_match.map(strong_object_tag_condition),
        if_none_match: if_none_match.map(weak_object_tag_condition),
    }
}

/// Converts an `If-Match` value into a strong storage tag condition.
#[must_use]
fn strong_object_tag_condition(condition: ETagCondition) -> ObjectTagCondition {
    match condition {
        ETagCondition::Any => ObjectTagCondition::Any,
        ETagCondition::ETag(ETag::Strong(md5)) => ObjectTagCondition::Md5(md5),
        ETagCondition::ETag(ETag::Weak(_)) => ObjectTagCondition::Never,
    }
}

/// Converts an `If-None-Match` value into a weak storage tag condition.
#[must_use]
fn weak_object_tag_condition(condition: ETagCondition) -> ObjectTagCondition {
    match condition {
        ETagCondition::Any => ObjectTagCondition::Any,
        ETagCondition::ETag(etag) => ObjectTagCondition::Md5(etag.into_value()),
    }
}

/// Drops sub-second precision to match HTTP date validator precision.
#[must_use]
fn http_date_time(value: OffsetDateTime) -> OffsetDateTime {
    match value.replace_nanosecond(0) {
        Ok(value) => value,
        Err(_) => value,
    }
}

/// Returns whether an S3 `If-Match` style condition matches stored object metadata.
#[must_use]
fn strong_etag_condition_matches(condition: &ETagCondition, md5: Option<&str>) -> bool {
    match condition {
        ETagCondition::Any => md5.is_some(),
        ETagCondition::ETag(ETag::Strong(value)) => md5 == Some(value.as_str()),
        ETagCondition::ETag(ETag::Weak(_)) => false,
    }
}

/// Returns whether an S3 `If-None-Match` style condition matches stored object metadata.
#[must_use]
fn weak_etag_condition_matches(condition: &ETagCondition, md5: Option<&str>) -> bool {
    match condition {
        ETagCondition::Any => md5.is_some(),
        ETagCondition::ETag(etag) => md5 == Some(etag.value()),
    }
}

/// Applies GET/HEAD conditional request semantics to loaded object metadata.
fn validate_read_conditions(
    metadata: &KeyMetadata,
    conditions: &ObjectReadConditions,
) -> S3Result<()> {
    let md5 = metadata.md5.as_deref();
    if let Some(condition) = conditions.etag_matches.as_ref()
        && strong_etag_condition_matches(condition, md5).not()
    {
        return Err(s3_error(S3ErrorCode::PreconditionFailed));
    }

    if let Some(condition) = conditions.etag_none_matches.as_ref() {
        if weak_etag_condition_matches(condition, md5) {
            return Err(s3_error(S3ErrorCode::NotModified));
        }
    } else if let Some(since) = conditions.modified_since
        && http_date_time(metadata.last_modified) <= http_date_time(since)
    {
        return Err(s3_error(S3ErrorCode::NotModified));
    }

    if conditions.etag_matches.is_none()
        && let Some(since) = conditions.unmodified_since
        && http_date_time(metadata.last_modified) > http_date_time(since)
    {
        return Err(s3_error(S3ErrorCode::PreconditionFailed));
    }

    Ok(())
}

/// Applies copy-source conditional request semantics to loaded source metadata.
fn validate_copy_source_conditions(
    metadata: &KeyMetadata,
    conditions: &ObjectReadConditions,
) -> Result<(), SqliteError> {
    let md5 = metadata.md5.as_deref();
    if let Some(condition) = conditions.etag_matches.as_ref()
        && strong_etag_condition_matches(condition, md5).not()
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if let Some(condition) = conditions.etag_none_matches.as_ref() {
        if weak_etag_condition_matches(condition, md5) {
            return Err(SqliteError::PreconditionFailed);
        }
    } else if let Some(since) = conditions.modified_since
        && http_date_time(metadata.last_modified) <= http_date_time(since)
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if conditions.etag_matches.is_none()
        && let Some(since) = conditions.unmodified_since
        && http_date_time(metadata.last_modified) > http_date_time(since)
    {
        return Err(SqliteError::PreconditionFailed);
    }

    Ok(())
}

/// Applies delete conditional request semantics inside a write transaction.
fn validate_delete_conditions(
    metadata: Option<&KeyMetadata>,
    conditions: &ObjectDeleteConditions,
) -> Result<(), SqliteError> {
    let Some(metadata) = metadata else {
        return Ok(());
    };

    if let Some(condition) = conditions.etag_matches.as_ref()
        && strong_etag_condition_matches(condition, metadata.md5.as_deref()).not()
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if let Some(last_modified_time) = conditions.last_modified_time
        && http_date_time(metadata.last_modified) != http_date_time(last_modified_time)
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if let Some(size) = conditions.size
        && metadata.size != size
    {
        return Err(SqliteError::PreconditionFailed);
    }

    Ok(())
}

/// Converts a stored object summary into an S3 list object.
fn object_from_summary(summary: ObjectSummary, encode_keys: bool, owner: Option<&Owner>) -> Object {
    Object {
        key: Some(encode_list_value(summary.key, encode_keys)),
        last_modified: Some(summary.last_modified.into()),
        size: Some(summary.size),
        e_tag: summary.md5.map(ETag::Strong),
        owner: owner.cloned(),
        ..Default::default()
    }
}

/// Converts a stored object summary into an S3 object-version entry.
fn object_version_from_summary(
    summary: ObjectSummary,
    encode_keys: bool,
    owner: Option<&Owner>,
) -> ObjectVersion {
    ObjectVersion {
        e_tag: summary.md5.map(ETag::Strong),
        is_latest: Some(true),
        key: Some(encode_list_value(summary.key, encode_keys)),
        last_modified: Some(summary.last_modified.into()),
        owner: owner.cloned(),
        size: Some(summary.size),
        storage_class: Some(ObjectVersionStorageClass::from_static(
            ObjectVersionStorageClass::STANDARD,
        )),
        version_id: Some("null".to_string()),
        ..Default::default()
    }
}

struct KeyListing {
    keys: Vec<ObjectSummary>,
    common_prefixes: Vec<CommonPrefix>,
    key_count: usize,
    next_marker: Option<String>,
    is_truncated: bool,
}

struct MultipartUploadListing {
    uploads: Vec<MultipartUploadSummary>,
    common_prefixes: Vec<CommonPrefix>,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
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
        let connection = self
            .try_get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let mut keys = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_common_prefixes = HashSet::new();
        let mut key_count = 0usize;
        let marker_filter = after_key.clone();
        let mut next_marker = None;
        let mut is_truncated = false;
        let list_batch_len = usize::try_from(LIST_BATCH_SIZE).map_err(S3Error::internal_error)?;

        if max_keys > 0 {
            'pages: loop {
                let prefix = prefix_filter.to_string();
                let after_key_query = after_key.clone();
                let summaries = connection
                    .read(move |connection| {
                        let transaction = connection.transaction()?;
                        Self::try_list_objects(
                            &transaction,
                            Some(&prefix),
                            after_key_query.as_deref(),
                            LIST_BATCH_SIZE,
                        )
                    })
                    .await
                    .map_err(s3_error_from_sqlite)?;

                if summaries.is_empty() {
                    break;
                }

                let summary_count = summaries.len();
                for summary in summaries {
                    if prefix_filter.is_empty().not()
                        && summary.key.starts_with(prefix_filter).not()
                    {
                        break 'pages;
                    }

                    if let Some(common_prefix) =
                        common_prefix_for_key(&summary.key, prefix_filter, delimiter_filter)
                    {
                        if marker_filter
                            .as_ref()
                            .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                        {
                            after_key = Some(summary.key);
                            continue;
                        }

                        if seen_common_prefixes.contains(&common_prefix) {
                            after_key = Some(summary.key);
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
                        after_key = Some(summary.key);
                        continue;
                    }

                    if key_count == max_keys {
                        is_truncated = true;
                        break 'pages;
                    }

                    after_key = Some(summary.key.clone());
                    next_marker = Some(summary.key.clone());
                    keys.push(summary);
                    key_count += 1;
                }

                if summary_count < list_batch_len {
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

    /// Lists active multipart uploads with S3 prefix, delimiter, and marker semantics.
    async fn list_active_multipart_uploads(
        &self,
        bucket: &str,
        prefix_filter: &str,
        delimiter_filter: Option<&str>,
        mut after_key: Option<String>,
        mut after_upload_id: Option<Uuid>,
        max_uploads: usize,
    ) -> S3Result<MultipartUploadListing> {
        let connection = self
            .try_get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let mut uploads = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_common_prefixes = HashSet::new();
        let marker_filter = after_key.clone();
        let mut next_key_marker = None;
        let mut next_upload_id_marker = None;
        let mut count = 0usize;
        let mut is_truncated = false;
        let list_batch_len = usize::try_from(LIST_BATCH_SIZE).map_err(S3Error::internal_error)?;

        if max_uploads > 0 {
            'pages: loop {
                let prefix = prefix_filter.to_string();
                let after_key_query = after_key.clone();
                let after_upload_id_query = after_upload_id;
                let summaries = connection
                    .read(move |connection| {
                        let transaction = connection.transaction()?;
                        Self::try_list_multipart_uploads(
                            &transaction,
                            Some(&prefix),
                            after_key_query.as_deref(),
                            after_upload_id_query,
                            LIST_BATCH_SIZE,
                        )
                    })
                    .await
                    .map_err(s3_error_from_sqlite)?;

                if summaries.is_empty() {
                    break;
                }

                let summary_count = summaries.len();
                for summary in summaries {
                    if prefix_filter.is_empty().not()
                        && summary.key.starts_with(prefix_filter).not()
                    {
                        break 'pages;
                    }

                    if let Some(common_prefix) =
                        common_prefix_for_key(&summary.key, prefix_filter, delimiter_filter)
                    {
                        if marker_filter
                            .as_ref()
                            .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                        {
                            after_key = Some(summary.key);
                            after_upload_id = Some(summary.upload_id);
                            continue;
                        }

                        if seen_common_prefixes.contains(&common_prefix) {
                            after_key = Some(summary.key);
                            after_upload_id = Some(summary.upload_id);
                            continue;
                        }

                        if count == max_uploads {
                            is_truncated = true;
                            break 'pages;
                        }

                        seen_common_prefixes.insert(common_prefix.clone());
                        next_key_marker = Some(common_prefix.clone());
                        next_upload_id_marker = None;
                        common_prefixes.push(CommonPrefix {
                            prefix: Some(common_prefix),
                        });
                        count += 1;
                        after_key = Some(summary.key);
                        after_upload_id = Some(summary.upload_id);
                        continue;
                    }

                    if count == max_uploads {
                        is_truncated = true;
                        break 'pages;
                    }

                    next_key_marker = Some(summary.key.clone());
                    next_upload_id_marker = Some(summary.upload_id.to_string());
                    after_key = Some(summary.key.clone());
                    after_upload_id = Some(summary.upload_id);
                    uploads.push(summary);
                    count += 1;
                }

                if summary_count < list_batch_len {
                    break;
                }
            }
        }

        Ok(MultipartUploadListing {
            uploads,
            common_prefixes,
            next_key_marker,
            next_upload_id_marker,
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

        self.try_create_bucket(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        let output = CreateBucketOutput {
            location: Some(format!("/{bucket}")),
        };
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
            copy_source_if_match,
            copy_source_if_modified_since,
            copy_source_if_none_match,
            copy_source_if_unmodified_since,
            expires,
            metadata,
            metadata_directive,
            ..
        } = req.input;

        self.validate_mutable_bucket(&tgt_bucket)
            .map_err(s3_error_from_sqlite)?;

        let (src_bucket, src_key) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error(S3ErrorCode::NotImplemented)),
            CopySource::Bucket { bucket, key, .. } => (bucket, key),
        };
        let replace_metadata = metadata_directive
            .as_ref()
            .is_some_and(|directive| directive.as_str() == MetadataDirective::REPLACE);
        if src_bucket.as_ref() == tgt_bucket.as_str()
            && src_key.as_ref() == tgt_key.as_str()
            && replace_metadata.not()
        {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidRequest,
                "copy source must be different from target unless replacing metadata",
            ));
        }
        let source_conditions = ObjectReadConditions {
            etag_matches: copy_source_if_match,
            modified_since: copy_source_if_modified_since.map(Into::into),
            etag_none_matches: copy_source_if_none_match,
            unmodified_since: copy_source_if_unmodified_since.map(Into::into),
        };

        if src_bucket.as_ref() == tgt_bucket.as_str() {
            let connection = self
                .try_get_connection(&tgt_bucket)
                .await
                .map_err(s3_error_from_sqlite)?;
            let copy_object_result = connection
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    let mut object = Self::try_get_object(&transaction, &src_key)?
                        .ok_or(SqliteError::ObjectNotFound)?;
                    validate_copy_source_conditions(&object.key_metadata, &source_conditions)?;

                    object.key = tgt_key;
                    if replace_metadata {
                        object.key_metadata.metadata = metadata;
                        object.key_metadata.content_type = content_type;
                        object.key_metadata.cache_control = cache_control;
                        object.key_metadata.expires = expires.map(Into::into);
                    }

                    let copy_object_result = CopyObjectResult {
                        e_tag: object.key_metadata.md5.clone().map(ETag::Strong),
                        last_modified: Some(object.key_metadata.last_modified.into()),
                        ..Default::default()
                    };

                    Self::try_put_object(&transaction, object)?;
                    transaction.commit()?;
                    Ok(copy_object_result)
                })
                .await
                .map_err(s3_error_from_sqlite)?;

            let output = CopyObjectOutput {
                copy_object_result: Some(copy_object_result),
                ..Default::default()
            };
            return Ok(S3Response::new(output));
        }

        // verify source and target buckets exist
        let connection = self
            .try_get_connection(&src_bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        let mut object = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_get_object(&transaction, &src_key)?.ok_or(SqliteError::ObjectNotFound)
            })
            .await
            .map_err(s3_error_from_sqlite)?;
        validate_copy_source_conditions(&object.key_metadata, &source_conditions)
            .map_err(s3_error_from_sqlite)?;

        // replace key with target key
        object.key = tgt_key.clone();
        if replace_metadata {
            object.key_metadata.metadata = metadata;
            object.key_metadata.content_type = content_type;
            object.key_metadata.cache_control = cache_control;
            object.key_metadata.expires = expires.map(Into::into);
        }

        let copy_object_result = CopyObjectResult {
            e_tag: object.key_metadata.md5.clone().map(ETag::Strong),
            last_modified: Some(object.key_metadata.last_modified.into()),
            ..Default::default()
        };

        let connection = self
            .try_get_connection(&tgt_bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_put_object(&transaction, object)?;
                Ok(transaction.commit()?)
            })
            .await
            .map_err(s3_error_from_sqlite)?;

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

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let has_objects = connection
            .read(|connection| {
                let transaction = connection.transaction()?;
                Self::try_has_objects(&transaction)
            })
            .await
            .map_err(s3_error_from_sqlite)?;
        if has_objects {
            return Err(s3_error(S3ErrorCode::BucketNotEmpty));
        }

        let connection = self
            .buckets
            .write()
            .await
            .remove(&bucket)
            .ok_or(SqliteError::BucketNotFound)
            .map_err(s3_error_from_sqlite)?;

        connection.close().await.map_err(s3_error_from_sqlite)?;

        let bucket_path = self
            .get_bucket_path(&bucket)
            .map_err(s3_error_from_sqlite)?;
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
        let DeleteObjectInput {
            bucket,
            key,
            if_match,
            if_match_last_modified_time,
            if_match_size,
            ..
        } = req.input;
        let conditions = ObjectDeleteConditions {
            etag_matches: if_match,
            last_modified_time: if_match_last_modified_time.map(Into::into),
            size: if_match_size,
        };

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let metadata = Self::try_get_metadata(&transaction, &key)?;
                validate_delete_conditions(metadata.as_ref(), &conditions)?;
                Self::try_delete_object(&transaction, &key)?;
                Ok(transaction.commit()?)
            })
            .await
            .map_err(s3_error_from_sqlite)?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    /// Deletes a batch of objects and returns S3 multi-delete results.
    #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let quiet = delete.quiet.unwrap_or(false);
        let delete_objects = delete
            .objects
            .into_iter()
            .map(|object| {
                (
                    object.key,
                    ObjectDeleteConditions {
                        etag_matches: object.e_tag.map(ETagCondition::ETag),
                        last_modified_time: object.last_modified_time.map(Into::into),
                        size: object.size,
                    },
                )
            })
            .collect::<Vec<_>>();
        if delete_objects.len() > 1000 {
            return Err(s3_error_with_message(
                S3ErrorCode::MalformedXML,
                "DeleteObjects supports at most 1000 keys",
            ));
        }

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        let (deleted_keys, errors) = connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let mut deleted_keys = Vec::new();
                let mut errors = Vec::new();

                for (key, conditions) in delete_objects {
                    let metadata = Self::try_get_metadata(&transaction, &key)?;
                    match validate_delete_conditions(metadata.as_ref(), &conditions) {
                        Ok(()) => {
                            Self::try_delete_object(&transaction, &key)?;
                            deleted_keys.push(key);
                        }
                        Err(SqliteError::PreconditionFailed) => errors.push(DeleteError {
                            key: Some(key),
                            code: Some("PreconditionFailed".to_string()),
                            message: Some("At least one precondition failed".to_string()),
                            ..Default::default()
                        }),
                        Err(err) => return Err(err),
                    }
                }

                transaction.commit()?;
                Ok((deleted_keys, errors))
            })
            .await
            .map_err(s3_error_from_sqlite)?;

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
            errors: errors.is_empty().not().then_some(errors),
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
            return Err(s3_error(S3ErrorCode::NoSuchBucket));
        }

        let file_path = self
            .get_bucket_path(&bucket)
            .map_err(s3_error_from_sqlite)?;
        if file_path.exists().not() {
            return Err(s3_error(S3ErrorCode::NoSuchBucket));
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
            bucket,
            key,
            range,
            part_number,
            if_match,
            if_modified_since,
            if_none_match,
            if_unmodified_since,
            ..
        } = req.input;
        if range.is_some() && part_number.is_some() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidRequest,
                "Range and partNumber cannot both be specified",
            ));
        }
        if let Some(part_number) = part_number {
            validate_part_number(part_number)?;
        }

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        let (object, completed_part, completed_part_count) = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                let object =
                    Self::try_get_object(&transaction, &key)?.ok_or(SqliteError::ObjectNotFound)?;
                let (completed_part, completed_part_count) = match part_number {
                    Some(part_number) => (
                        Self::try_get_completed_object_part(&transaction, &key, part_number)?,
                        Self::try_count_completed_object_parts(&transaction, &key)?,
                    ),
                    None => (None, 0),
                };
                Ok((object, completed_part, completed_part_count))
            })
            .await
            .map_err(s3_error_from_sqlite)?;
        validate_read_conditions(
            &object.key_metadata,
            &ObjectReadConditions {
                etag_matches: if_match,
                modified_since: if_modified_since.map(Into::into),
                etag_none_matches: if_none_match,
                unmodified_since: if_unmodified_since.map(Into::into),
            },
        )?;

        let object_size =
            u64::try_from(object.key_metadata.size).map_err(S3Error::internal_error)?;
        let object_range = match (part_number, range.as_ref()) {
            (Some(part_number), _) => requested_part_range(
                part_number,
                completed_part.as_ref(),
                completed_part_count,
                object_size,
            )?,
            (None, None) => 0..object_size,
            (None, Some(range)) => {
                let object_range = range.check(object_size)?;
                if object_range.is_empty() {
                    return Err(s3_error(S3ErrorCode::InvalidRange));
                }
                object_range
            }
        };
        let content_length = object_range.end - object_range.start;
        let content_length_i64 = i64::try_from(content_length).map_err(S3Error::internal_error)?;

        let value = match object.value {
            Some(value) => value,
            None if object.key_metadata.size == 0 => Vec::new(),
            None => {
                return Err(S3Error::with_message(
                    InternalError,
                    "object metadata has nonzero size but no value",
                ));
            }
        };

        let start = usize::try_from(object_range.start).map_err(S3Error::internal_error)?;
        let end = usize::try_from(object_range.end).map_err(S3Error::internal_error)?;
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
            cache_control: object.key_metadata.cache_control,
            content_range: (range.is_some() || part_number.is_some())
                .then(|| {
                    object_range
                        .end
                        .checked_sub(1)
                        .map(|end| format!("bytes {}-{}/{}", object_range.start, end, object_size))
                })
                .flatten(),
            content_length: Some(content_length_i64),
            content_type: object.key_metadata.content_type,
            expires: object.key_metadata.expires.map(Into::into),
            last_modified: Some(object.key_metadata.last_modified.into()),
            metadata: object.key_metadata.metadata,
            e_tag: object.key_metadata.md5.map(ETag::Strong),
            parts_count: (completed_part_count > 0).then_some(completed_part_count),
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
            return Err(s3_error(S3ErrorCode::NoSuchBucket));
        }

        let file_path = self
            .get_bucket_path(&bucket)
            .map_err(s3_error_from_sqlite)?;
        if file_path.exists().not() {
            return Err(s3_error(S3ErrorCode::NoSuchBucket));
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
        let HeadObjectInput {
            bucket,
            key,
            range,
            part_number,
            if_match,
            if_modified_since,
            if_none_match,
            if_unmodified_since,
            ..
        } = req.input;
        if range.is_some() && part_number.is_some() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidRequest,
                "Range and partNumber cannot both be specified",
            ));
        }
        if let Some(part_number) = part_number {
            validate_part_number(part_number)?;
        }

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        let (object, completed_part, completed_part_count) = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                let object = Self::try_get_metadata(&transaction, &key)?
                    .ok_or(SqliteError::ObjectNotFound)?;
                let (completed_part, completed_part_count) = match part_number {
                    Some(part_number) => (
                        Self::try_get_completed_object_part(&transaction, &key, part_number)?,
                        Self::try_count_completed_object_parts(&transaction, &key)?,
                    ),
                    None => (None, 0),
                };
                Ok((object, completed_part, completed_part_count))
            })
            .await
            .map_err(s3_error_from_sqlite)?;
        validate_read_conditions(
            &object,
            &ObjectReadConditions {
                etag_matches: if_match,
                modified_since: if_modified_since.map(Into::into),
                etag_none_matches: if_none_match,
                unmodified_since: if_unmodified_since.map(Into::into),
            },
        )?;

        let content_type = object
            .content_type
            .unwrap_or_else(|| mime::APPLICATION_OCTET_STREAM.to_string());
        let object_size = u64::try_from(object.size).map_err(S3Error::internal_error)?;
        let object_range = match (part_number, range.as_ref()) {
            (Some(part_number), _) => requested_part_range(
                part_number,
                completed_part.as_ref(),
                completed_part_count,
                object_size,
            )?,
            (None, None) => 0..object_size,
            (None, Some(range)) => {
                let object_range = range.check(object_size)?;
                if object_range.is_empty() {
                    return Err(s3_error(S3ErrorCode::InvalidRange));
                }
                object_range
            }
        };
        let content_length = i64::try_from(object_range.end - object_range.start)
            .map_err(S3Error::internal_error)?;

        let output = HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            cache_control: object.cache_control,
            content_length: Some(content_length),
            content_range: (range.is_some() || part_number.is_some())
                .then(|| {
                    object_range
                        .end
                        .checked_sub(1)
                        .map(|end| format!("bytes {}-{}/{}", object_range.start, end, object_size))
                })
                .flatten(),
            content_type: Some(content_type),
            e_tag: object.md5.map(ETag::Strong),
            expires: object.expires.map(Into::into),
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            parts_count: (completed_part_count > 0).then_some(completed_part_count),
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

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_get_metadata(&transaction, &key)?.ok_or(SqliteError::ObjectNotFound)?;
                Ok(())
            })
            .await
            .map_err(s3_error_from_sqlite)?;

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
            let file_path = self.get_bucket_path(name).map_err(s3_error_from_sqlite)?;
            let file_meta = fs::metadata(file_path)
                .await
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
            let created_or_modified_date = Timestamp::from(
                file_meta
                    .created()
                    .or_else(|_| file_meta.modified())
                    .map_err(S3Error::internal_error)?,
            );

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
        let max_keys_usize = usize::try_from(max_keys).map_err(S3Error::internal_error)?;
        let marker_output = Some(marker.clone().unwrap_or_default());
        let prefix_filter = prefix.clone().unwrap_or_default();
        let encode_keys = is_url_encoding(encoding_type.as_ref());
        let delimiter_filter = delimiter
            .as_deref()
            .filter(|delimiter| delimiter.is_empty().not())
            .map(str::to_string);
        if allow_unordered && delimiter_filter.is_some() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "allow-unordered cannot be used with delimiter",
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
            .map(|summary| object_from_summary(summary, encode_keys, Some(&owner)))
            .collect::<Vec<_>>();
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
        let max_keys_usize = usize::try_from(max_keys).map_err(S3Error::internal_error)?;
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
            .map(|summary| object_version_from_summary(summary, encode_keys, Some(&owner)))
            .collect::<Vec<_>>();

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
        let max_keys_usize = usize::try_from(max_keys).map_err(S3Error::internal_error)?;
        let continuation_token_clone = continuation_token.clone();
        let prefix_filter = prefix.clone().unwrap_or_default();
        let encode_keys = is_url_encoding(encoding_type.as_ref());
        let delimiter_filter = delimiter
            .as_deref()
            .filter(|delimiter| delimiter.is_empty().not())
            .map(str::to_string);
        if allow_unordered && delimiter_filter.is_some() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "allow-unordered cannot be used with delimiter",
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
            .map(|summary| object_from_summary(summary, encode_keys, owner.as_ref()))
            .collect::<Vec<_>>();

        let next_continuation_token = listing
            .is_truncated
            .then(|| {
                listing
                    .next_marker
                    .as_deref()
                    .map(encode_continuation_token)
            })
            .flatten();

        let key_count = i32::try_from(listing.key_count).map_err(S3Error::internal_error)?;

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

    /// Lists active multipart uploads in a bucket.
    #[tracing::instrument]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let ListMultipartUploadsInput {
            bucket,
            mut prefix,
            mut delimiter,
            mut key_marker,
            upload_id_marker,
            max_uploads,
            encoding_type,
            ..
        } = req.input;

        prefix = prefix.map(decode_list_input_value);
        delimiter = delimiter.map(decode_list_input_value);
        key_marker = key_marker.map(decode_list_input_value);

        let max_uploads = list_limit(max_uploads, 1000, 1000)?;
        let max_uploads_usize = usize::try_from(max_uploads).map_err(S3Error::internal_error)?;
        let prefix_filter = prefix.clone().unwrap_or_default();
        let encode_keys = is_url_encoding(encoding_type.as_ref());
        let delimiter_filter = delimiter
            .as_deref()
            .filter(|delimiter| delimiter.is_empty().not())
            .map(str::to_string);
        let delimiter = output_delimiter(delimiter, encode_keys);
        let after_upload_id = if key_marker.is_some() {
            upload_id_marker
                .as_deref()
                .map(parse_upload_id)
                .transpose()?
        } else {
            None
        };

        let listing = self
            .list_active_multipart_uploads(
                &bucket,
                &prefix_filter,
                delimiter_filter.as_deref(),
                key_marker.clone(),
                after_upload_id,
                max_uploads_usize,
            )
            .await?;
        let owner = owner();
        let initiator = Initiator {
            display_name: owner.display_name.clone(),
            id: owner.id.clone(),
        };
        let uploads = listing
            .uploads
            .into_iter()
            .map(|summary| MultipartUpload {
                initiated: Some(summary.initiated.into()),
                initiator: Some(initiator.clone()),
                key: Some(encode_list_value(summary.key, encode_keys)),
                owner: Some(owner.clone()),
                storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                upload_id: Some(summary.upload_id.to_string()),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListMultipartUploadsOutput {
            bucket: Some(bucket),
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
            is_truncated: Some(listing.is_truncated),
            key_marker: key_marker.map(|marker| encode_list_value(marker, encode_keys)),
            max_uploads: Some(max_uploads),
            next_key_marker: listing
                .next_key_marker
                .map(|marker| encode_list_value(marker, encode_keys)),
            next_upload_id_marker: listing.next_upload_id_marker,
            prefix: prefix.map(|prefix| encode_list_value(prefix, encode_keys)),
            upload_id_marker,
            uploads: Some(uploads),
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
        let S3Request { input, headers, .. } = req;
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
            if_match,
            if_none_match,
            storage_class,
            ..
        } = input;
        let write_conditions = object_write_conditions(if_match, if_none_match);

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error(S3ErrorCode::NoSuchBucket));
        }

        if let Some(ref storage_class) = storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if is_valid.not() {
                return Err(s3_error(S3ErrorCode::InvalidStorageClass));
            }
        }

        let Some(body) = body else {
            return Err(s3_error(S3ErrorCode::IncompleteBody));
        };

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        let size = i64::try_from(copy_bytes(stream, &mut value).await?)
            .map_err(S3Error::internal_error)?;
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);

        validate_content_md5(
            headers.contains_key(CONTENT_MD5),
            content_md5.as_deref(),
            md5_bytes.as_ref(),
        )?;

        debug!(path = %key, ?size, %md5, "write file");

        let md5_clone = md5.clone();
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_check_object_write_conditions(&transaction, &key, &write_conditions)?;
                Self::try_put_object(
                    &transaction,
                    KeyValue {
                        key,
                        value: Some(value),
                        key_metadata: KeyMetadata {
                            size,
                            metadata,
                            content_type,
                            cache_control,
                            expires: expires.map(Into::into),
                            last_modified: OffsetDateTime::now_utc(),
                            md5: Some(md5_clone),
                        },
                    },
                )?;
                Ok(transaction.commit()?)
            })
            .await
            .map_err(s3_error_from_sqlite)?;

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

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let upload_id = Uuid::new_v4();

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
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
            .await
            .map_err(s3_error_from_sqlite)?;

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
        let S3Request { input, headers, .. } = req;
        let UploadPartInput {
            body,
            bucket,
            key,
            upload_id,
            part_number,
            content_md5,
            ..
        } = input;

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let body = body.ok_or_else(|| s3_error(S3ErrorCode::IncompleteBody))?;
        let upload_id = parse_upload_id(&upload_id)?;
        validate_part_number(part_number)?;

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        copy_bytes(stream, &mut value).await?;
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);
        let size = i64::try_from(value.len()).map_err(S3Error::internal_error)?;

        validate_content_md5(
            headers.contains_key(CONTENT_MD5),
            content_md5.as_deref(),
            md5_bytes.as_ref(),
        )?;

        let md5_clone = md5.clone();
        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;

                let upload_state =
                    Self::try_get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                        .ok_or(SqliteError::MultipartUploadNotFound)?;
                Self::try_validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials
                        .as_ref()
                        .map(|credentials| credentials.access_key.as_str()),
                )?;

                Self::try_touch_multipart_upload(&transaction, upload_id)?;
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
            .await
            .map_err(s3_error_from_sqlite)?;

        let output = UploadPartOutput {
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Copies an existing object byte range into a multipart upload part.
    #[tracing::instrument]
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let S3Request {
            input, credentials, ..
        } = req;
        let UploadPartCopyInput {
            bucket,
            key,
            upload_id,
            part_number,
            copy_source,
            copy_source_if_match,
            copy_source_if_modified_since,
            copy_source_if_none_match,
            copy_source_if_unmodified_since,
            copy_source_range,
            ..
        } = input;

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;
        let upload_id = parse_upload_id(&upload_id)?;
        validate_part_number(part_number)?;
        let copy_source_range = parse_copy_source_range(copy_source_range)?;
        let access_key = credentials.map(|credentials| credentials.access_key);

        let (src_bucket, src_key) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error(S3ErrorCode::NotImplemented)),
            CopySource::Bucket { bucket, key, .. } => (bucket, key),
        };
        let source_conditions = ObjectReadConditions {
            etag_matches: copy_source_if_match,
            modified_since: copy_source_if_modified_since.map(Into::into),
            etag_none_matches: copy_source_if_none_match,
            unmodified_since: copy_source_if_unmodified_since.map(Into::into),
        };

        if src_bucket.as_ref() == bucket.as_str() {
            let connection = self
                .try_get_connection(&bucket)
                .await
                .map_err(s3_error_from_sqlite)?;
            let copy_part_result = connection
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    let object = Self::try_get_object(&transaction, &src_key)?
                        .ok_or(SqliteError::ObjectNotFound)?;
                    validate_copy_source_conditions(&object.key_metadata, &source_conditions)?;
                    let object_size = u64::try_from(object.key_metadata.size)
                        .map_err(|_| SqliteError::InvalidObjectRange)?;
                    let byte_range = checked_copy_source_range(copy_source_range, object_size)?;
                    let value = copied_part_bytes(&object, byte_range)?;
                    let size = i64::try_from(value.len())
                        .map_err(|_| SqliteError::MultipartObjectTooLarge)?;
                    let md5 = part_md5(&value);
                    let now = OffsetDateTime::now_utc();

                    let upload_state = Self::try_get_multipart_upload_state(
                        &transaction,
                        upload_id,
                        &bucket,
                        &key,
                    )?
                    .ok_or(SqliteError::MultipartUploadNotFound)?;
                    Self::try_validate_multipart_access(
                        upload_state.access_key.as_ref(),
                        access_key.as_deref(),
                    )?;

                    Self::try_touch_multipart_upload(&transaction, upload_id)?;
                    Self::try_put_multipart(
                        &transaction,
                        Multipart {
                            upload_id,
                            part_number,
                            last_modified: now,
                            value,
                            size,
                            md5: Some(md5.clone()),
                        },
                    )?;

                    transaction.commit()?;
                    Ok(CopyPartResult {
                        e_tag: Some(ETag::Strong(md5)),
                        last_modified: Some(now.into()),
                        ..Default::default()
                    })
                })
                .await
                .map_err(s3_error_from_sqlite)?;

            let output = UploadPartCopyOutput {
                copy_part_result: Some(copy_part_result),
                ..Default::default()
            };
            return Ok(S3Response::new(output));
        }

        let source_connection = self
            .try_get_connection(&src_bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let copied_part = source_connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                let object = Self::try_get_object(&transaction, &src_key)?
                    .ok_or(SqliteError::ObjectNotFound)?;
                validate_copy_source_conditions(&object.key_metadata, &source_conditions)?;
                let object_size = u64::try_from(object.key_metadata.size)
                    .map_err(|_| SqliteError::InvalidObjectRange)?;
                let byte_range = checked_copy_source_range(copy_source_range, object_size)?;
                let value = copied_part_bytes(&object, byte_range)?;
                let size =
                    i64::try_from(value.len()).map_err(|_| SqliteError::MultipartObjectTooLarge)?;
                let md5 = part_md5(&value);
                Ok(CopiedPart { value, size, md5 })
            })
            .await
            .map_err(s3_error_from_sqlite)?;

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let copy_part_result = connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let upload_state =
                    Self::try_get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                        .ok_or(SqliteError::MultipartUploadNotFound)?;
                Self::try_validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    access_key.as_deref(),
                )?;

                let now = OffsetDateTime::now_utc();
                let md5 = copied_part.md5;
                Self::try_touch_multipart_upload(&transaction, upload_id)?;
                Self::try_put_multipart(
                    &transaction,
                    Multipart {
                        upload_id,
                        part_number,
                        last_modified: now,
                        value: copied_part.value,
                        size: copied_part.size,
                        md5: Some(md5.clone()),
                    },
                )?;

                transaction.commit()?;
                Ok(CopyPartResult {
                    e_tag: Some(ETag::Strong(md5)),
                    last_modified: Some(now.into()),
                    ..Default::default()
                })
            })
            .await
            .map_err(s3_error_from_sqlite)?;

        let output = UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
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
            max_parts,
            part_number_marker,
            ..
        } = req.input;

        let upload_id = parse_upload_id(&upload_id)?;
        let max_parts = list_limit(max_parts, 1000, 1000)?;
        let part_number_marker = part_number_marker.unwrap_or(0);
        if part_number_marker < 0 {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "part-number-marker must be nonnegative",
            ));
        }

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let parts = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;

                let upload_state = Self::try_get_multipart_upload_state(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                )?
                .ok_or(SqliteError::MultipartUploadNotFound)?;
                Self::try_validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials
                        .as_ref()
                        .map(|credentials| credentials.access_key.as_str()),
                )?;

                let parts = Self::try_list_multipart(
                    &transaction,
                    upload_id,
                    part_number_marker,
                    i64::from(max_parts) + 1,
                )?;

                Ok(parts)
            })
            .await
            .map_err(s3_error_from_sqlite)?;

        let max_parts_usize = usize::try_from(max_parts).map_err(S3Error::internal_error)?;
        let is_truncated = parts.len() > max_parts_usize;
        let mut parts = parts;
        if is_truncated {
            parts.truncate(max_parts_usize);
        }
        let next_part_number_marker = is_truncated
            .then(|| parts.last().map(|part| part.part_number))
            .flatten();
        let parts = parts
            .into_iter()
            .map(|part| Part {
                e_tag: part.md5.map(ETag::Strong),
                last_modified: Some(part.last_modified.into()),
                part_number: Some(part.part_number),
                size: Some(part.size),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListPartsOutput {
            bucket: Some(bucket),
            is_truncated: Some(is_truncated),
            key: Some(key),
            max_parts: Some(max_parts),
            next_part_number_marker,
            part_number_marker: Some(part_number_marker),
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

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let upload_id =
            Uuid::parse_str(&upload_id).map_err(|_| s3_error(S3ErrorCode::NoSuchUpload))?;

        let connection = self
            .try_get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let upload_state =
                    Self::try_get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                        .ok_or(SqliteError::MultipartUploadNotFound)?;
                Self::try_validate_multipart_access(
                    upload_state.access_key.as_ref(),
                    req.credentials
                        .as_ref()
                        .map(|credentials| credentials.access_key.as_str()),
                )?;

                let deleted = Self::try_delete_multipart(&transaction, upload_id)?;
                if deleted == 0 {
                    return Err(SqliteError::MultipartUploadNotFound);
                }

                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(s3_error_from_sqlite)?;

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
            if_match,
            if_none_match,
            ..
        } = req.input;
        let write_conditions = object_write_conditions(if_match, if_none_match);

        self.validate_mutable_bucket(&bucket)
            .map_err(s3_error_from_sqlite)?;

        let upload_id = parse_upload_id(&upload_id)?;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error(S3ErrorCode::InvalidPart));
        };

        let requested_parts = multipart_upload.parts.unwrap_or_default();
        if requested_parts.is_empty() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidPart,
                "You must specify at least one part",
            ));
        }

        let mut previous_part_number = 0;
        let requested_parts = requested_parts
            .into_iter()
            .map(|part| {
                let part_number = part
                    .part_number
                    .ok_or_else(|| s3_error(S3ErrorCode::InvalidPart))?;
                validate_part_number(part_number)?;
                if part_number <= previous_part_number {
                    return Err(s3_error(S3ErrorCode::InvalidPartOrder));
                }
                previous_part_number = part_number;

                let e_tag = part
                    .e_tag
                    .and_then(ETag::into_strong)
                    .ok_or_else(|| s3_error(S3ErrorCode::InvalidPart))?;

                Ok(MultipartCompletionPart {
                    part_number,
                    md5: e_tag,
                })
            })
            .collect::<S3Result<Vec<_>>>()?;

        let access_key = req.credentials.map(|credentials| credentials.access_key);
        let md5 = self
            .try_complete_multipart_upload(
                bucket.clone(),
                key.clone(),
                upload_id,
                requested_parts,
                access_key,
                write_conditions,
            )
            .await
            .map_err(s3_error_from_sqlite)?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

#[cfg(test)]
mod tests {
    use s3s::S3ErrorCode;

    use super::{decode_base64, validate_content_md5};

    fn assert_error_code(result: s3s::S3Result<()>, code: &S3ErrorCode) {
        match result {
            Ok(()) => panic!("expected {code:?}"),
            Err(err) => assert_eq!(err.code(), code),
        }
    }

    #[test]
    fn decodes_standard_base64() {
        assert_eq!(
            decode_base64("SpRKmvVRaPLiBjkHxCGwYQ=="),
            Some(
                [
                    0x4a, 0x94, 0x4a, 0x9a, 0xf5, 0x51, 0x68, 0xf2, 0xe2, 0x06, 0x39, 0x07, 0xc4,
                    0x21, 0xb0, 0x61
                ]
                .to_vec()
            )
        );
    }

    #[test]
    fn rejects_malformed_content_md5() {
        assert_error_code(
            validate_content_md5(true, None, &[0; 16]),
            &S3ErrorCode::InvalidDigest,
        );
        assert_error_code(
            validate_content_md5(true, Some("YWJyYWNhZGFicmE="), &[0; 16]),
            &S3ErrorCode::InvalidDigest,
        );
        assert_error_code(
            validate_content_md5(true, Some("not base64"), &[0; 16]),
            &S3ErrorCode::InvalidDigest,
        );
    }

    #[test]
    fn distinguishes_wrong_content_md5() {
        assert_error_code(
            validate_content_md5(true, Some("rL0Y20xC+Fzt72VPzMSk2A=="), &[0; 16]),
            &S3ErrorCode::BadDigest,
        );
    }
}
