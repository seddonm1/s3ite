use std::{
    fmt,
    ops::{Not, Range as ByteRange},
};

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use hyper::http::{HeaderMap, HeaderValue};
use md5::{Digest, Md5};
use s3s::{
    S3, S3Error, S3ErrorCode,
    S3ErrorCode::InternalError,
    S3Request, S3Response, S3Result, StdError,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketCannedACL,
        BucketVersioningStatus, ChecksumMode, CommonPrefix, CompleteMultipartUploadInput,
        CompleteMultipartUploadOutput, CompletedPart, CopyObjectInput, CopyObjectOutput,
        CopyObjectResult, CopyPartResult, CopySource, CreateBucketInput, CreateBucketOutput,
        CreateMultipartUploadInput, CreateMultipartUploadOutput, DefaultRetention,
        DeleteBucketCorsInput, DeleteBucketCorsOutput, DeleteBucketEncryptionInput,
        DeleteBucketEncryptionOutput, DeleteBucketInput, DeleteBucketLifecycleInput,
        DeleteBucketLifecycleOutput, DeleteBucketOutput, DeleteBucketTaggingInput,
        DeleteBucketTaggingOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectTaggingInput,
        DeleteObjectTaggingOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag,
        ETagCondition, Error as DeleteError, ExpirationStatus, GetBucketAclInput,
        GetBucketAclOutput, GetBucketCorsInput, GetBucketCorsOutput, GetBucketEncryptionInput,
        GetBucketEncryptionOutput, GetBucketLifecycleConfigurationInput,
        GetBucketLifecycleConfigurationOutput, GetBucketLocationInput, GetBucketLocationOutput,
        GetBucketTaggingInput, GetBucketTaggingOutput, GetBucketVersioningInput,
        GetBucketVersioningOutput, GetObjectAclInput, GetObjectAclOutput, GetObjectAttributesInput,
        GetObjectAttributesOutput, GetObjectAttributesParts, GetObjectInput,
        GetObjectLegalHoldInput, GetObjectLegalHoldOutput, GetObjectLockConfigurationInput,
        GetObjectLockConfigurationOutput, GetObjectOutput, GetObjectRetentionInput,
        GetObjectRetentionOutput, GetObjectTaggingInput, GetObjectTaggingOutput,
        GetObjectTorrentInput, GetObjectTorrentOutput, Grant, Grantee, HeadBucketInput,
        HeadBucketOutput, HeadObjectInput, HeadObjectOutput, Initiator, LifecycleExpiration,
        LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter, LifecycleRules,
        ListBucketsInput, ListBucketsOutput, ListMultipartUploadsInput, ListMultipartUploadsOutput,
        ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
        ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput, MFADelete,
        MetadataDirective, MultipartUpload, NoncurrentVersionExpiration, ObjectAttributes,
        ObjectCannedACL, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
        ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode,
        ObjectLockRule, ObjectPart, Owner, Part, Permission, PostObjectInput, PostObjectOutput,
        PutBucketAclInput, PutBucketAclOutput, PutBucketCorsInput, PutBucketCorsOutput,
        PutBucketEncryptionInput, PutBucketEncryptionOutput, PutBucketLifecycleConfigurationInput,
        PutBucketLifecycleConfigurationOutput, PutBucketTaggingInput, PutBucketTaggingOutput,
        PutBucketVersioningInput, PutBucketVersioningOutput, PutObjectAclInput, PutObjectAclOutput,
        PutObjectInput, PutObjectLegalHoldInput, PutObjectLegalHoldOutput,
        PutObjectLockConfigurationInput, PutObjectLockConfigurationOutput, PutObjectOutput,
        PutObjectRetentionInput, PutObjectRetentionOutput, PutObjectTaggingInput,
        PutObjectTaggingOutput, Range as S3Range, ServerSideEncryption,
        ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
        StorageClass, StreamingBlob, Tag, TagSet, Tagging, TaggingDirective, TaggingHeader,
        Timestamp, Type, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput,
        UploadPartOutput,
    },
    header::{
        CONTENT_MD5, CONTENT_TYPE, ETAG, X_AMZ_CONTENT_SHA256, X_AMZ_DECODED_CONTENT_LENGTH,
        X_AMZ_DELETE_MARKER, X_AMZ_REQUEST_ID, X_AMZ_TAGGING_COUNT,
    },
};
use time::OffsetDateTime;
use tokio::{
    fs,
    io::{AsyncWrite, AsyncWriteExt},
};
use uuid::Uuid;

use crate::ClientAddress;
use crate::sqlite::{
    BucketVersioningState, CompletedMultipartUploadOutcome, CompletedObjectPart, KeyMetadata,
    KeyValue, Multipart, MultipartCompletion, MultipartCompletionPart, MultipartCompletionRequest,
    MultipartPartWrite, MultipartUploadListEntry, MultipartUploadState, ObjectBodyReference,
    ObjectDeleteOutcome, ObjectTagCondition, ObjectWriteConditions, Sqlite, SqliteError,
    SqliteResult,
};

const LIST_BATCH_SIZE: i64 = 1024;
const CONTINUATION_TOKEN_PREFIX: &str = "s3ite:v1:";
const OWNER_DISPLAY_NAME: &str = "s3ite";
const OWNER_ID: &str = "s3ite";
const S3_SINGLE_REQUEST_BODY_MAX: i64 = 5_i64 * 1024 * 1024 * 1024;

struct TraceOption<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for TraceOption<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(value) = &self.0 {
            write!(formatter, "{value}")?;
        }
        Ok(())
    }
}

/// Wraps optional trace fields so missing values render as empty strings.
fn trace_option<T: fmt::Display>(value: Option<T>) -> TraceOption<T> {
    TraceOption(value)
}

/// Returns the authenticated access key for request trace fields.
fn user_id<T>(req: &S3Request<T>) -> &str {
    req.credentials
        .as_ref()
        .map_or("", |credentials| credentials.access_key.as_str())
}

/// Returns the remote client IP address for request trace fields.
fn client_ip<T>(req: &S3Request<T>) -> TraceOption<std::net::IpAddr> {
    trace_option(
        req.extensions
            .get::<ClientAddress>()
            .map(|client_address| client_address.0.ip()),
    )
}

mod bucket;
mod checksum;
mod copy;
mod cors;
mod delete;
mod error;
mod get;
mod lifecycle;
mod list;
mod metadata;
mod multipart;
mod object_lock;
mod owner;
mod post;
mod put;
mod sse;
mod tagging;

pub(crate) use self::{
    checksum::{
        ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, MultipartChecksumConfig,
        MultipartCompletionChecksum, PartChecksum, StoredChecksum, decode_base64, decode_md5_hex,
        encode_hex,
    },
    cors::{BucketCorsLayer, StoredCorsRule},
    lifecycle::{
        LifecycleExpirationHeader, StoredLifecycleAndFilter, StoredLifecycleExpiration,
        StoredLifecycleFilter, StoredLifecycleRule, StoredNoncurrentVersionExpiration,
        current_expiration_time, lifecycle_expiration_header, lifecycle_rule_matches_object,
        noncurrent_expiration_time,
    },
    object_lock::{
        StoredObjectLock, StoredObjectLockConfig, StoredObjectLockDefaultRetention,
        StoredObjectLockMode, StoredObjectRetention, object_lock_allows_lifecycle_delete,
    },
    sse::{SSE_S3_ALGORITHM_CODE, StoredSse, parse_master_key},
    tagging::StoredTag,
};

#[cfg(test)]
pub(crate) use self::checksum::FullObjectChecksum;

#[cfg(test)]
pub(crate) use self::checksum::test_support as checksum_test_support;

use self::{
    checksum::{
        ChecksumInput, ChecksumPolicy, checksum_algorithm_from_request, checksum_algorithm_output,
        checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_mode_enabled, checksum_sha1,
        checksum_sha256, checksum_struct, checksum_type_output, configured_part_checksum,
        default_crc64nvme_full_object_checksum, finish_part_checksum, finish_payload_checksum, hex,
        multipart_checksum_config, object_checksum_for_part, object_write_checksum_from_value,
        part_checksum_accumulator, payload_checksum_accumulator, validate_content_md5,
    },
    error::{
        not_found_delete_marker_error, not_modified_error, s3_error, s3_error_from_sqlite,
        s3_error_with_message,
    },
    list::{
        decode_continuation_token, decode_list_input_value, encode_continuation_token,
        encode_list_value, is_url_encoding, list_limit, output_delimiter,
    },
    metadata::{normalize_content_encoding, normalize_metadata},
    multipart::{requested_part_range, validate_part_number},
    object_lock::{
        ObjectLockWriteHeaders, delete_requires_object_lock_check, load_bucket_object_lock_config,
        object_lock_for_copied_version, object_lock_for_new_version, object_lock_legal_hold_status,
        object_lock_mode, object_lock_retain_until_date, object_lock_write_headers,
        validate_object_lock_delete,
    },
    owner::{
        BucketAclRequest, ObjectAclRequest, owner, validate_bucket_acl,
        validate_expected_bucket_owner, validate_object_acl,
    },
    sse::{
        ActiveSse, active_sse_from_stored, decrypt_stored_chunks, encrypt_value_chunks,
        read_object_body_encrypted, read_part_body_encrypted, requested_sse_s3,
        require_sse_s3_master_key, resolve_write_encryption, sse_s3_output,
        validate_no_sse_headers, validate_no_unsupported_sse_headers,
    },
    tagging::{CopyTaggingDirective, copy_tagging_directive, stored_tags_from_header},
};

/// Returns whether request headers describe an AWS `SigV4` streaming body.
fn is_aws_streaming_body(headers: &HeaderMap) -> bool {
    headers
        .get(X_AMZ_CONTENT_SHA256)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("STREAMING-"))
        || headers.contains_key(X_AMZ_DECODED_CONTENT_LENGTH)
}

/// Reads `x-amz-decoded-content-length` as a declared object-byte length.
fn decoded_content_length(headers: &HeaderMap) -> S3Result<Option<i64>> {
    let Some(value) = headers.get(X_AMZ_DECODED_CONTENT_LENGTH) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| s3_error(S3ErrorCode::InvalidArgument))?;
    value
        .parse::<i64>()
        .map(Some)
        .map_err(|_| s3_error(S3ErrorCode::InvalidArgument))
}

/// Validates an already-declared single-request object body size.
fn validate_single_request_body_size(size: i64) -> S3Result<i64> {
    if size < 0 {
        return Err(s3_error(S3ErrorCode::InvalidArgument));
    }
    if size > S3_SINGLE_REQUEST_BODY_MAX {
        return Err(s3_error(S3ErrorCode::EntityTooLarge));
    }
    Ok(size)
}

/// Validates object-write size declarations before any request bytes are read.
fn validate_declared_object_write_length(
    headers: &HeaderMap,
    content_length: Option<i64>,
) -> S3Result<i64> {
    let declared_size = if is_aws_streaming_body(headers) {
        decoded_content_length(headers)?
            .ok_or_else(|| s3_error(S3ErrorCode::MissingContentLength))?
    } else {
        content_length.ok_or_else(|| s3_error(S3ErrorCode::MissingContentLength))?
    };
    validate_single_request_body_size(declared_size)
}

#[derive(Clone, Debug)]
struct ObjectReadConditions {
    etag_matches: Option<ETagCondition>,
    modified_since: Option<OffsetDateTime>,
    etag_none_matches: Option<ETagCondition>,
    unmodified_since: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug)]
enum ObjectReadConditionFailure {
    NotModified,
    PreconditionFailed,
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
) -> Result<(), ObjectReadConditionFailure> {
    let md5 = metadata.md5.as_deref();
    if let Some(condition) = conditions.etag_matches.as_ref()
        && strong_etag_condition_matches(condition, md5).not()
    {
        return Err(ObjectReadConditionFailure::PreconditionFailed);
    }

    if let Some(condition) = conditions.etag_none_matches.as_ref() {
        if weak_etag_condition_matches(condition, md5) {
            return Err(ObjectReadConditionFailure::NotModified);
        }
    } else if let Some(since) = conditions.modified_since
        && http_date_time(metadata.last_modified) <= http_date_time(since)
    {
        return Err(ObjectReadConditionFailure::NotModified);
    }

    if conditions.etag_matches.is_none()
        && let Some(since) = conditions.unmodified_since
        && http_date_time(metadata.last_modified) > http_date_time(since)
    {
        return Err(ObjectReadConditionFailure::PreconditionFailed);
    }

    Ok(())
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

#[async_trait::async_trait]
impl S3 for Sqlite {
    /// Creates a bucket database if it does not already exist.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        bucket::create_bucket(self, req).await
    }

    /// Copies an object between buckets while applying S3 metadata-directive rules.
    #[tracing::instrument(
        skip_all,
        fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket, key = %req.input.key)
    )]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        copy::copy_object(self, req).await
    }

    /// Deletes an empty bucket database and its `SQLite` sidecar files.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        bucket::delete_bucket(self, req).await
    }

    /// Deletes a bucket's CORS configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        cors::delete_bucket_cors(self, req).await
    }

    /// Deletes a bucket's lifecycle configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        lifecycle::delete_bucket_lifecycle(self, req).await
    }

    /// Deletes a bucket's default encryption configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        bucket::delete_bucket_encryption(self, req).await
    }

    /// Deletes a bucket's tag set.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        tagging::delete_bucket_tagging(self, req).await
    }

    /// Deletes an object by key.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        delete::delete_object(self, req).await
    }

    /// Deletes all tags from an existing object version.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        tagging::delete_object_tagging(self, req).await
    }

    /// Deletes a batch of objects and returns S3 multi-delete results.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key_count = req.input.delete.objects.len()
        )
    )]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        delete::delete_objects(self, req).await
    }

    /// Returns the default bucket location for an existing bucket.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        bucket::get_bucket_location(self, req).await
    }

    /// Returns a bucket's CORS configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        cors::get_bucket_cors(self, req).await
    }

    /// Returns a bucket's lifecycle configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        lifecycle::get_bucket_lifecycle_configuration(self, req).await
    }

    /// Returns a bucket's default encryption configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        bucket::get_bucket_encryption(self, req).await
    }

    /// Returns the fixed owner ACL for an existing bucket.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_acl(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        owner::get_bucket_acl(self, req).await
    }

    /// Returns a bucket's tag set.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        tagging::get_bucket_tagging(self, req).await
    }

    /// Returns the bucket versioning state.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        bucket::get_bucket_versioning(self, req).await
    }

    /// Updates the bucket versioning state.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        bucket::put_bucket_versioning(self, req).await
    }

    /// Returns an object body and metadata, applying byte-range requests when present.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or(""),
            part_number = %trace_option(req.input.part_number),
            range = %trace_option(req.input.range.map(|range| range.to_header_string()))
        )
    )]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        get::get_object(self, req).await
    }

    /// Returns an object's legal hold metadata.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        object_lock::get_object_legal_hold(self, req).await
    }

    /// Returns a bucket's Object Lock configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        object_lock::get_object_lock_configuration(self, req).await
    }

    /// Returns an object's retention metadata.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        object_lock::get_object_retention(self, req).await
    }

    /// Returns tags for an existing object version.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        tagging::get_object_tagging(self, req).await
    }

    /// Reports that `BitTorrent` metadata is not available for the object.
    #[tracing::instrument(
        skip_all,
        fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket, key = %req.input.key)
    )]
    async fn get_object_torrent(
        &self,
        req: S3Request<GetObjectTorrentInput>,
    ) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        get::get_object_torrent(self, req)
    }

    /// Checks whether a bucket exists.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        bucket::head_bucket(self, req).await
    }

    /// Returns object metadata without reading the object body.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or(""),
            part_number = %trace_option(req.input.part_number)
        )
    )]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        get::head_object(self, req).await
    }

    /// Returns selected object attributes without returning the object body.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or(""),
            max_parts = %trace_option(req.input.max_parts),
            part_number_marker = %trace_option(req.input.part_number_marker)
        )
    )]
    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        get::get_object_attributes(self, req).await
    }

    /// Returns the fixed owner ACL for an existing object.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        owner::get_object_acl(self, req).await
    }

    /// Accepts owner-equivalent bucket ACL writes as no-ops.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_bucket_acl(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        owner::put_bucket_acl(self, req).await
    }

    /// Replaces a bucket's CORS configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        cors::put_bucket_cors(self, req).await
    }

    /// Replaces a bucket's lifecycle configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        lifecycle::put_bucket_lifecycle_configuration(self, req).await
    }

    /// Updates a bucket's default encryption configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        bucket::put_bucket_encryption(self, req).await
    }

    /// Replaces a bucket's tag set.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_bucket_tagging(
        &self,
        req: S3Request<PutBucketTaggingInput>,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        tagging::put_bucket_tagging(self, req).await
    }

    /// Lists all open bucket databases.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            prefix = %req.input.prefix.as_deref().unwrap_or(""),
            continuation_token = %req.input.continuation_token.as_deref().unwrap_or(""),
            max_buckets = %trace_option(req.input.max_buckets)
        )
    )]
    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        bucket::list_buckets(self, req).await
    }

    /// Lists objects using the original `ListObjects` marker protocol.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            prefix = %req.input.prefix.as_deref().unwrap_or(""),
            delimiter = %req.input.delimiter.as_deref().unwrap_or(""),
            marker = %req.input.marker.as_deref().unwrap_or(""),
            max_keys = %trace_option(req.input.max_keys)
        )
    )]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        list::list_objects(self, req).await
    }

    /// Lists all object versions and delete markers.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            prefix = %req.input.prefix.as_deref().unwrap_or(""),
            delimiter = %req.input.delimiter.as_deref().unwrap_or(""),
            key_marker = %req.input.key_marker.as_deref().unwrap_or(""),
            version_id_marker = %req.input.version_id_marker.as_deref().unwrap_or(""),
            max_keys = %trace_option(req.input.max_keys)
        )
    )]
    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        list::list_object_versions(self, req).await
    }

    /// Lists objects using the `ListObjectsV2` continuation-token protocol.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            prefix = %req.input.prefix.as_deref().unwrap_or(""),
            delimiter = %req.input.delimiter.as_deref().unwrap_or(""),
            continuation_token = %req.input.continuation_token.as_deref().unwrap_or(""),
            start_after = %req.input.start_after.as_deref().unwrap_or(""),
            max_keys = %trace_option(req.input.max_keys)
        )
    )]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        list::list_objects_v2(self, req).await
    }

    /// Lists active multipart uploads in a bucket.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            prefix = %req.input.prefix.as_deref().unwrap_or(""),
            delimiter = %req.input.delimiter.as_deref().unwrap_or(""),
            key_marker = %req.input.key_marker.as_deref().unwrap_or(""),
            upload_id_marker = %req.input.upload_id_marker.as_deref().unwrap_or(""),
            max_uploads = %trace_option(req.input.max_uploads)
        )
    )]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        multipart::list_multipart_uploads(self, req).await
    }

    /// Stores an object body and metadata.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            content_length = %trace_option(req.input.content_length)
        )
    )]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        put::put_object(self, req).await
    }

    /// Stores an object from a browser-form POST upload.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            content_length = %trace_option(req.input.content_length)
        )
    )]
    async fn post_object(
        &self,
        req: S3Request<PostObjectInput>,
    ) -> S3Result<S3Response<PostObjectOutput>> {
        post::post_object(self, req).await
    }

    /// Accepts owner-equivalent object ACL writes as no-ops.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        owner::put_object_acl(self, req).await
    }

    /// Replaces an object's legal hold metadata.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        object_lock::put_object_legal_hold(self, req).await
    }

    /// Replaces a bucket's Object Lock configuration.
    #[tracing::instrument(skip_all, fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        object_lock::put_object_lock_configuration(self, req).await
    }

    /// Replaces an object's retention metadata.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        object_lock::put_object_retention(self, req).await
    }

    /// Replaces tags on an existing object version.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            version_id = %req.input.version_id.as_deref().unwrap_or("")
        )
    )]
    async fn put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        tagging::put_object_tagging(self, req).await
    }

    /// Creates multipart upload state for a future object.
    #[tracing::instrument(
        skip_all,
        fields(user_id = %user_id(&req),
            client_ip = %client_ip(&req), bucket = %req.input.bucket, key = %req.input.key)
    )]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        multipart::create_multipart_upload(self, req).await
    }

    /// Stores or replaces a multipart upload part.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            upload_id = %req.input.upload_id,
            part_number = req.input.part_number,
            content_length = %trace_option(req.input.content_length)
        )
    )]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        multipart::upload_part(self, req).await
    }

    /// Copies an existing object byte range into a multipart upload part.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            upload_id = %req.input.upload_id,
            part_number = req.input.part_number,
            source_range = %req.input.copy_source_range.as_deref().unwrap_or("")
        )
    )]
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        multipart::upload_part_copy(self, req).await
    }

    /// Lists parts uploaded for an active multipart upload.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            upload_id = %req.input.upload_id,
            part_number_marker = %trace_option(req.input.part_number_marker),
            max_parts = %trace_option(req.input.max_parts)
        )
    )]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        multipart::list_parts(self, req).await
    }

    /// Aborts an active multipart upload and deletes its parts.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            upload_id = %req.input.upload_id
        )
    )]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        multipart::abort_multipart_upload(self, req).await
    }

    /// Completes a multipart upload and records the result for idempotent retries.
    #[tracing::instrument(
        skip_all,
        fields(
            user_id = %user_id(&req),
            client_ip = %client_ip(&req),
            bucket = %req.input.bucket,
            key = %req.input.key,
            upload_id = %req.input.upload_id
        )
    )]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        multipart::complete_multipart_upload(self, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use time::Duration;

    fn metadata(md5: Option<&str>, last_modified: OffsetDateTime, size: i64) -> KeyMetadata {
        KeyMetadata {
            version_id: "version".to_string(),
            size,
            metadata: None,
            content_type: None,
            content_encoding: None,
            cache_control: None,
            expires: None,
            last_modified,
            md5: md5.map(str::to_string),
            checksum: None,
            stored_sse: None,
            object_lock: StoredObjectLock::default(),
            is_latest: true,
            is_delete_marker: false,
        }
    }

    fn read_conditions(
        etag_matches: Option<ETagCondition>,
        modified_since: Option<OffsetDateTime>,
        etag_none_matches: Option<ETagCondition>,
        unmodified_since: Option<OffsetDateTime>,
    ) -> ObjectReadConditions {
        ObjectReadConditions {
            etag_matches,
            modified_since,
            etag_none_matches,
            unmodified_since,
        }
    }

    #[test]
    fn validates_declared_object_write_length_boundaries() {
        let headers = HeaderMap::new();
        assert_eq!(
            validate_declared_object_write_length(&headers, Some(S3_SINGLE_REQUEST_BODY_MAX)).ok(),
            Some(S3_SINGLE_REQUEST_BODY_MAX)
        );

        let error =
            validate_declared_object_write_length(&headers, Some(S3_SINGLE_REQUEST_BODY_MAX + 1))
                .err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::EntityTooLarge)
        ));

        let error = validate_declared_object_write_length(&headers, None).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::MissingContentLength)
        ));

        let error = validate_declared_object_write_length(&headers, Some(-1)).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
    }

    #[test]
    fn validates_aws_streaming_decoded_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_AMZ_CONTENT_SHA256,
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
                .parse()
                .unwrap_or_else(|err| panic!("valid header should parse: {err:?}")),
        );

        let error = validate_declared_object_write_length(&headers, Some(1)).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::MissingContentLength)
        ));

        headers.insert(
            X_AMZ_DECODED_CONTENT_LENGTH,
            S3_SINGLE_REQUEST_BODY_MAX
                .to_string()
                .parse()
                .unwrap_or_else(|err| panic!("valid header should parse: {err:?}")),
        );
        assert_eq!(
            validate_declared_object_write_length(&headers, None).ok(),
            Some(S3_SINGLE_REQUEST_BODY_MAX)
        );

        headers.insert(
            X_AMZ_DECODED_CONTENT_LENGTH,
            (S3_SINGLE_REQUEST_BODY_MAX + 1)
                .to_string()
                .parse()
                .unwrap_or_else(|err| panic!("valid header should parse: {err:?}")),
        );
        let error = validate_declared_object_write_length(&headers, Some(1)).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::EntityTooLarge)
        ));

        headers.insert(
            X_AMZ_DECODED_CONTENT_LENGTH,
            "not-a-number"
                .parse()
                .unwrap_or_else(|err| panic!("valid header should parse: {err:?}")),
        );
        let error = validate_declared_object_write_length(&headers, Some(1)).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
    }

    #[test]
    fn matches_strong_and_weak_etag_conditions() {
        let strong = ETagCondition::ETag(ETag::Strong("md5".to_string()));
        let strong_mismatch = ETagCondition::ETag(ETag::Strong("other".to_string()));
        let weak = ETagCondition::ETag(ETag::Weak("md5".to_string()));

        assert!(strong_etag_condition_matches(&strong, Some("md5")));
        assert!(!strong_etag_condition_matches(
            &strong_mismatch,
            Some("md5")
        ));
        assert!(!strong_etag_condition_matches(&weak, Some("md5")));
        assert!(strong_etag_condition_matches(
            &ETagCondition::Any,
            Some("md5")
        ));
        assert!(!strong_etag_condition_matches(&ETagCondition::Any, None));

        assert!(weak_etag_condition_matches(&strong, Some("md5")));
        assert!(weak_etag_condition_matches(&weak, Some("md5")));
        assert!(!weak_etag_condition_matches(&strong_mismatch, Some("md5")));
        assert!(weak_etag_condition_matches(
            &ETagCondition::Any,
            Some("md5")
        ));
        assert!(!weak_etag_condition_matches(&ETagCondition::Any, None));
    }

    #[test]
    fn maps_write_conditions_to_storage_conditions() {
        let conditions = object_write_conditions(
            Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))),
            Some(ETagCondition::ETag(ETag::Weak("none".to_string()))),
        );
        assert!(matches!(
            conditions.if_match,
            Some(ObjectTagCondition::Md5(value)) if value == "md5"
        ));
        assert!(matches!(
            conditions.if_none_match,
            Some(ObjectTagCondition::Md5(value)) if value == "none"
        ));

        let conditions = object_write_conditions(
            Some(ETagCondition::ETag(ETag::Weak("md5".to_string()))),
            Some(ETagCondition::Any),
        );
        assert!(matches!(
            conditions.if_match,
            Some(ObjectTagCondition::Never)
        ));
        assert!(matches!(
            conditions.if_none_match,
            Some(ObjectTagCondition::Any)
        ));

        let conditions = object_write_conditions(Some(ETagCondition::Any), None);
        assert!(matches!(conditions.if_match, Some(ObjectTagCondition::Any)));
        assert!(conditions.if_none_match.is_none());
    }

    #[test]
    fn applies_read_condition_precedence() {
        let last_modified = OffsetDateTime::UNIX_EPOCH + Duration::seconds(10);
        let before = last_modified - Duration::seconds(1);
        let after = last_modified + Duration::seconds(1);
        let object = metadata(Some("md5"), last_modified, 5);

        let result = validate_read_conditions(
            &object,
            &read_conditions(
                Some(ETagCondition::ETag(ETag::Strong("other".to_string()))),
                None,
                Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))),
                None,
            ),
        );
        assert!(matches!(
            result,
            Err(ObjectReadConditionFailure::PreconditionFailed)
        ));

        let result = validate_read_conditions(
            &object,
            &read_conditions(
                None,
                None,
                Some(ETagCondition::ETag(ETag::Weak("md5".to_string()))),
                None,
            ),
        );
        assert!(matches!(
            result,
            Err(ObjectReadConditionFailure::NotModified)
        ));

        assert!(
            validate_read_conditions(
                &object,
                &read_conditions(
                    None,
                    Some(after),
                    Some(ETagCondition::ETag(ETag::Strong("other".to_string()))),
                    None,
                ),
            )
            .is_ok()
        );
        assert!(
            validate_read_conditions(
                &object,
                &read_conditions(
                    Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))),
                    None,
                    None,
                    Some(before),
                ),
            )
            .is_ok()
        );
        assert!(matches!(
            validate_read_conditions(&object, &read_conditions(None, Some(after), None, None),),
            Err(ObjectReadConditionFailure::NotModified)
        ));
        assert!(matches!(
            validate_read_conditions(&object, &read_conditions(None, None, None, Some(before)),),
            Err(ObjectReadConditionFailure::PreconditionFailed)
        ));
    }
}
