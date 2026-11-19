//! S3 `CopyObject` request handling.

use std::{collections::HashMap, ops::Range as ByteRange, sync::Arc};

use crate::database::BucketConnection;
use crate::sqlite::{
    ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, EncryptionMode, KeyMetadata,
    ObjectBodyReference, ObjectChunkLocation, ObjectWriteConditions, StoredChecksum,
    StoredObjectLock, StoredSse, StoredTag,
};

use super::source::object_range_stream;
use super::sse::ActiveSse;
use super::stage::{StagedBody, stage_streaming_body};

use super::metadata::parse_expires;
use super::multipart::checked_copy_source_range;
use super::{
    CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource, CopyTaggingDirective, Digest,
    ETag, Md5, MetadataDirective, Not, ObjectAclRequest, ObjectLockWriteHeaders,
    ObjectReadConditions, OffsetDateTime, S3_SINGLE_REQUEST_BODY_MAX, S3ErrorCode, S3Range,
    S3Request, S3Response, S3Result, Sqlite, SqliteError, SqliteResult, StreamingBlob,
    WriteEncryption, checksum_algorithm_from_request, checksum_crc32, checksum_crc32c,
    checksum_crc64nvme, checksum_md5, checksum_sha1, checksum_sha256, checksum_sha512,
    checksum_type_output, checksum_xxhash3, checksum_xxhash64, checksum_xxhash128,
    copy_tagging_directive, encryption_headers, hex, http_date_time,
    load_bucket_object_lock_config, normalize_content_encoding, normalize_metadata,
    object_lock_for_new_version, object_lock_write_headers, object_write_conditions,
    open_encryption, requested_encryption, resolve_write_encryption, s3_error,
    s3_error_from_sqlite, s3_error_with_message, strong_etag_condition_matches,
    validate_expected_bucket_owner, validate_no_unsupported_sse_headers, validate_object_acl,
    weak_etag_condition_matches,
};

/// Parsed copy request with source, destination, metadata, and checksum intent.
struct CopyObjectRequest {
    target_bucket: String,
    target_key: String,
    source_bucket: String,
    source_key: String,
    source_version_id: Option<String>,
    replace_metadata: bool,
    metadata: Option<HashMap<String, String>>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_disposition: Option<String>,
    content_language: Option<String>,
    cache_control: Option<String>,
    expires: Option<OffsetDateTime>,
    source_conditions: ObjectReadConditions,
    target_conditions: ObjectWriteConditions,
    target_checksum_kind: Option<ChecksumAlgorithmKind>,
    explicit_encryption: Option<WriteEncryption>,
    tagging_directive: CopyTaggingDirective,
    object_lock_headers: ObjectLockWriteHeaders,
}

/// Source version selected for a copy: its metadata, tags, and a plan for streaming its body.
///
/// The body is never loaded whole; [`CopySourceObject::body`] streams the
/// requested plaintext range one storage chunk at a time.
pub(super) struct CopySourceObject {
    pub(super) key_metadata: KeyMetadata,
    tags: Vec<StoredTag>,
    connection: Arc<BucketConnection>,
    sequence: i64,
    locations: Vec<ObjectChunkLocation>,
    pub(super) range: ByteRange<u64>,
    encryption: Option<Arc<ActiveSse>>,
}

impl CopySourceObject {
    /// Streams the planned source bytes, decrypting each stored chunk when needed.
    ///
    /// A source version deleted while the copy streams ends the stream with `NoSuchKey`.
    pub(super) fn body(&self) -> StreamingBlob {
        object_range_stream(
            self.connection.clone(),
            self.sequence,
            self.locations.clone(),
            self.range.clone(),
            self.encryption.clone(),
        )
    }
}

/// Applies copy-source conditional request semantics to loaded source metadata.
pub(super) fn validate_copy_source_conditions(
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

/// Rejects source objects too large for single-request `CopyObject`.
fn validate_copy_source_size(metadata: &KeyMetadata) -> SqliteResult<()> {
    if metadata.size > S3_SINGLE_REQUEST_BODY_MAX {
        return Err(SqliteError::SingleRequestObjectTooLarge);
    }
    Ok(())
}

/// Converts supported S3 copy sources into bucket, key, and optional version identifiers.
fn parse_copy_source(source: CopySource) -> S3Result<(String, String, Option<String>)> {
    match source {
        CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => {
            Err(s3_error(S3ErrorCode::NotImplemented))
        }
        CopySource::Bucket {
            bucket,
            key,
            version_id,
        } => Ok((
            bucket.to_string(),
            key.to_string(),
            version_id.map(|version_id| version_id.to_string()),
        )),
    }
}

/// Normalizes an S3 `CopyObject` input into the local copy operation shape.
fn parse_copy_object_request(input: CopyObjectInput) -> S3Result<CopyObjectRequest> {
    let CopyObjectInput {
        bucket: target_bucket,
        key: target_key,
        acl,
        copy_source,
        copy_source_if_match,
        copy_source_if_modified_since,
        copy_source_if_none_match,
        copy_source_if_unmodified_since,
        checksum_algorithm,
        expires,
        if_match,
        if_none_match,
        metadata_directive,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
        tagging,
        tagging_directive,
        bucket_key_enabled,
        grant_full_control,
        grant_read,
        grant_read_acp,
        grant_write_acp,
        copy_source_sse_customer_algorithm,
        copy_source_sse_customer_key,
        copy_source_sse_customer_key_md5,
        server_side_encryption,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        ssekms_encryption_context,
        ssekms_key_id,
        ..
    } = input;

    let (source_bucket, source_key, source_version_id) = parse_copy_source(copy_source)?;
    let replace_metadata = metadata_directive
        .as_ref()
        .is_some_and(|directive| directive.as_str() == MetadataDirective::REPLACE);
    let tagging_directive = copy_tagging_directive(tagging_directive.as_ref(), tagging)?;
    let target_checksum_kind = checksum_algorithm_from_request(checksum_algorithm)?;
    let object_lock_headers = object_lock_write_headers(
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    )?;
    let explicit_encryption = requested_encryption(
        server_side_encryption.as_ref(),
        ssekms_key_id.as_deref(),
        ssekms_encryption_context.as_deref(),
        sse_customer_algorithm.is_some()
            || sse_customer_key.is_some()
            || sse_customer_key_md5.is_some()
            || copy_source_sse_customer_algorithm.is_some()
            || copy_source_sse_customer_key.is_some()
            || copy_source_sse_customer_key_md5.is_some(),
        bucket_key_enabled.is_some(),
    )?;
    validate_object_acl(&ObjectAclRequest {
        acl: acl.as_ref(),
        has_access_control_policy: false,
        grant_header_count: [
            grant_full_control.is_some(),
            grant_read.is_some(),
            grant_read_acp.is_some(),
            grant_write_acp.is_some(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count(),
    })?;

    Ok(CopyObjectRequest {
        target_bucket,
        target_key,
        source_bucket,
        source_key,
        source_version_id,
        replace_metadata,
        metadata: normalize_metadata(input.metadata),
        content_type: input.content_type,
        content_encoding: normalize_content_encoding(input.content_encoding),
        content_disposition: input.content_disposition,
        content_language: input.content_language,
        cache_control: input.cache_control,
        expires: parse_expires(expires.as_deref())?,
        target_conditions: object_write_conditions(if_match, if_none_match, false)?,
        source_conditions: ObjectReadConditions {
            etag_matches: copy_source_if_match,
            modified_since: copy_source_if_modified_since.map(Into::into),
            etag_none_matches: copy_source_if_none_match,
            unmodified_since: copy_source_if_unmodified_since.map(Into::into),
        },
        target_checksum_kind,
        explicit_encryption,
        tagging_directive,
        object_lock_headers,
    })
}

/// Rejects same-key copies of an implicit source version without metadata or encryption changes.
fn validate_copy_target(request: &CopyObjectRequest) -> S3Result<()> {
    if request.source_bucket.as_str() == request.target_bucket.as_str()
        && request.source_key.as_str() == request.target_key.as_str()
        && request.source_version_id.is_none()
        && request.explicit_encryption.is_none()
        && request.replace_metadata.not()
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "This copy request is illegal because it is trying to copy an object to itself \
             without changing the object's metadata, storage class, website redirect location \
             or encryption attributes.",
        ));
    }
    Ok(())
}

/// Rewrites loaded source metadata for the destination object.
///
/// The checksum is left for the caller to compute while streaming the body;
/// see [`copy_checksum_kind`] for the algorithm the copy must use.
fn prepare_copied_object(
    mut metadata: KeyMetadata,
    request: &CopyObjectRequest,
    target_stored_sse: Option<StoredSse>,
    target_object_lock: StoredObjectLock,
) -> KeyMetadata {
    metadata.last_modified = OffsetDateTime::now_utc();
    if request.replace_metadata {
        metadata.metadata.clone_from(&request.metadata);
        metadata.content_type.clone_from(&request.content_type);
        metadata
            .content_encoding
            .clone_from(&request.content_encoding);
        metadata
            .content_disposition
            .clone_from(&request.content_disposition);
        metadata
            .content_language
            .clone_from(&request.content_language);
        metadata.cache_control.clone_from(&request.cache_control);
        metadata.expires = request.expires;
    }
    metadata.checksum = None;
    metadata.stored_sse = target_stored_sse;
    metadata.object_lock = target_object_lock;
    metadata
}

/// Chooses the copy's full-object checksum algorithm.
///
/// An explicit request algorithm wins, then the source's algorithm, then the
/// `CRC64NVME` default.
fn copy_checksum_kind(request: &CopyObjectRequest, source: &KeyMetadata) -> ChecksumAlgorithmKind {
    request
        .target_checksum_kind
        .or_else(|| source.checksum.as_ref().map(StoredChecksum::algorithm))
        .unwrap_or(ChecksumAlgorithmKind::Crc64Nvme)
}

/// Reads the source version's metadata, tags, and chunk plan, then unwraps its data key.
///
/// Conditions are checked in the same read transaction that selects the
/// version, so the streamed body belongs to the version they were checked
/// against.
pub(super) async fn load_copy_source_object(
    sqlite: &Sqlite,
    bucket: &str,
    key: &str,
    version_id: Option<String>,
    conditions: ObjectReadConditions,
    requested_range: Option<S3Range>,
) -> S3Result<CopySourceObject> {
    let source_key = key.to_owned();
    let connection = sqlite
        .get_connection(bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (key_metadata, sequence, locations, tags, range) = connection
        .read(move |transaction| {
            let source = match version_id {
                Some(id) => {
                    Sqlite::get_object_version_body_reference(transaction, &source_key, &id)?
                        .ok_or(SqliteError::ObjectVersionNotFound)?
                }
                None => Sqlite::get_current_object_body_reference(transaction, &source_key)?
                    .ok_or(SqliteError::ObjectNotFound)?,
            };
            let ObjectBodyReference {
                sequence,
                key_metadata,
                ..
            } = source;
            if key_metadata.is_delete_marker {
                return Err(SqliteError::ObjectNotFound);
            }
            validate_copy_source_conditions(&key_metadata, &conditions)?;
            let size = u64::try_from(key_metadata.size)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let range = checked_copy_source_range(requested_range, size)?;
            let locations = if requested_range.is_some() {
                Sqlite::object_chunk_locations(transaction, sequence, range.clone())?
            } else {
                Sqlite::whole_object_chunk_locations(transaction, sequence, size)?
            };
            let tags = Sqlite::get_object_tags(transaction, sequence)?;
            Ok((key_metadata, sequence, locations, tags, range))
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    let encryption = match key_metadata.stored_sse.clone() {
        Some(stored) => Some(Arc::new(
            open_encryption(sqlite, bucket, key, stored).await?,
        )),
        None => None,
    };
    Ok(CopySourceObject {
        key_metadata,
        tags,
        connection,
        sequence,
        locations,
        range,
        encryption,
    })
}

/// Builds the S3 copy result from the stored target object metadata.
fn copy_object_result(
    checksum: &StoredChecksum,
    md5: Option<String>,
    last_modified: OffsetDateTime,
) -> CopyObjectResult {
    let checksum_type = checksum_type_output(checksum);
    CopyObjectResult {
        checksum_crc32: checksum_crc32(checksum),
        checksum_crc32c: checksum_crc32c(checksum),
        checksum_crc64nvme: checksum_crc64nvme(checksum),
        checksum_sha1: checksum_sha1(checksum),
        checksum_sha256: checksum_sha256(checksum),
        checksum_sha512: checksum_sha512(checksum),
        checksum_md5: checksum_md5(checksum),
        checksum_xxhash3: checksum_xxhash3(checksum),
        checksum_xxhash64: checksum_xxhash64(checksum),
        checksum_xxhash128: checksum_xxhash128(checksum),
        checksum_type,
        e_tag: md5.map(ETag::Strong),
        last_modified: Some(last_modified.into()),
    }
}

/// Streams a copy's body into staged target chunks while computing its checksum.
///
/// The source is decrypted and the target encrypted one chunk at a time. A
/// stream failure, such as the source being deleted mid-copy, or a size
/// mismatch discards the staged chunks; the source's own error, such as
/// `NoSuchKey`, is returned.
///
/// Also returns the hex MD5 of the copied bytes. `CopyObject` always creates a
/// single-part object, and AWS gives copies an `ETag` that is the MD5 of their
/// data, so a copy of a multipart object must not keep the source's `-N` `ETag`.
async fn stage_copy_body(
    source: &CopySourceObject,
    target_connection: Arc<BucketConnection>,
    chunk_size: usize,
    target_encryption: Option<&ActiveSse>,
    checksum_kind: ChecksumAlgorithmKind,
) -> S3Result<(StagedBody, StoredChecksum, String)> {
    let mut accumulator = ChecksumAccumulator::new(checksum_kind);
    let mut md5 = Md5::new();
    let staged = stage_streaming_body(
        target_connection,
        source.body(),
        chunk_size,
        source.key_metadata.size,
        1,
        target_encryption,
        |bytes| {
            accumulator.update(bytes);
            md5.update(bytes);
        },
    )
    .await?;
    if let Some(checksum) = accumulator.finish_stored(ChecksumTypeKind::FullObject) {
        return Ok((staged, checksum, hex(md5.finalize())));
    }
    staged.discard();
    Err(s3_error_from_sqlite(SqliteError::ChecksumMismatch))
}

/// Streams a source version into a new target version.
///
/// The source size limit is enforced before any body bytes are read. The
/// destination conditions are checked in the transaction that claims the
/// staged body and installs the copied version and its tags.
async fn copy_object_data(
    sqlite: &Sqlite,
    request: CopyObjectRequest,
) -> S3Result<(CopyObjectResult, Option<String>, Option<EncryptionMode>)> {
    let source = load_copy_source_object(
        sqlite,
        &request.source_bucket,
        &request.source_key,
        request.source_version_id.clone(),
        request.source_conditions.clone(),
        None,
    )
    .await?;
    validate_copy_source_size(&source.key_metadata).map_err(s3_error_from_sqlite)?;
    let target_tags = match &request.tagging_directive {
        CopyTaggingDirective::Copy => source.tags.clone(),
        CopyTaggingDirective::Replace(tags) => tags.clone(),
    };
    let target_encryption = resolve_write_encryption(
        sqlite,
        &request.target_bucket,
        &request.target_key,
        request.explicit_encryption.as_ref(),
    )
    .await?;
    let target_object_lock_config =
        load_bucket_object_lock_config(sqlite, &request.target_bucket).await?;
    let encrypted = target_encryption
        .as_ref()
        .map(|active| active.stored.mode());
    // S3 never copies the source version's Object Lock settings: the copy is a
    // new version locked only by request headers or the destination default.
    let target_object_lock = object_lock_for_new_version(
        &target_object_lock_config,
        request.object_lock_headers.clone(),
        OffsetDateTime::now_utc(),
    )?;
    let target_connection = sqlite
        .get_connection(&request.target_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (staged, checksum, md5) = stage_copy_body(
        &source,
        target_connection.clone(),
        sqlite.blob_chunk_size(),
        target_encryption.as_ref(),
        copy_checksum_kind(&request, &source.key_metadata),
    )
    .await?;
    let mut metadata = prepare_copied_object(
        source.key_metadata,
        &request,
        target_encryption
            .as_ref()
            .map(|encryption| encryption.stored.clone()),
        target_object_lock,
    );
    metadata.checksum = Some(checksum.clone());
    metadata.md5 = Some(md5);
    let staging_id = staged.staging_id();
    let written = target_connection
        .write(move |transaction| {
            Sqlite::check_object_write_conditions(
                transaction,
                &request.target_key,
                &request.target_conditions,
            )?;
            let write_outcome = Sqlite::put_object_with_staged_chunks(
                transaction,
                &request.target_key,
                metadata,
                staging_id,
            )?;
            Sqlite::replace_object_tags(transaction, write_outcome.sequence, &target_tags)?;
            let result = copy_object_result(
                &checksum,
                write_outcome.md5.clone(),
                write_outcome.last_modified,
            );
            Ok((result, write_outcome.returned_version_id, encrypted))
        })
        .await
        .map_err(s3_error_from_sqlite);
    match written {
        Ok(written) => {
            staged.claimed();
            Ok(written)
        }
        Err(err) => {
            staged.discard();
            Err(err)
        }
    }
}

/// Copies an object between buckets while applying S3 metadata-directive rules.
pub(super) async fn copy_object(
    sqlite: &Sqlite,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    let S3Request { input, headers, .. } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_expected_bucket_owner(input.expected_source_bucket_owner.as_deref())?;
    validate_no_unsupported_sse_headers(&headers)?;
    let ssekms_encryption_context = input.ssekms_encryption_context.clone();
    let request = parse_copy_object_request(input)?;

    sqlite
        .validate_mutable_bucket(&request.target_bucket)
        .map_err(s3_error_from_sqlite)?;
    validate_copy_target(&request)?;

    let copy_source_version_id = request.source_version_id.clone();
    let (copy_object_result, returned_version_id, encrypted) =
        copy_object_data(sqlite, request).await?;

    let (server_side_encryption, ssekms_key_id) = encryption_headers(encrypted);
    let output = CopyObjectOutput {
        copy_object_result: Some(copy_object_result),
        copy_source_version_id,
        ssekms_encryption_context,
        server_side_encryption,
        ssekms_key_id,
        version_id: returned_version_id,
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sqlite::{ChecksumAlgorithmKind, FullObjectChecksum, KeyMetadata, StoredObjectLock};
    use s3s::dto::{
        ChecksumAlgorithm, ETagCondition, ObjectCannedACL, ObjectLockLegalHoldStatus,
        ObjectLockMode, Timestamp,
    };
    use time::Duration;

    fn assert_error_code<T>(result: S3Result<T>, code: &S3ErrorCode) {
        match result {
            Ok(_) => panic!("expected {code:?}"),
            Err(err) => assert_eq!(err.code(), code),
        }
    }

    fn source(bucket: &str, key: &str, version_id: Option<&str>) -> CopySource {
        CopySource::Bucket {
            bucket: bucket.into(),
            key: key.into(),
            version_id: version_id.map(Into::into),
        }
    }

    /// Builds an unsupported access point copy source for parser rejection tests.
    fn access_point_source() -> CopySource {
        CopySource::AccessPoint {
            partition: "aws".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            access_point_name: "ap".into(),
            key: "key".into(),
            version_id: None,
        }
    }

    fn copy_input() -> CopyObjectInput {
        match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(source(
                "source-bucket",
                "source-key",
                Some("source-version"),
            ))
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        }
    }

    /// Builds copy source metadata with user metadata and the requested checksum.
    fn object(checksum: Option<StoredChecksum>) -> KeyMetadata {
        KeyMetadata {
            version_id: "source-version".to_string(),
            size: 7,
            metadata: Some(HashMap::from([("old".to_string(), "value".to_string())])),
            content_type: Some("text/plain".to_string()),
            content_encoding: Some("gzip".to_string()),
            content_disposition: None,
            content_language: None,
            cache_control: Some("old-cache".to_string()),
            expires: Some(OffsetDateTime::UNIX_EPOCH),
            last_modified: OffsetDateTime::UNIX_EPOCH,
            md5: Some("md5".to_string()),
            checksum,
            stored_sse: None,
            object_lock: StoredObjectLock::default(),
            is_latest: true,
            is_delete_marker: false,
        }
    }

    /// Builds a five-byte source fixture with the given MD5 and modification time.
    fn metadata(md5: Option<&str>, last_modified: OffsetDateTime) -> KeyMetadata {
        let mut metadata = object(None);
        metadata.md5 = md5.map(str::to_string);
        metadata.last_modified = last_modified;
        metadata.size = 5;
        metadata
    }

    fn source_conditions(
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

    fn assert_precondition_failed(result: &SqliteResult<()>) {
        assert!(matches!(result, Err(SqliteError::PreconditionFailed)));
    }

    #[test]
    #[should_panic(expected = "expected")]
    fn assert_error_code_panics_on_success() {
        assert_error_code(Ok(()), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn assert_precondition_failed_panics_on_success() {
        assert_precondition_failed(&Ok(()));
    }

    #[test]
    fn parses_copy_object_request_defaults() {
        let request = match parse_copy_object_request(copy_input()) {
            Ok(request) => request,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert_eq!(request.target_bucket, "target-bucket");
        assert_eq!(request.target_key, "target-key");
        assert_eq!(request.source_bucket, "source-bucket");
        assert_eq!(request.source_key, "source-key");
        assert_eq!(request.source_version_id.as_deref(), Some("source-version"));
        assert!(!request.replace_metadata);
        assert!(request.metadata.is_none());
        assert!(request.content_type.is_none());
        assert!(request.content_encoding.is_none());
        assert!(request.cache_control.is_none());
        assert!(request.expires.is_none());
        assert!(request.target_checksum_kind.is_none());
        assert!(request.source_conditions.etag_matches.is_none());
        assert!(request.source_conditions.modified_since.is_none());
        assert!(request.source_conditions.etag_none_matches.is_none());
        assert!(request.source_conditions.unmodified_since.is_none());
    }

    /// Checks that COPY metadata and conditions map to the storage request.
    #[test]
    fn parses_copy_object_replace_metadata_and_conditions() {
        let expires = "Thu, 01 Jan 1970 00:00:00 GMT".to_string();
        let retain_until = Timestamp::from(OffsetDateTime::now_utc() + Duration::days(1));
        let input = match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(source("source-bucket", "source-key", None))
            .metadata_directive(Some(MetadataDirective::from_static(
                MetadataDirective::REPLACE,
            )))
            .metadata(Some(HashMap::from([(
                "new".to_string(),
                "value".to_string(),
            )])))
            .content_type(Some("application/json".to_string()))
            .content_encoding(Some("gzip, aws-chunked".to_string()))
            .cache_control(Some("max-age=60".to_string()))
            .expires(Some(expires))
            .checksum_algorithm(Some(ChecksumAlgorithm::from_static(
                ChecksumAlgorithm::SHA256,
            )))
            .copy_source_if_match(Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))))
            .copy_source_if_none_match(Some(ETagCondition::ETag(ETag::Weak("other".to_string()))))
            .copy_source_if_modified_since(Some(Timestamp::from(OffsetDateTime::UNIX_EPOCH)))
            .copy_source_if_unmodified_since(Some(Timestamp::from(OffsetDateTime::UNIX_EPOCH)))
            .object_lock_legal_hold_status(Some(ObjectLockLegalHoldStatus::from_static(
                ObjectLockLegalHoldStatus::ON,
            )))
            .object_lock_mode(Some(ObjectLockMode::from_static(
                ObjectLockMode::GOVERNANCE,
            )))
            .object_lock_retain_until_date(Some(retain_until))
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        };

        let request = match parse_copy_object_request(input) {
            Ok(request) => request,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert!(request.replace_metadata);
        assert_eq!(
            request
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("new")),
            Some(&"value".to_string())
        );
        assert_eq!(request.content_type.as_deref(), Some("application/json"));
        assert_eq!(request.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(request.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(request.expires, Some(OffsetDateTime::UNIX_EPOCH));
        assert_eq!(
            request.target_checksum_kind,
            Some(ChecksumAlgorithmKind::Sha256)
        );
        assert!(matches!(
            request.source_conditions.etag_matches,
            Some(ETagCondition::ETag(ETag::Strong(value))) if value == "md5"
        ));
        assert!(request.source_conditions.modified_since.is_some());
        assert!(matches!(
            request.source_conditions.etag_none_matches,
            Some(ETagCondition::ETag(ETag::Weak(value))) if value == "other"
        ));
        assert!(request.source_conditions.unmodified_since.is_some());
        assert!(request.object_lock_headers.has_any());
    }

    #[test]
    fn rejects_unsupported_copy_inputs() {
        let access_point = match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(access_point_source())
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        };
        assert_error_code(
            parse_copy_object_request(access_point),
            &S3ErrorCode::NotImplemented,
        );

        let public_acl = match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(source("source-bucket", "source-key", None))
            .acl(Some(ObjectCannedACL::from_static(
                ObjectCannedACL::PUBLIC_READ,
            )))
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        };
        assert_error_code(
            parse_copy_object_request(public_acl),
            &S3ErrorCode::AccessControlListNotSupported,
        );

        let grant_header = match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(source("source-bucket", "source-key", None))
            .grant_read(Some("id=owner".to_string()))
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        };
        assert_error_code(
            parse_copy_object_request(grant_header),
            &S3ErrorCode::AccessControlListNotSupported,
        );

        let unsupported_sse = match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(source("source-bucket", "source-key", None))
            .ssekms_key_id(Some("key".to_string()))
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        };
        assert_error_code(
            parse_copy_object_request(unsupported_sse),
            &S3ErrorCode::InvalidArgument,
        );
    }

    /// Checks same-key copy validation with and without an explicit source version.
    #[test]
    fn validates_copy_target_collisions() {
        let mut request = match parse_copy_object_request(copy_input()) {
            Ok(request) => request,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert!(validate_copy_target(&request).is_ok());
        request.source_bucket.clone_from(&request.target_bucket);
        request.source_key.clone_from(&request.target_key);
        request.source_version_id = None;
        assert_error_code(validate_copy_target(&request), &S3ErrorCode::InvalidRequest);

        request.source_version_id = Some("old-version".into());
        assert!(validate_copy_target(&request).is_ok());
        request.source_version_id = None;

        request.replace_metadata = true;
        assert!(validate_copy_target(&request).is_ok());
    }

    #[test]
    fn applies_copy_source_condition_precedence() {
        let last_modified = OffsetDateTime::UNIX_EPOCH + Duration::seconds(10);
        let before = last_modified - Duration::seconds(1);
        let after = last_modified + Duration::seconds(1);
        let object = metadata(Some("md5"), last_modified);

        assert_precondition_failed(&validate_copy_source_conditions(
            &object,
            &source_conditions(
                Some(ETagCondition::ETag(ETag::Strong("other".to_string()))),
                None,
                Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))),
                None,
            ),
        ));
        assert_precondition_failed(&validate_copy_source_conditions(
            &object,
            &source_conditions(
                None,
                None,
                Some(ETagCondition::ETag(ETag::Weak("md5".to_string()))),
                None,
            ),
        ));
        assert!(
            validate_copy_source_conditions(
                &object,
                &source_conditions(
                    None,
                    Some(after),
                    Some(ETagCondition::ETag(ETag::Strong("other".to_string()))),
                    None,
                ),
            )
            .is_ok()
        );
        assert!(
            validate_copy_source_conditions(
                &object,
                &source_conditions(
                    Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))),
                    None,
                    None,
                    Some(before),
                ),
            )
            .is_ok()
        );
        assert_precondition_failed(&validate_copy_source_conditions(
            &object,
            &source_conditions(None, Some(after), None, None),
        ));
        assert_precondition_failed(&validate_copy_source_conditions(
            &object,
            &source_conditions(None, None, None, Some(before)),
        ));
    }

    #[test]
    fn rejects_copy_source_over_single_request_limit() {
        let mut metadata = metadata(Some("md5"), OffsetDateTime::UNIX_EPOCH);
        metadata.size = S3_SINGLE_REQUEST_BODY_MAX;
        assert!(validate_copy_source_size(&metadata).is_ok());

        metadata.size = S3_SINGLE_REQUEST_BODY_MAX + 1;
        assert!(matches!(
            validate_copy_source_size(&metadata),
            Err(SqliteError::SingleRequestObjectTooLarge)
        ));
    }

    #[test]
    fn prepares_copied_object_with_inherited_metadata_and_source_checksum_algorithm() {
        let mut request = match parse_copy_object_request(copy_input()) {
            Ok(request) => request,
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        request.target_key = "target-key".to_string();
        let source_checksum = StoredChecksum::Full(FullObjectChecksum::Sha256("old".to_string()));

        let source = object(Some(source_checksum));
        let kind = copy_checksum_kind(&request, &source);
        let copied = prepare_copied_object(source, &request, None, StoredObjectLock::default());

        assert_eq!(
            copied
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("old")),
            Some(&"value".to_string())
        );
        assert_eq!(copied.content_type.as_deref(), Some("text/plain"));
        assert_eq!(kind, ChecksumAlgorithmKind::Sha256);
        assert_eq!(copied.checksum, None);
    }

    #[test]
    fn prepares_copied_object_with_replaced_metadata_and_default_checksum() {
        let input = match CopyObjectInput::builder()
            .bucket("target-bucket".to_string())
            .key("target-key".to_string())
            .copy_source(source("source-bucket", "source-key", None))
            .metadata_directive(Some(MetadataDirective::from_static(
                MetadataDirective::REPLACE,
            )))
            .metadata(Some(HashMap::from([(
                "new".to_string(),
                "value".to_string(),
            )])))
            .content_type(Some("application/json".to_string()))
            .content_encoding(Some("gzip, aws-chunked".to_string()))
            .cache_control(Some("max-age=60".to_string()))
            .build()
        {
            Ok(input) => input,
            Err(err) => panic!("unexpected build error: {err:?}"),
        };
        let request = match parse_copy_object_request(input) {
            Ok(request) => request,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        let source = object(None);
        let kind = copy_checksum_kind(&request, &source);
        let copied = prepare_copied_object(source, &request, None, StoredObjectLock::default());

        assert_eq!(
            copied
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("new")),
            Some(&"value".to_string())
        );
        assert_eq!(copied.content_type.as_deref(), Some("application/json"));
        assert_eq!(copied.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(copied.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(kind, ChecksumAlgorithmKind::Crc64Nvme);
    }

    #[test]
    fn builds_copy_object_result_checksum_fields() {
        let checksum = StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("crc64".to_string()));
        let result = copy_object_result(
            &checksum,
            Some("md5".to_string()),
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(result.e_tag, Some(ETag::Strong("md5".to_string())));
        assert_eq!(result.checksum_crc64nvme.as_deref(), Some("crc64"));
        assert!(result.checksum_sha256.is_none());
        assert!(result.checksum_type.is_some());
        assert!(result.last_modified.is_some());
    }
}
