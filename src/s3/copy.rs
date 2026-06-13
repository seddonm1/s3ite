//! S3 `CopyObject` request handling.

use std::collections::HashMap;

use crate::sqlite::{
    ChecksumAlgorithmKind, KeyMetadata, KeyValue, ObjectBodyReference, StoredChecksum,
    StoredObjectLock, StoredSse, StoredTag,
};

use super::{
    CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource, CopyTaggingDirective, ETag,
    InternalError, MetadataDirective, Not, ObjectAclRequest, ObjectLockWriteHeaders,
    ObjectReadConditions, OffsetDateTime, S3_SINGLE_REQUEST_BODY_MAX, S3ErrorCode, S3Request,
    S3Response, S3Result, Sqlite, SqliteError, SqliteResult, active_sse_from_stored,
    checksum_algorithm_from_request, checksum_crc32, checksum_crc32c, checksum_crc64nvme,
    checksum_sha1, checksum_sha256, checksum_type_output, copy_tagging_directive,
    decrypt_stored_chunks, encrypt_value_chunks, http_date_time, load_bucket_object_lock_config,
    normalize_content_encoding, normalize_metadata, object_lock_for_copied_version,
    object_lock_write_headers, object_write_checksum_from_value, requested_sse_s3,
    resolve_write_encryption, s3_error, s3_error_from_sqlite, s3_error_with_message, sse_s3_output,
    strong_etag_condition_matches, validate_expected_bucket_owner,
    validate_no_unsupported_sse_headers, validate_object_acl, weak_etag_condition_matches,
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
    cache_control: Option<String>,
    expires: Option<OffsetDateTime>,
    source_conditions: ObjectReadConditions,
    target_checksum_kind: Option<ChecksumAlgorithmKind>,
    explicit_sse_s3: bool,
    tagging_directive: CopyTaggingDirective,
    object_lock_headers: ObjectLockWriteHeaders,
}

/// Source object body plus tags used by `CopyObject`.
struct CopySourceObject {
    object: KeyValue,
    tags: Vec<StoredTag>,
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

fn parse_copy_source(source: CopySource) -> S3Result<(String, String, Option<String>)> {
    match source {
        CopySource::AccessPoint { .. } => Err(s3_error(S3ErrorCode::NotImplemented)),
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
        cache_control,
        content_encoding,
        content_type,
        copy_source,
        copy_source_if_match,
        copy_source_if_modified_since,
        copy_source_if_none_match,
        copy_source_if_unmodified_since,
        checksum_algorithm,
        expires,
        metadata,
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
    let explicit_sse_s3 = requested_sse_s3(
        server_side_encryption.as_ref(),
        ssekms_key_id.is_some() || ssekms_encryption_context.is_some(),
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
        metadata: normalize_metadata(metadata),
        content_type,
        content_encoding: normalize_content_encoding(content_encoding),
        cache_control,
        expires: expires.map(Into::into),
        source_conditions: ObjectReadConditions {
            etag_matches: copy_source_if_match,
            modified_since: copy_source_if_modified_since.map(Into::into),
            etag_none_matches: copy_source_if_none_match,
            unmodified_since: copy_source_if_unmodified_since.map(Into::into),
        },
        target_checksum_kind,
        explicit_sse_s3,
        tagging_directive,
        object_lock_headers,
    })
}

/// Rejects invalid destination/source combinations before opening transactions.
fn validate_copy_target(request: &CopyObjectRequest) -> S3Result<()> {
    if request.source_bucket.as_str() == request.target_bucket.as_str()
        && request.source_key.as_str() == request.target_key.as_str()
        && request.replace_metadata.not()
        && !matches!(request.tagging_directive, CopyTaggingDirective::Replace(_))
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "copy source must be different from target unless replacing metadata or tags",
        ));
    }
    Ok(())
}

/// Rewrites loaded source metadata for the destination object.
fn prepare_copied_object(
    mut object: KeyValue,
    request: &CopyObjectRequest,
    target_stored_sse: Option<StoredSse>,
    target_object_lock: StoredObjectLock,
) -> SqliteResult<(KeyValue, StoredChecksum)> {
    object.key.clone_from(&request.target_key);
    object.key_metadata.last_modified = OffsetDateTime::now_utc();
    if request.replace_metadata {
        object.key_metadata.metadata.clone_from(&request.metadata);
        object
            .key_metadata
            .content_type
            .clone_from(&request.content_type);
        object
            .key_metadata
            .content_encoding
            .clone_from(&request.content_encoding);
        object
            .key_metadata
            .cache_control
            .clone_from(&request.cache_control);
        object.key_metadata.expires = request.expires;
    }
    let checksum_kind = request
        .target_checksum_kind
        .or_else(|| {
            object
                .key_metadata
                .checksum
                .as_ref()
                .map(StoredChecksum::algorithm)
        })
        .unwrap_or(ChecksumAlgorithmKind::Crc64Nvme);
    let checksum = object_write_checksum_from_value(checksum_kind, &object)?;
    object.key_metadata.checksum = Some(checksum.clone());
    object.key_metadata.stored_sse = target_stored_sse;
    object.key_metadata.object_lock = target_object_lock;
    Ok((object, checksum))
}

/// Loads a copy source body, decrypting SSE-S3 chunks when needed.
pub(super) fn load_copy_source_object(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
    sse_s3_master_key: Option<&[u8; 32]>,
) -> SqliteResult<KeyValue> {
    let (object, _) =
        load_copy_source_object_with_sequence(transaction, key, version_id, sse_s3_master_key)?;
    Ok(object)
}

/// Loads a copy source object and its tags for `CopyObject`.
fn load_copy_source_object_and_tags(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
    sse_s3_master_key: Option<&[u8; 32]>,
) -> SqliteResult<CopySourceObject> {
    let (object, sequence) =
        load_copy_source_object_with_sequence(transaction, key, version_id, sse_s3_master_key)?;
    let tags = Sqlite::get_object_tags(transaction, sequence)?;
    Ok(CopySourceObject { object, tags })
}

fn load_copy_source_object_with_sequence(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
    sse_s3_master_key: Option<&[u8; 32]>,
) -> SqliteResult<(KeyValue, i64)> {
    let object = match version_id {
        Some(version_id) => {
            Sqlite::get_object_version_body_reference(transaction, key, version_id)?
                .ok_or(SqliteError::ObjectVersionNotFound)?
        }
        None => Sqlite::get_current_object_body_reference(transaction, key)?
            .ok_or(SqliteError::ObjectNotFound)?,
    };
    let ObjectBodyReference {
        sequence,
        key_metadata,
        ..
    } = object;
    if key_metadata.is_delete_marker {
        return Err(SqliteError::ObjectNotFound);
    }

    let value = if let Some(stored) = key_metadata.stored_sse.clone() {
        let master_key = sse_s3_master_key.ok_or_else(|| {
            SqliteError::Configuration(
                "SSE-S3 master key is required to read encrypted objects".to_string(),
            )
        })?;
        let active =
            active_sse_from_stored(master_key, stored).map_err(SqliteError::Configuration)?;
        let chunks = Sqlite::get_object_blob_chunks(transaction, sequence, key_metadata.size)?;
        decrypt_stored_chunks(&active, chunks)?
    } else {
        Sqlite::get_object_bytes(transaction, sequence, key_metadata.size)?
    };

    Ok((
        KeyValue {
            key: key.to_string(),
            value: Some(value),
            key_metadata,
        },
        sequence,
    ))
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
        checksum_type,
        e_tag: md5.map(ETag::Strong),
        last_modified: Some(last_modified.into()),
    }
}

/// Copies within one bucket while keeping body/checksum work off the writer.
async fn copy_object_same_bucket(
    sqlite: &Sqlite,
    request: CopyObjectRequest,
) -> S3Result<(CopyObjectResult, Option<String>, bool)> {
    let source_key = request.source_key.clone();
    let source_version_id = request.source_version_id.clone();
    let source_conditions = request.source_conditions.clone();
    let sse_s3_master_key = sqlite.config.sse_s3_master_key();
    let object = {
        let connection = sqlite
            .get_connection(&request.target_bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .read(move |transaction| {
                let source = load_copy_source_object_and_tags(
                    transaction,
                    &source_key,
                    source_version_id.as_deref(),
                    sse_s3_master_key.as_ref(),
                )?;
                validate_copy_source_conditions(&source.object.key_metadata, &source_conditions)?;
                validate_copy_source_size(&source.object.key_metadata)?;
                Ok(source)
            })
            .await
            .map_err(s3_error_from_sqlite)?
    };

    let target_tags = match &request.tagging_directive {
        CopyTaggingDirective::Copy => object.tags.clone(),
        CopyTaggingDirective::Replace(tags) => tags.clone(),
    };
    let target_encryption =
        resolve_write_encryption(sqlite, &request.target_bucket, request.explicit_sse_s3).await?;
    let target_object_lock_config =
        load_bucket_object_lock_config(sqlite, &request.target_bucket).await?;
    let encrypted = target_encryption.is_some();
    let chunk_size = sqlite.blob_chunk_size();
    let target_stored_sse = target_encryption
        .as_ref()
        .map(|encryption| encryption.stored.clone());
    let target_object_lock = object_lock_for_copied_version(
        &object.object.key_metadata.object_lock,
        &target_object_lock_config,
        request.object_lock_headers.clone(),
        OffsetDateTime::now_utc(),
    )?;
    let (object, checksum) = prepare_copied_object(
        object.object,
        &request,
        target_stored_sse,
        target_object_lock,
    )
    .map_err(s3_error_from_sqlite)?;
    let encrypted_chunks = target_encryption
        .as_ref()
        .map(|encryption| {
            let value = object.value.as_deref().ok_or_else(|| {
                s3_error_with_message(InternalError, "copy source body is missing")
            })?;
            encrypt_value_chunks(value, chunk_size, encryption)
        })
        .transpose()?;
    let connection = sqlite
        .get_connection(&request.target_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            let write_outcome = if let Some(chunks) = encrypted_chunks {
                let KeyValue {
                    key, key_metadata, ..
                } = object;
                Sqlite::put_object_with_encoded_chunks(transaction, &key, key_metadata, chunks)?
            } else {
                Sqlite::put_object_with_chunk_size(transaction, object, chunk_size)?
            };
            Sqlite::replace_object_tags(transaction, write_outcome.sequence, &target_tags)?;
            let result = copy_object_result(
                &checksum,
                write_outcome.md5.clone(),
                write_outcome.last_modified,
            );
            Ok((result, write_outcome.returned_version_id, encrypted))
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Copies across buckets by reading source state before writing the target bucket.
async fn copy_object_cross_bucket(
    sqlite: &Sqlite,
    request: CopyObjectRequest,
) -> S3Result<(CopyObjectResult, Option<String>, bool)> {
    let source_key = request.source_key.clone();
    let source_version_id = request.source_version_id.clone();
    let source_conditions = request.source_conditions.clone();
    let sse_s3_master_key = sqlite.config.sse_s3_master_key();
    let object = {
        let source_connection = sqlite
            .get_connection(&request.source_bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        source_connection
            .read(move |transaction| {
                let source = load_copy_source_object_and_tags(
                    transaction,
                    &source_key,
                    source_version_id.as_deref(),
                    sse_s3_master_key.as_ref(),
                )?;
                validate_copy_source_conditions(&source.object.key_metadata, &source_conditions)?;
                validate_copy_source_size(&source.object.key_metadata)?;
                Ok(source)
            })
            .await
            .map_err(s3_error_from_sqlite)?
    };

    let target_tags = match &request.tagging_directive {
        CopyTaggingDirective::Copy => object.tags.clone(),
        CopyTaggingDirective::Replace(tags) => tags.clone(),
    };
    let target_encryption =
        resolve_write_encryption(sqlite, &request.target_bucket, request.explicit_sse_s3).await?;
    let target_object_lock_config =
        load_bucket_object_lock_config(sqlite, &request.target_bucket).await?;
    let encrypted = target_encryption.is_some();
    let chunk_size = sqlite.blob_chunk_size();
    let target_stored_sse = target_encryption
        .as_ref()
        .map(|encryption| encryption.stored.clone());
    let target_object_lock = object_lock_for_copied_version(
        &object.object.key_metadata.object_lock,
        &target_object_lock_config,
        request.object_lock_headers.clone(),
        OffsetDateTime::now_utc(),
    )?;
    let (object, checksum) = prepare_copied_object(
        object.object,
        &request,
        target_stored_sse,
        target_object_lock,
    )
    .map_err(s3_error_from_sqlite)?;
    let encrypted_chunks = target_encryption
        .as_ref()
        .map(|encryption| {
            let value = object.value.as_deref().ok_or_else(|| {
                s3_error_with_message(InternalError, "copy source body is missing")
            })?;
            encrypt_value_chunks(value, chunk_size, encryption)
        })
        .transpose()?;
    let target_connection = sqlite
        .get_connection(&request.target_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    target_connection
        .write(move |transaction| {
            let write_outcome = if let Some(chunks) = encrypted_chunks {
                let KeyValue {
                    key, key_metadata, ..
                } = object;
                Sqlite::put_object_with_encoded_chunks(transaction, &key, key_metadata, chunks)?
            } else {
                Sqlite::put_object_with_chunk_size(transaction, object, chunk_size)?
            };
            Sqlite::replace_object_tags(transaction, write_outcome.sequence, &target_tags)?;
            let result = copy_object_result(
                &checksum,
                write_outcome.md5.clone(),
                write_outcome.last_modified,
            );
            Ok((result, write_outcome.returned_version_id, encrypted))
        })
        .await
        .map_err(s3_error_from_sqlite)
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
    let request = parse_copy_object_request(input)?;

    sqlite
        .validate_mutable_bucket(&request.target_bucket)
        .map_err(s3_error_from_sqlite)?;
    validate_copy_target(&request)?;

    let copy_source_version_id = request.source_version_id.clone();
    let same_bucket = request.source_bucket == request.target_bucket;
    let (copy_object_result, returned_version_id, encrypted) = if same_bucket {
        copy_object_same_bucket(sqlite, request).await?
    } else {
        copy_object_cross_bucket(sqlite, request).await?
    };

    let output = CopyObjectOutput {
        copy_object_result: Some(copy_object_result),
        copy_source_version_id,
        server_side_encryption: sse_s3_output(encrypted),
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

    fn access_point_source() -> CopySource {
        CopySource::AccessPoint {
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            access_point_name: "ap".into(),
            key: "key".into(),
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

    fn object(value: Vec<u8>, checksum: Option<StoredChecksum>) -> KeyValue {
        KeyValue {
            key: "source-key".to_string(),
            value: Some(value),
            key_metadata: KeyMetadata {
                version_id: "source-version".to_string(),
                size: 7,
                metadata: Some(HashMap::from([("old".to_string(), "value".to_string())])),
                content_type: Some("text/plain".to_string()),
                content_encoding: Some("gzip".to_string()),
                cache_control: Some("old-cache".to_string()),
                expires: Some(OffsetDateTime::UNIX_EPOCH),
                last_modified: OffsetDateTime::UNIX_EPOCH,
                md5: Some("md5".to_string()),
                checksum,
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    fn metadata(md5: Option<&str>, last_modified: OffsetDateTime) -> KeyMetadata {
        let mut metadata = object(Vec::new(), None).key_metadata;
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

    #[test]
    fn parses_copy_object_replace_metadata_and_conditions() {
        let expires = Timestamp::from(OffsetDateTime::UNIX_EPOCH);
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

    #[test]
    fn validates_copy_target_collisions() {
        let mut request = match parse_copy_object_request(copy_input()) {
            Ok(request) => request,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert!(validate_copy_target(&request).is_ok());
        request.source_bucket.clone_from(&request.target_bucket);
        request.source_key.clone_from(&request.target_key);
        assert_error_code(validate_copy_target(&request), &S3ErrorCode::InvalidRequest);

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

        let (copied, checksum) = match prepare_copied_object(
            object(b"payload".to_vec(), Some(source_checksum)),
            &request,
            None,
            StoredObjectLock::default(),
        ) {
            Ok(result) => result,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert_eq!(copied.key, "target-key");
        assert_eq!(
            copied
                .key_metadata
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("old")),
            Some(&"value".to_string())
        );
        assert_eq!(
            copied.key_metadata.content_type.as_deref(),
            Some("text/plain")
        );
        assert_eq!(checksum.algorithm(), ChecksumAlgorithmKind::Sha256);
        assert_eq!(copied.key_metadata.checksum.as_ref(), Some(&checksum));
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

        let (copied, checksum) = match prepare_copied_object(
            object(b"payload".to_vec(), None),
            &request,
            None,
            StoredObjectLock::default(),
        ) {
            Ok(result) => result,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert_eq!(copied.key, "target-key");
        assert_eq!(
            copied
                .key_metadata
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("new")),
            Some(&"value".to_string())
        );
        assert_eq!(
            copied.key_metadata.content_type.as_deref(),
            Some("application/json")
        );
        assert_eq!(
            copied.key_metadata.content_encoding.as_deref(),
            Some("gzip")
        );
        assert_eq!(
            copied.key_metadata.cache_control.as_deref(),
            Some("max-age=60")
        );
        assert_eq!(checksum.algorithm(), ChecksumAlgorithmKind::Crc64Nvme);
        assert_eq!(copied.key_metadata.checksum.as_ref(), Some(&checksum));
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
