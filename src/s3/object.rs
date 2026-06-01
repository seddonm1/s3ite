use std::{collections::HashMap, ops::Range as ByteRange};

use super::{
    CONTENT_MD5, ChecksumAlgorithmKind, ChecksumInput, ChecksumMode, ChecksumPolicy,
    ChecksumTypeKind, CompletedObjectPart, CopyObjectInput, CopyObjectOutput, CopyObjectResult,
    CopySource, DeleteError, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, Digest, ETag, ETagCondition, GetObjectAttributesInput,
    GetObjectAttributesOutput, GetObjectAttributesParts, GetObjectInput, GetObjectOutput,
    HeadObjectInput, HeadObjectOutput, KeyMetadata, KeyValue, Md5, MetadataDirective, Not,
    ObjectAttributes, ObjectBodyReference, ObjectDeleteConditions, ObjectDeleteOutcome, ObjectPart,
    ObjectReadConditionFailure, ObjectReadConditions, OffsetDateTime, PutObjectInput,
    PutObjectOutput, S3Error, S3ErrorCode, S3Range, S3Request, S3Response, S3Result, Sqlite,
    SqliteError, SqliteResult, StorageClass, StoredChecksum, StreamExt, StreamingBlob,
    TryStreamExt, checksum_algorithm_from_request, checksum_crc32, checksum_crc32c,
    checksum_crc64nvme, checksum_mode_enabled, checksum_sha1, checksum_sha256, checksum_struct,
    checksum_type_output, copy_bytes, debug, finish_payload_checksum, hex, list_limit,
    not_found_delete_marker_error, not_modified_error, object_checksum_for_part,
    object_write_checksum_from_value, object_write_conditions, payload_checksum_accumulator,
    requested_part_range, s3_error, s3_error_from_sqlite, s3_error_with_message, stream,
    validate_content_md5, validate_copy_source_conditions, validate_delete_conditions,
    validate_part_number, validate_read_conditions,
};

use crate::database::{BlobReadReceiver, BlobReadRequest, BucketConnection};

pub(super) fn visible_version_id(version_id: String) -> Option<String> {
    (version_id != "null").then_some(version_id)
}

pub(super) fn has_object_attribute(attributes: &[ObjectAttributes], name: &str) -> bool {
    attributes.iter().any(|attribute| {
        attribute
            .as_str()
            .split(',')
            .any(|part| part.trim() == name)
    })
}

/// Stores HTTP content encodings while hiding S3's transport-only aws-chunked marker.
pub(super) fn normalize_content_encoding(content_encoding: Option<String>) -> Option<String> {
    let content_encoding = content_encoding?;
    let values = content_encoding
        .split(',')
        .filter_map(|value| {
            let value = value.trim();
            (value.is_empty().not() && value.eq_ignore_ascii_case("aws-chunked").not())
                .then(|| value.to_string())
        })
        .collect::<Vec<_>>();

    values.is_empty().not().then(|| values.join(", "))
}

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
}

fn parse_copy_object_request(input: CopyObjectInput) -> S3Result<CopyObjectRequest> {
    let CopyObjectInput {
        bucket: target_bucket,
        key: target_key,
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
        ..
    } = input;

    let (source_bucket, source_key, source_version_id) = match copy_source {
        CopySource::AccessPoint { .. } => return Err(s3_error(S3ErrorCode::NotImplemented)),
        CopySource::Bucket {
            bucket,
            key,
            version_id,
        } => (
            bucket,
            key,
            version_id.map(|version_id| version_id.to_string()),
        ),
    };
    let replace_metadata = metadata_directive
        .as_ref()
        .is_some_and(|directive| directive.as_str() == MetadataDirective::REPLACE);
    let target_checksum_kind = checksum_algorithm_from_request(checksum_algorithm)?;

    Ok(CopyObjectRequest {
        target_bucket,
        target_key,
        source_bucket: source_bucket.to_string(),
        source_key: source_key.to_string(),
        source_version_id,
        replace_metadata,
        metadata,
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
    })
}

fn validate_copy_target(request: &CopyObjectRequest) -> S3Result<()> {
    if request.source_bucket.as_str() == request.target_bucket.as_str()
        && request.source_key.as_str() == request.target_key.as_str()
        && request.replace_metadata.not()
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "copy source must be different from target unless replacing metadata",
        ));
    }
    Ok(())
}

fn load_copy_source_object(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
) -> SqliteResult<KeyValue> {
    match version_id {
        Some(version_id) => Sqlite::try_get_object_version(transaction, key, version_id)?
            .ok_or(SqliteError::ObjectVersionNotFound),
        None => Sqlite::try_get_object(transaction, key)?.ok_or(SqliteError::ObjectNotFound),
    }
}

fn prepare_copied_object(
    mut object: KeyValue,
    request: &CopyObjectRequest,
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
    Ok((object, checksum))
}

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

async fn copy_object_same_bucket(
    sqlite: &Sqlite,
    request: CopyObjectRequest,
) -> S3Result<(CopyObjectResult, Option<String>)> {
    let connection = sqlite
        .try_get_connection(&request.target_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let object = load_copy_source_object(
                &transaction,
                &request.source_key,
                request.source_version_id.as_deref(),
            )?;
            validate_copy_source_conditions(&object.key_metadata, &request.source_conditions)?;
            let (object, checksum) = prepare_copied_object(object, &request)?;
            let write_outcome = Sqlite::try_put_object(&transaction, object)?;
            let result = copy_object_result(
                &checksum,
                write_outcome.md5.clone(),
                write_outcome.last_modified,
            );
            transaction.commit()?;
            Ok((result, write_outcome.returned_version_id))
        })
        .await
        .map_err(s3_error_from_sqlite)
}

async fn copy_object_cross_bucket(
    sqlite: &Sqlite,
    request: CopyObjectRequest,
) -> S3Result<(CopyObjectResult, Option<String>)> {
    let source_connection = sqlite
        .try_get_connection(&request.source_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let source_key = request.source_key.clone();
    let source_version_id = request.source_version_id.clone();
    let source_conditions = request.source_conditions.clone();
    let object = source_connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            let object =
                load_copy_source_object(&transaction, &source_key, source_version_id.as_deref())?;
            validate_copy_source_conditions(&object.key_metadata, &source_conditions)?;
            Ok(object)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let (object, checksum) =
        prepare_copied_object(object, &request).map_err(s3_error_from_sqlite)?;
    let target_connection = sqlite
        .try_get_connection(&request.target_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    target_connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let write_outcome = Sqlite::try_put_object(&transaction, object)?;
            let result = copy_object_result(
                &checksum,
                write_outcome.md5.clone(),
                write_outcome.last_modified,
            );
            transaction.commit()?;
            Ok((result, write_outcome.returned_version_id))
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Copies an object between buckets while applying S3 metadata-directive rules.
#[tracing::instrument]
pub(super) async fn copy_object(
    sqlite: &Sqlite,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    let request = parse_copy_object_request(req.input)?;

    sqlite
        .validate_mutable_bucket(&request.target_bucket)
        .map_err(s3_error_from_sqlite)?;
    validate_copy_target(&request)?;

    let copy_source_version_id = request.source_version_id.clone();
    let same_bucket = request.source_bucket == request.target_bucket;
    let (copy_object_result, returned_version_id) = if same_bucket {
        copy_object_same_bucket(sqlite, request).await?
    } else {
        copy_object_cross_bucket(sqlite, request).await?
    };

    let output = CopyObjectOutput {
        copy_object_result: Some(copy_object_result),
        copy_source_version_id,
        version_id: returned_version_id,
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

/// Deletes an object by key.
#[tracing::instrument]
pub(super) async fn delete_object(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let DeleteObjectInput {
        bucket,
        key,
        version_id,
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

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let outcome = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let metadata = match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                }
                None => Sqlite::try_get_latest_object_metadata(&transaction, &key)?,
            };
            validate_delete_conditions(metadata.as_ref(), &conditions)?;
            let outcome = Sqlite::try_delete_object(&transaction, &key, version_id.as_deref())?;
            transaction.commit()?;
            Ok(outcome)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteObjectOutput {
        delete_marker: outcome.is_delete_marker.then_some(true),
        version_id: outcome.version_id,
        ..Default::default()
    }))
}

/// Deletes a batch of objects and returns S3 multi-delete results.
#[tracing::instrument]
pub(super) async fn delete_objects(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let DeleteObjectsInput { bucket, delete, .. } = req.input;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let quiet = delete.quiet.unwrap_or(false);
    let requested_deletions = delete
        .objects
        .into_iter()
        .map(|object| {
            (
                object.key,
                object.version_id,
                ObjectDeleteConditions {
                    etag_matches: object.e_tag.map(ETagCondition::ETag),
                    last_modified_time: object.last_modified_time.map(Into::into),
                    size: object.size,
                },
            )
        })
        .collect::<Vec<_>>();
    if requested_deletions.len() > 1000 {
        return Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "DeleteObjects supports at most 1000 keys",
        ));
    }

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let (deleted_entries, errors) = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let mut deleted_entries = Vec::new();
            let mut errors = Vec::new();

            for (key, version_id, conditions) in requested_deletions {
                let metadata = match version_id.as_deref() {
                    Some(version_id) => {
                        Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                    }
                    None => Sqlite::try_get_latest_object_metadata(&transaction, &key)?,
                };
                match validate_delete_conditions(metadata.as_ref(), &conditions) {
                    Ok(()) => {
                        let outcome =
                            Sqlite::try_delete_object(&transaction, &key, version_id.as_deref())?;
                        deleted_entries.push((key, outcome));
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
            Ok((deleted_entries, errors))
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let output = DeleteObjectsOutput {
        deleted: quiet.not().then(|| {
            deleted_entries
                .into_iter()
                .map(
                    |(key, outcome): (String, ObjectDeleteOutcome)| DeletedObject {
                        key: Some(key),
                        version_id: outcome.version_id,
                        delete_marker: outcome.is_delete_marker.then_some(true),
                        delete_marker_version_id: outcome.delete_marker_version_id,
                    },
                )
                .collect::<Vec<_>>()
        }),
        errors: errors.is_empty().not().then_some(errors),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

struct ObjectBodyRead {
    object: ObjectBodyReference,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
    object_range: ByteRange<u64>,
    body: BlobReadReceiver,
}

struct ObjectBodyReadFound {
    object: ObjectBodyReference,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
    object_range: ByteRange<u64>,
}

enum ObjectBodyReadStart {
    Missing,
    DeleteMarker,
    ConditionFailed {
        failure: ObjectReadConditionFailure,
        md5: Option<String>,
    },
    Found(Box<ObjectBodyReadFound>),
}

struct ObjectMetadataRead {
    metadata: KeyMetadata,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
}

fn validate_range_request(range: Option<&S3Range>, part_number: Option<i32>) -> S3Result<()> {
    if range.is_some() && part_number.is_some() {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Range and partNumber cannot both be specified",
        ));
    }
    if let Some(part_number) = part_number {
        validate_part_number(part_number)?;
    }
    Ok(())
}

fn validate_loaded_read_conditions(
    metadata: &KeyMetadata,
    conditions: &ObjectReadConditions,
) -> S3Result<()> {
    if let Err(error) = validate_read_conditions(metadata, conditions) {
        return Err(match error {
            ObjectReadConditionFailure::PreconditionFailed => {
                s3_error(S3ErrorCode::PreconditionFailed)
            }
            ObjectReadConditionFailure::NotModified => not_modified_error(metadata.md5.as_deref())?,
        });
    }
    Ok(())
}

fn requested_object_range(
    part_number: Option<i32>,
    range: Option<&S3Range>,
    completed_part: Option<&CompletedObjectPart>,
    completed_part_count: i32,
    object_size: u64,
) -> S3Result<ByteRange<u64>> {
    match (part_number, range) {
        (Some(part_number), _) => requested_part_range(
            part_number,
            completed_part,
            completed_part_count,
            object_size,
        ),
        (None, None) => Ok(0..object_size),
        (None, Some(range)) => {
            let object_range = range.check(object_size)?;
            if object_range.is_empty() {
                return Err(s3_error(S3ErrorCode::InvalidRange));
            }
            Ok(object_range)
        }
    }
}

fn completed_part_range_sqlite(part: &CompletedObjectPart) -> SqliteResult<ByteRange<u64>> {
    let start = u64::try_from(part.start).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
    let size = u64::try_from(part.size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
    let end = start
        .checked_add(size)
        .ok_or(SqliteError::ObjectBodyInconsistent)?;
    Ok(start..end)
}

fn requested_object_range_sqlite(
    part_number: Option<i32>,
    range: Option<&S3Range>,
    completed_part: Option<&CompletedObjectPart>,
    completed_part_count: i32,
    object_size: u64,
) -> SqliteResult<ByteRange<u64>> {
    match (part_number, range) {
        (Some(part_number), _) => match completed_part {
            Some(part) => completed_part_range_sqlite(part),
            None if completed_part_count == 0 && part_number == 1 => Ok(0..object_size),
            None => Err(SqliteError::InvalidMultipartPart),
        },
        (None, None) => Ok(0..object_size),
        (None, Some(range)) => {
            let object_range = range
                .check(object_size)
                .map_err(|_| SqliteError::InvalidObjectRange)?;
            if object_range.is_empty() {
                return Err(SqliteError::InvalidObjectRange);
            }
            Ok(object_range)
        }
    }
}

fn content_range_header(
    range_requested: bool,
    part_number_requested: bool,
    object_range: &ByteRange<u64>,
    object_size: u64,
) -> Option<String> {
    (range_requested || part_number_requested)
        .then(|| {
            object_range
                .end
                .checked_sub(1)
                .map(|end| format!("bytes {}-{}/{}", object_range.start, end, object_size))
        })
        .flatten()
}

fn response_checksum(
    object_checksum: Option<&StoredChecksum>,
    completed_part: Option<&CompletedObjectPart>,
    part_number_requested: bool,
    checksum_mode: Option<&ChecksumMode>,
    range_requested: bool,
) -> Option<StoredChecksum> {
    if part_number_requested || (checksum_mode_enabled(checksum_mode) && range_requested.not()) {
        object_checksum_for_part(object_checksum, completed_part)
    } else {
        None
    }
}

async fn load_get_object(
    connection: &BucketConnection,
    key: String,
    version_id: Option<String>,
    part_number: Option<i32>,
    range: Option<S3Range>,
    conditions: ObjectReadConditions,
) -> S3Result<ObjectBodyRead> {
    let (start, body) = connection
        .read_blob(move |transaction| {
            let object = match version_id.as_deref() {
                Some(version_id) => Some(
                    Sqlite::try_get_object_version_body_reference(transaction, &key, version_id)?
                        .ok_or(SqliteError::ObjectVersionNotFound)?,
                ),
                None => Sqlite::try_get_current_object_body_reference(transaction, &key)?,
            };
            let Some(object) = object else {
                return Ok((ObjectBodyReadStart::Missing, None));
            };
            if object.key_metadata.is_delete_marker {
                return Ok((ObjectBodyReadStart::DeleteMarker, None));
            }
            if let Err(failure) = validate_read_conditions(&object.key_metadata, &conditions) {
                return Ok((
                    ObjectBodyReadStart::ConditionFailed {
                        failure,
                        md5: object.key_metadata.md5.clone(),
                    },
                    None,
                ));
            }

            let object_size = u64::try_from(object.key_metadata.size)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let (completed_part, completed_part_count) =
                load_completed_part_info(transaction, &key, &object.version_id, part_number)?;
            let object_range = requested_object_range_sqlite(
                part_number,
                range.as_ref(),
                completed_part.as_ref(),
                completed_part_count,
                object_size,
            )?;
            let length = object_range.end - object_range.start;
            let request = BlobReadRequest::new(
                "object_body",
                "body",
                object.sequence,
                object_range.start,
                length,
                object_size,
            );

            Ok((
                ObjectBodyReadStart::Found(Box::new(ObjectBodyReadFound {
                    object,
                    completed_part,
                    completed_part_count,
                    object_range,
                })),
                Some(request),
            ))
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    match start {
        ObjectBodyReadStart::Missing => Err(not_found_delete_marker_error(false)),
        ObjectBodyReadStart::DeleteMarker => Err(not_found_delete_marker_error(true)),
        ObjectBodyReadStart::ConditionFailed { failure, md5 } => Err(match failure {
            ObjectReadConditionFailure::PreconditionFailed => {
                s3_error(S3ErrorCode::PreconditionFailed)
            }
            ObjectReadConditionFailure::NotModified => not_modified_error(md5.as_deref())?,
        }),
        ObjectBodyReadStart::Found(read) => Ok(ObjectBodyRead {
            object: read.object,
            completed_part: read.completed_part,
            completed_part_count: read.completed_part_count,
            object_range: read.object_range,
            body,
        }),
    }
}

async fn load_head_object(
    connection: &BucketConnection,
    key: String,
    version_id: Option<String>,
    part_number: Option<i32>,
) -> S3Result<ObjectMetadataRead> {
    let (metadata, completed_part, completed_part_count) = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            let metadata = match version_id.as_deref() {
                Some(version_id) => Some(
                    Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                        .filter(|metadata| metadata.is_delete_marker.not())
                        .ok_or(SqliteError::ObjectVersionNotFound)?,
                ),
                None => Sqlite::try_get_current_metadata(&transaction, &key)?,
            };
            let Some(metadata) = metadata else {
                return Ok((None, None, 0));
            };
            if metadata.is_delete_marker {
                return Ok((Some(metadata), None, 0));
            }
            let (completed_part, completed_part_count) =
                load_completed_part_info(&transaction, &key, &metadata.version_id, part_number)?;
            Ok((Some(metadata), completed_part, completed_part_count))
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    let Some(metadata) = metadata else {
        return Err(not_found_delete_marker_error(false));
    };
    if metadata.is_delete_marker {
        return Err(not_found_delete_marker_error(true));
    }
    Ok(ObjectMetadataRead {
        metadata,
        completed_part,
        completed_part_count,
    })
}

fn load_completed_part_info(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: &str,
    part_number: Option<i32>,
) -> SqliteResult<(Option<CompletedObjectPart>, i32)> {
    match part_number {
        Some(part_number) => Ok((
            Sqlite::try_get_completed_object_part(transaction, key, version_id, part_number)?,
            Sqlite::try_count_completed_object_parts(transaction, key, version_id)?,
        )),
        None => Ok((None, 0)),
    }
}

fn get_object_output(
    read: ObjectBodyRead,
    range: Option<&S3Range>,
    part_number: Option<i32>,
    checksum_mode: Option<&ChecksumMode>,
) -> S3Result<GetObjectOutput> {
    let ObjectBodyReference {
        version_id,
        key_metadata,
        ..
    } = read.object;
    let object_size = u64::try_from(key_metadata.size).map_err(S3Error::internal_error)?;
    let object_range = read.object_range;
    let content_length = object_range.end - object_range.start;
    let content_length_i64 = i64::try_from(content_length).map_err(S3Error::internal_error)?;
    let body = stream::unfold(
        (read.body, content_length, false),
        |(mut receiver, remaining, done)| async move {
            if done {
                return None;
            }
            match receiver.recv().await {
                Some(Ok(bytes)) => {
                    let Ok(chunk_len) = u64::try_from(bytes.len()) else {
                        return Some((
                            Err(S3Error::with_message(
                                S3ErrorCode::InternalError,
                                "object body chunk length does not fit u64",
                            )),
                            (receiver, 0, true),
                        ));
                    };
                    let Some(remaining) = remaining.checked_sub(chunk_len) else {
                        return Some((
                            Err(s3_error_from_sqlite(SqliteError::ObjectBodyInconsistent)),
                            (receiver, 0, true),
                        ));
                    };
                    Some((Ok(bytes), (receiver, remaining, false)))
                }
                Some(Err(error)) => Some((Err(s3_error_from_sqlite(error)), (receiver, 0, true))),
                None if remaining == 0 => None,
                None if receiver.timed_out() => Some((
                    Err(s3_error_from_sqlite(SqliteError::ObjectBodyReadTimeout)),
                    (receiver, 0, true),
                )),
                None => Some((
                    Err(s3_error_from_sqlite(SqliteError::ObjectBodyInconsistent)),
                    (receiver, 0, true),
                )),
            }
        },
    )
    .fuse();
    let response_checksum = response_checksum(
        key_metadata.checksum.as_ref(),
        read.completed_part.as_ref(),
        part_number.is_some(),
        checksum_mode,
        range.is_some(),
    );
    let checksum_type = checksum_type_output(&response_checksum);

    Ok(GetObjectOutput {
        body: Some(StreamingBlob::wrap::<_, S3Error>(body)),
        accept_ranges: Some("bytes".to_string()),
        cache_control: key_metadata.cache_control,
        checksum_crc32: checksum_crc32(&response_checksum),
        checksum_crc32c: checksum_crc32c(&response_checksum),
        checksum_crc64nvme: checksum_crc64nvme(&response_checksum),
        checksum_sha1: checksum_sha1(&response_checksum),
        checksum_sha256: checksum_sha256(&response_checksum),
        checksum_type,
        content_encoding: key_metadata.content_encoding,
        content_range: content_range_header(
            range.is_some(),
            part_number.is_some(),
            &object_range,
            object_size,
        ),
        content_length: Some(content_length_i64),
        content_type: key_metadata.content_type,
        expires: key_metadata.expires.map(Into::into),
        last_modified: Some(key_metadata.last_modified.into()),
        metadata: key_metadata.metadata,
        e_tag: key_metadata.md5.map(ETag::Strong),
        parts_count: (read.completed_part_count > 0).then_some(read.completed_part_count),
        version_id: Some(version_id),
        ..Default::default()
    })
}

fn head_object_output(
    read: ObjectMetadataRead,
    range: Option<&S3Range>,
    part_number: Option<i32>,
    checksum_mode: Option<&ChecksumMode>,
) -> S3Result<HeadObjectOutput> {
    let object = read.metadata;
    let content_type = object
        .content_type
        .unwrap_or_else(|| mime::APPLICATION_OCTET_STREAM.to_string());
    let object_size = u64::try_from(object.size).map_err(S3Error::internal_error)?;
    let object_range = requested_object_range(
        part_number,
        range,
        read.completed_part.as_ref(),
        read.completed_part_count,
        object_size,
    )?;
    let content_length =
        i64::try_from(object_range.end - object_range.start).map_err(S3Error::internal_error)?;
    let response_checksum = response_checksum(
        object.checksum.as_ref(),
        read.completed_part.as_ref(),
        part_number.is_some(),
        checksum_mode,
        range.is_some(),
    );
    let checksum_type = checksum_type_output(&response_checksum);

    Ok(HeadObjectOutput {
        accept_ranges: Some("bytes".to_string()),
        cache_control: object.cache_control,
        checksum_crc32: checksum_crc32(&response_checksum),
        checksum_crc32c: checksum_crc32c(&response_checksum),
        checksum_crc64nvme: checksum_crc64nvme(&response_checksum),
        checksum_sha1: checksum_sha1(&response_checksum),
        checksum_sha256: checksum_sha256(&response_checksum),
        checksum_type,
        content_encoding: object.content_encoding,
        content_length: Some(content_length),
        content_range: content_range_header(
            range.is_some(),
            part_number.is_some(),
            &object_range,
            object_size,
        ),
        content_type: Some(content_type),
        e_tag: object.md5.map(ETag::Strong),
        expires: object.expires.map(Into::into),
        last_modified: Some(object.last_modified.into()),
        metadata: object.metadata,
        parts_count: (read.completed_part_count > 0).then_some(read.completed_part_count),
        version_id: Some(object.version_id),
        ..Default::default()
    })
}

/// Returns an object body and metadata, applying byte-range requests when present.
#[tracing::instrument]
pub(super) async fn get_object(
    sqlite: &Sqlite,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let GetObjectInput {
        bucket,
        key,
        range,
        part_number,
        version_id,
        if_match,
        if_modified_since,
        if_none_match,
        if_unmodified_since,
        checksum_mode,
        ..
    } = req.input;
    validate_range_request(range.as_ref(), part_number)?;

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let read = load_get_object(
        &connection,
        key,
        version_id,
        part_number,
        range,
        ObjectReadConditions {
            etag_matches: if_match,
            modified_since: if_modified_since.map(Into::into),
            etag_none_matches: if_none_match,
            unmodified_since: if_unmodified_since.map(Into::into),
        },
    )
    .await?;
    let output = get_object_output(read, range.as_ref(), part_number, checksum_mode.as_ref())?;
    Ok(S3Response::new(output))
}

/// Returns object metadata without reading the object body.
#[tracing::instrument]
pub(super) async fn head_object(
    sqlite: &Sqlite,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let HeadObjectInput {
        bucket,
        key,
        range,
        part_number,
        version_id,
        if_match,
        if_modified_since,
        if_none_match,
        if_unmodified_since,
        checksum_mode,
        ..
    } = req.input;
    validate_range_request(range.as_ref(), part_number)?;

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let read = load_head_object(&connection, key, version_id, part_number).await?;
    validate_loaded_read_conditions(
        &read.metadata,
        &ObjectReadConditions {
            etag_matches: if_match,
            modified_since: if_modified_since.map(Into::into),
            etag_none_matches: if_none_match,
            unmodified_since: if_unmodified_since.map(Into::into),
        },
    )?;
    let output = head_object_output(read, range.as_ref(), part_number, checksum_mode.as_ref())?;
    Ok(S3Response::new(output))
}

fn object_attributes_parts(
    wants_object_parts: bool,
    total_parts_count: i32,
    mut parts: Vec<CompletedObjectPart>,
    max_parts: i32,
    part_number_marker: i32,
) -> S3Result<Option<GetObjectAttributesParts>> {
    if wants_object_parts.not() || total_parts_count == 0 {
        return Ok(None);
    }

    let max_parts_usize = usize::try_from(max_parts).map_err(S3Error::internal_error)?;
    let is_truncated = parts.len() > max_parts_usize;
    if is_truncated {
        parts.truncate(max_parts_usize);
    }
    let next_part_number_marker = is_truncated
        .then(|| parts.last().map(|part| part.part_number))
        .flatten();
    let parts = parts
        .into_iter()
        .map(|part| ObjectPart {
            checksum_crc32: checksum_crc32(&part.checksum),
            checksum_crc32c: checksum_crc32c(&part.checksum),
            checksum_crc64nvme: checksum_crc64nvme(&part.checksum),
            checksum_sha1: checksum_sha1(&part.checksum),
            checksum_sha256: checksum_sha256(&part.checksum),
            part_number: Some(part.part_number),
            size: Some(part.size),
        })
        .collect::<Vec<_>>();

    Ok(Some(GetObjectAttributesParts {
        is_truncated: Some(is_truncated),
        max_parts: Some(max_parts),
        next_part_number_marker,
        part_number_marker: Some(part_number_marker),
        parts: Some(parts),
        total_parts_count: Some(total_parts_count),
    }))
}

/// Returns selected object attributes without returning the object body.
#[tracing::instrument]
pub(super) async fn get_object_attributes(
    sqlite: &Sqlite,
    req: S3Request<GetObjectAttributesInput>,
) -> S3Result<S3Response<GetObjectAttributesOutput>> {
    let GetObjectAttributesInput {
        bucket,
        key,
        version_id,
        object_attributes,
        max_parts,
        part_number_marker,
        ..
    } = req.input;

    let max_parts = list_limit(max_parts, 1000, 1000)?;
    let part_number_marker = part_number_marker.unwrap_or(0);
    if part_number_marker < 0 {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "part-number-marker must be nonnegative",
        ));
    }

    let wants_checksum = has_object_attribute(&object_attributes, ObjectAttributes::CHECKSUM);
    let wants_etag = has_object_attribute(&object_attributes, ObjectAttributes::ETAG);
    let wants_object_parts =
        has_object_attribute(&object_attributes, ObjectAttributes::OBJECT_PARTS);
    let wants_object_size = has_object_attribute(&object_attributes, ObjectAttributes::OBJECT_SIZE);
    let wants_storage_class =
        has_object_attribute(&object_attributes, ObjectAttributes::STORAGE_CLASS);

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (object, parts, total_parts_count) = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            let object = match version_id.as_deref() {
                Some(version_id) => Some(
                    Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                        .filter(|metadata| metadata.is_delete_marker.not())
                        .ok_or(SqliteError::ObjectVersionNotFound)?,
                ),
                None => Sqlite::try_get_current_metadata(&transaction, &key)?,
            };
            let Some(object) = object else {
                return Ok((None, Vec::new(), 0));
            };
            if object.is_delete_marker {
                return Ok((Some(object), Vec::new(), 0));
            }

            let total_parts_count =
                Sqlite::try_count_completed_object_parts(&transaction, &key, &object.version_id)?;
            let parts = if wants_object_parts && total_parts_count > 0 {
                Sqlite::try_list_completed_object_parts(
                    &transaction,
                    &key,
                    &object.version_id,
                    part_number_marker,
                    i64::from(max_parts) + 1,
                )?
            } else {
                Vec::new()
            };
            Ok((Some(object), parts, total_parts_count))
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let Some(object) = object else {
        return Err(not_found_delete_marker_error(false));
    };
    if object.is_delete_marker {
        return Err(not_found_delete_marker_error(true));
    }

    let object_parts = object_attributes_parts(
        wants_object_parts,
        total_parts_count,
        parts,
        max_parts,
        part_number_marker,
    )?;

    let output = GetObjectAttributesOutput {
        checksum: wants_checksum
            .then(|| checksum_struct(&object.checksum))
            .flatten(),
        e_tag: wants_etag.then(|| object.md5.map(ETag::Strong)).flatten(),
        last_modified: Some(object.last_modified.into()),
        object_parts,
        object_size: wants_object_size.then_some(object.size),
        storage_class: wants_storage_class
            .then(|| StorageClass::from_static(StorageClass::STANDARD)),
        version_id: visible_version_id(object.version_id),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

struct BufferedObjectBody {
    value: Vec<u8>,
    size: i64,
    md5: String,
    checksum: StoredChecksum,
}

async fn read_put_object_body(
    body: StreamingBlob,
    content_md5_header_present: bool,
    content_md5: Option<String>,
    checksum_input: ChecksumInput,
) -> S3Result<BufferedObjectBody> {
    let mut md5_hash = Md5::new();
    let mut checksum_accumulator =
        payload_checksum_accumulator(&checksum_input, ChecksumPolicy::DefaultCrc64NvmeFullObject)?;
    let stream = body.inspect_ok(|bytes| {
        let bytes = bytes.as_ref();
        md5_hash.update(bytes);
        checksum_accumulator.update(bytes);
    });
    let mut value = Vec::new();
    let size =
        i64::try_from(copy_bytes(stream, &mut value).await?).map_err(S3Error::internal_error)?;
    let md5_bytes = md5_hash.finalize();
    let md5 = hex(md5_bytes);

    validate_content_md5(
        content_md5_header_present,
        content_md5.as_deref(),
        md5_bytes.as_ref(),
    )?;

    let checksum = finish_payload_checksum(
        &checksum_input,
        checksum_accumulator,
        ChecksumTypeKind::FullObject,
    )?;

    Ok(BufferedObjectBody {
        value,
        size,
        md5,
        checksum,
    })
}

fn validate_put_storage_class(storage_class: Option<&StorageClass>) -> S3Result<()> {
    if let Some(storage_class) = storage_class {
        let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
        if is_valid.not() {
            return Err(s3_error(S3ErrorCode::InvalidStorageClass));
        }
    }
    Ok(())
}

fn put_object_output(
    md5: String,
    checksum: &StoredChecksum,
    version_id: Option<String>,
) -> PutObjectOutput {
    let checksum_type = checksum_type_output(checksum);
    PutObjectOutput {
        e_tag: Some(ETag::Strong(md5)),
        version_id,
        checksum_crc32: checksum_crc32(checksum),
        checksum_crc32c: checksum_crc32c(checksum),
        checksum_crc64nvme: checksum_crc64nvme(checksum),
        checksum_sha1: checksum_sha1(checksum),
        checksum_sha256: checksum_sha256(checksum),
        checksum_type,
        ..Default::default()
    }
}

/// Stores an object body and metadata.
#[tracing::instrument]
pub(super) async fn put_object(
    sqlite: &Sqlite,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let S3Request { input, headers, .. } = req;
    let PutObjectInput {
        body,
        bucket,
        key,
        metadata,
        cache_control,
        content_encoding,
        content_length: _,
        content_md5,
        content_type,
        checksum_algorithm,
        checksum_crc32,
        checksum_crc32c,
        checksum_crc64nvme,
        checksum_sha1,
        checksum_sha256,
        expires,
        if_match,
        if_none_match,
        storage_class,
        ..
    } = input;
    let write_conditions = object_write_conditions(if_match, if_none_match);
    let content_encoding = normalize_content_encoding(content_encoding);

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    if sqlite.buckets.read().await.contains_key(&bucket).not() {
        return Err(s3_error(S3ErrorCode::NoSuchBucket));
    }

    validate_put_storage_class(storage_class.as_ref())?;

    let Some(body) = body else {
        return Err(s3_error(S3ErrorCode::IncompleteBody));
    };

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let BufferedObjectBody {
        value,
        size,
        md5,
        checksum,
    } = read_put_object_body(
        body,
        headers.contains_key(CONTENT_MD5),
        content_md5,
        ChecksumInput {
            algorithm: checksum_algorithm,
            crc32: checksum_crc32,
            crc32c: checksum_crc32c,
            crc64nvme: checksum_crc64nvme,
            sha1: checksum_sha1,
            sha256: checksum_sha256,
            ..ChecksumInput::default()
        },
    )
    .await?;

    debug!(path = %key, ?size, %md5, "write file");

    let md5_clone = md5.clone();
    let checksum_clone = checksum.clone();
    let write_outcome = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            Sqlite::try_check_object_write_conditions(&transaction, &key, &write_conditions)?;
            let outcome = Sqlite::try_put_object(
                &transaction,
                KeyValue {
                    key,
                    value: Some(value),
                    key_metadata: KeyMetadata {
                        version_id: String::new(),
                        size,
                        metadata,
                        content_type,
                        content_encoding,
                        cache_control,
                        expires: expires.map(Into::into),
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5_clone),
                        checksum: Some(checksum_clone),
                        is_latest: true,
                        is_delete_marker: false,
                    },
                },
            )?;
            transaction.commit()?;
            Ok(outcome)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let output = put_object_output(md5, &checksum, write_outcome.returned_version_id);
    Ok(S3Response::new(output))
}
