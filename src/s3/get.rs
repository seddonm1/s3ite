//! S3 object read and attribute request handling.

use std::ops::Range as ByteRange;

use super::{
    ChecksumMode, CompletedObjectPart, ETag, GetObjectAttributesInput, GetObjectAttributesOutput,
    GetObjectAttributesParts, GetObjectInput, GetObjectOutput, GetObjectTorrentInput,
    GetObjectTorrentOutput, HeadObjectInput, HeadObjectOutput, KeyMetadata, Not, ObjectAttributes,
    ObjectBodyReference, ObjectPart, ObjectReadConditionFailure, ObjectReadConditions, S3Error,
    S3ErrorCode, S3Range, S3Request, S3Response, S3Result, Sqlite, SqliteError, SqliteResult,
    StorageClass, StoredChecksum, StreamExt, StreamingBlob, checksum_crc32, checksum_crc32c,
    checksum_crc64nvme, checksum_mode_enabled, checksum_sha1, checksum_sha256, checksum_struct,
    checksum_type_output, list_limit, not_found_delete_marker_error, not_modified_error,
    object_checksum_for_part, requested_part_range, s3_error, s3_error_from_sqlite,
    s3_error_with_message, stream, validate_part_number, validate_read_conditions,
};

use crate::database::{BlobReadReceiver, BlobReadRequest, BucketConnection};

/// Reports that `BitTorrent` metadata is unavailable for this object store.
pub(super) fn get_object_torrent(
    _sqlite: &Sqlite,
    _req: S3Request<GetObjectTorrentInput>,
) -> S3Result<S3Response<GetObjectTorrentOutput>> {
    Err(s3_error(S3ErrorCode::NoSuchKey))
}

/// Hides the special S3 `null` version ID from response fields that omit it.
fn visible_version_id(version_id: String) -> Option<String> {
    (version_id != "null").then_some(version_id)
}

/// Returns whether the comma-separated object attributes request includes a name.
fn has_object_attribute(attributes: &[ObjectAttributes], name: &str) -> bool {
    attributes.iter().any(|attribute| {
        attribute
            .as_str()
            .split(',')
            .any(|part| part.trim() == name)
    })
}

/// Loaded object metadata plus the streaming body reader for `GetObject`.
struct ObjectBodyRead {
    object: ObjectBodyReference,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
    object_range: ByteRange<u64>,
    body: BlobReadReceiver,
}

/// Metadata selected inside the blob-reader transaction before streaming starts.
struct ObjectBodyReadFound {
    object: ObjectBodyReference,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
    object_range: ByteRange<u64>,
}

/// Result of resolving an object body read before opening the response stream.
enum ObjectBodyReadStart {
    /// No current or requested object version exists.
    Missing,
    /// The resolved current version is a delete marker.
    DeleteMarker,
    /// Preconditions failed while evaluating metadata in the reader transaction.
    ConditionFailed {
        failure: ObjectReadConditionFailure,
        md5: Option<String>,
    },
    /// The object exists and has an associated blob read request.
    Found(Box<ObjectBodyReadFound>),
}

/// Object metadata selected for `HeadObject` and attribute responses.
struct ObjectMetadataRead {
    metadata: KeyMetadata,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
}

/// Validates S3 range and `partNumber` request parameters before storage access.
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

/// Maps loaded object conditional validation failures into protocol errors.
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

/// Computes the byte span represented by a range or completed multipart part.
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

/// Converts a completed multipart part boundary into a byte span.
fn completed_part_range_sqlite(part: &CompletedObjectPart) -> SqliteResult<ByteRange<u64>> {
    let start = u64::try_from(part.start).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
    let size = u64::try_from(part.size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
    let end = start
        .checked_add(size)
        .ok_or(SqliteError::ObjectBodyInconsistent)?;
    Ok(start..end)
}

/// Computes the byte span for a streaming read inside the `SQLite` reader path.
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

/// Formats the response `Content-Range` header when a partial span was requested.
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

/// Selects the checksum value that should be exposed for the response shape.
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

/// Resolves object metadata and opens a bounded blob stream for `GetObject`.
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

/// Loads metadata and completed-part state for `HeadObject`.
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

/// Loads completed multipart part metadata when `partNumber` was requested.
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

/// Builds a `GetObject` response around the `SQLite` blob reader stream.
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

/// Builds a `HeadObject` response using metadata-only object state.
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

/// Builds paginated object-part attributes for `GetObjectAttributes`.
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use super::*;

    use hyper::http::{Extensions, HeaderMap, Method, Uri};
    use tokio::sync::RwLock;

    use crate::Config;
    use crate::checksum::{FullObjectChecksum, PartChecksum};

    fn assert_error_code<T>(result: S3Result<T>, code: &S3ErrorCode) {
        let err = result.err();
        assert!(
            err.as_ref().is_some_and(|err| err.code() == code),
            "expected {code:?}"
        );
    }

    fn assert_s3_ok<T>(result: S3Result<T>, expected: &T)
    where
        T: PartialEq + std::fmt::Debug,
    {
        match result {
            Ok(value) => assert_eq!(&value, expected),
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }

    fn assert_sqlite_ok<T>(result: SqliteResult<T>, expected: &T)
    where
        T: PartialEq + std::fmt::Debug,
    {
        match result {
            Ok(value) => assert_eq!(&value, expected),
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }

    fn range(header: &str) -> S3Range {
        match S3Range::parse(header) {
            Ok(range) => range,
            Err(err) => panic!("unexpected range parse error: {err:?}"),
        }
    }

    fn completed_part(part_number: i32, start: i64, size: i64) -> CompletedObjectPart {
        CompletedObjectPart {
            part_number,
            start,
            size,
            md5: Some("md5".to_string()),
            checksum: Some(PartChecksum::Sha256("part-sha256".to_string())),
        }
    }

    fn object_checksum() -> StoredChecksum {
        StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("object-crc64".to_string()))
    }

    fn request<T: Default>() -> S3Request<T> {
        S3Request {
            input: T::default(),
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn sqlite_without_buckets() -> Sqlite {
        Sqlite {
            root: PathBuf::new(),
            config: Config::default(),
            buckets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[test]
    fn torrent_requests_are_not_supported() {
        assert_error_code(
            get_object_torrent(
                &sqlite_without_buckets(),
                request::<GetObjectTorrentInput>(),
            ),
            &S3ErrorCode::NoSuchKey,
        );
    }

    #[test]
    fn hides_null_version_ids() {
        assert_eq!(visible_version_id("null".to_string()), None);
        assert_eq!(
            visible_version_id("version-1".to_string()),
            Some("version-1".to_string())
        );
    }

    #[test]
    fn matches_requested_object_attributes_by_exact_trimmed_token() {
        let attributes = vec![
            ObjectAttributes::from("Checksum, ObjectParts".to_string()),
            ObjectAttributes::from_static(ObjectAttributes::OBJECT_SIZE),
        ];

        assert!(has_object_attribute(
            &attributes,
            ObjectAttributes::CHECKSUM
        ));
        assert!(has_object_attribute(
            &attributes,
            ObjectAttributes::OBJECT_PARTS
        ));
        assert!(has_object_attribute(
            &attributes,
            ObjectAttributes::OBJECT_SIZE
        ));
        assert!(!has_object_attribute(&attributes, ObjectAttributes::ETAG));
        assert!(!has_object_attribute(&attributes, "Object"));
    }

    #[test]
    fn validates_range_and_part_number_inputs() {
        let parsed_range = range("bytes=1-3");

        assert!(validate_range_request(None, None).is_ok());
        assert!(validate_range_request(Some(&parsed_range), None).is_ok());
        assert!(validate_range_request(None, Some(1)).is_ok());
        assert_error_code(
            validate_range_request(Some(&parsed_range), Some(1)),
            &S3ErrorCode::InvalidRequest,
        );
        assert_error_code(
            validate_range_request(None, Some(0)),
            &S3ErrorCode::InvalidArgument,
        );
    }

    #[test]
    fn resolves_requested_object_ranges() {
        assert_s3_ok(requested_object_range(None, None, None, 0, 10), &(0..10));
        assert_s3_ok(
            requested_object_range(None, Some(&range("bytes=2-5")), None, 0, 10),
            &(2..6),
        );
        assert_s3_ok(
            requested_object_range(None, Some(&range("bytes=3-")), None, 0, 10),
            &(3..10),
        );
        assert_s3_ok(
            requested_object_range(None, Some(&range("bytes=-4")), None, 0, 10),
            &(6..10),
        );
        assert_error_code(
            requested_object_range(None, Some(&range("bytes=20-30")), None, 0, 10),
            &S3ErrorCode::InvalidRange,
        );
        assert_error_code(
            requested_object_range(None, Some(&range("bytes=-0")), None, 0, 10),
            &S3ErrorCode::InvalidRange,
        );

        let part = completed_part(2, 4, 3);
        assert_s3_ok(
            requested_object_range(Some(2), None, Some(&part), 2, 10),
            &(4..7),
        );
        assert_s3_ok(requested_object_range(Some(1), None, None, 0, 10), &(0..10));
        assert_error_code(
            requested_object_range(Some(2), None, None, 2, 10),
            &S3ErrorCode::InvalidPart,
        );
    }

    #[test]
    fn resolves_requested_object_ranges_for_sqlite_reader_path() {
        assert_sqlite_ok(
            requested_object_range_sqlite(None, None, None, 0, 10),
            &(0..10),
        );
        assert_sqlite_ok(
            requested_object_range_sqlite(None, Some(&range("bytes=2-5")), None, 0, 10),
            &(2..6),
        );
        assert_sqlite_ok(
            requested_object_range_sqlite(None, Some(&range("bytes=3-")), None, 0, 10),
            &(3..10),
        );
        assert_sqlite_ok(
            requested_object_range_sqlite(None, Some(&range("bytes=-4")), None, 0, 10),
            &(6..10),
        );
        assert!(matches!(
            requested_object_range_sqlite(None, Some(&range("bytes=20-30")), None, 0, 10),
            Err(SqliteError::InvalidObjectRange)
        ));
        assert!(matches!(
            requested_object_range_sqlite(None, Some(&range("bytes=-0")), None, 0, 10),
            Err(SqliteError::InvalidObjectRange)
        ));

        let part = completed_part(2, 4, 3);
        assert_sqlite_ok(
            requested_object_range_sqlite(Some(2), None, Some(&part), 2, 10),
            &(4..7),
        );
        assert_sqlite_ok(
            requested_object_range_sqlite(Some(1), None, None, 0, 10),
            &(0..10),
        );
        assert!(matches!(
            requested_object_range_sqlite(Some(2), None, None, 2, 10),
            Err(SqliteError::InvalidMultipartPart)
        ));

        let invalid_part = completed_part(1, -1, 1);
        assert!(matches!(
            requested_object_range_sqlite(Some(1), None, Some(&invalid_part), 1, 10),
            Err(SqliteError::ObjectBodyInconsistent)
        ));
    }

    #[test]
    fn formats_content_range_headers() {
        assert_eq!(content_range_header(false, false, &(2..6), 10), None);
        assert_eq!(
            content_range_header(true, false, &(2..6), 10),
            Some("bytes 2-5/10".to_string())
        );
        assert_eq!(
            content_range_header(false, true, &(2..6), 10),
            Some("bytes 2-5/10".to_string())
        );
        assert_eq!(content_range_header(true, false, &(0..0), 10), None);
    }

    #[test]
    fn selects_response_checksums_for_requested_response_shape() {
        let object_checksum = object_checksum();
        let part = completed_part(1, 0, 5);
        let checksum_mode = ChecksumMode::from_static(ChecksumMode::ENABLED);

        assert_eq!(
            response_checksum(Some(&object_checksum), None, false, None, false),
            None
        );
        assert_eq!(
            response_checksum(
                Some(&object_checksum),
                None,
                false,
                Some(&checksum_mode),
                false
            ),
            Some(object_checksum.clone())
        );
        assert_eq!(
            response_checksum(
                Some(&object_checksum),
                None,
                false,
                Some(&checksum_mode),
                true
            ),
            None
        );
        assert_eq!(
            response_checksum(Some(&object_checksum), None, true, None, false),
            Some(object_checksum.clone())
        );
        assert_eq!(
            response_checksum(Some(&object_checksum), Some(&part), true, None, false),
            Some(StoredChecksum::Full(FullObjectChecksum::Sha256(
                "part-sha256".to_string()
            )))
        );
    }
}
