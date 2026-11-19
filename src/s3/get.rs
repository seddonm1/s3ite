//! S3 object read and attribute request handling.

use std::{ops::Range as ByteRange, sync::Arc};

use time::OffsetDateTime;

use crate::sqlite::{
    CompletedObjectPart, EncryptedChunkRead, KeyMetadata, ObjectBodyReference, StoredChecksum,
    StoredSse,
};

use super::source::decrypting_snapshot_stream;

use super::metadata::format_expires;
use super::{
    ChecksumMode, ETag, GetObjectAttributesInput, GetObjectAttributesOutput,
    GetObjectAttributesParts, GetObjectInput, GetObjectOutput, GetObjectTorrentInput,
    GetObjectTorrentOutput, HeadObjectInput, HeadObjectOutput, HeaderValue,
    LifecycleExpirationHeader, Not, ObjectAttributes, ObjectPart, ObjectReadConditionFailure,
    ObjectReadConditions, S3Error, S3ErrorCode, S3Range, S3Request, S3Response, S3Result, Sqlite,
    SqliteError, SqliteResult, StorageClass, StreamExt, StreamingBlob, X_AMZ_TAGGING_COUNT,
    checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_md5, checksum_mode_enabled,
    checksum_sha1, checksum_sha256, checksum_sha512, checksum_struct, checksum_type_output,
    checksum_xxhash3, checksum_xxhash64, checksum_xxhash128, delete_marker_version_error,
    encryption_headers, list_limit, not_found_delete_marker_error, not_modified_error,
    object_checksum_for_part, object_lock_legal_hold_status, object_lock_mode,
    object_lock_retain_until_date, open_encryption, requested_part_range, s3_error,
    s3_error_from_sqlite, s3_error_with_message, stream, validate_expected_bucket_owner,
    validate_no_sse_headers, validate_part_number, validate_read_conditions,
};

use crate::database::{BlobReadReceiver, BlobReadRequest};

/// Reports that `BitTorrent` metadata is unavailable for this object store.
pub(super) fn get_object_torrent(
    _sqlite: &Sqlite,
    req: S3Request<GetObjectTorrentInput>,
) -> S3Result<S3Response<GetObjectTorrentOutput>> {
    let GetObjectTorrentInput {
        expected_bucket_owner,
        ..
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
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
    expiration: Option<LifecycleExpirationHeader>,
    body: ObjectBodyReadBody,
}

/// Backing body for a `GetObject` response.
enum ObjectBodyReadBody {
    Blob(BlobReadReceiver),
    /// Already-decoded plaintext stream, such as decrypted SSE chunks.
    Stream(StreamingBlob),
}

/// Metadata selected inside the blob-reader transaction before streaming starts.
struct ObjectBodyReadFound {
    object: ObjectBodyReference,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
    object_range: ByteRange<u64>,
    expiration: Option<LifecycleExpirationHeader>,
}

/// Result of resolving an object body read before opening the response stream.
enum ObjectBodyReadStart {
    /// No current or requested object version exists.
    Missing,
    /// The resolved current version is a delete marker.
    /// The resolved version is a delete marker; carries its version id and
    /// last-modified time when the request named that version.
    DeleteMarker(Option<(String, OffsetDateTime)>),
    /// Preconditions failed while evaluating metadata in the reader transaction.
    ConditionFailed {
        failure: ObjectReadConditionFailure,
        md5: Option<String>,
    },
    /// The object exists and has an associated blob read request.
    Found {
        read: Box<ObjectBodyReadFound>,
        /// Encrypted chunks whose ciphertext the BLOB stream delivers, for SSE objects.
        chunks: Option<Vec<EncryptedChunkRead>>,
    },
}

/// Object metadata selected for `HeadObject` and attribute responses.
struct ObjectMetadataRead {
    metadata: KeyMetadata,
    completed_part: Option<CompletedObjectPart>,
    completed_part_count: i32,
    tag_count: i32,
    expiration: Option<LifecycleExpirationHeader>,
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
            None => Err(SqliteError::PartNumberNotSatisfiable),
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

/// Formats lifecycle expiration metadata for object read response headers.
fn lifecycle_expiration_response_header(
    expiration: Option<LifecycleExpirationHeader>,
) -> S3Result<Option<String>> {
    expiration
        .map(|expiration| expiration.to_header_value())
        .transpose()
}

/// Loads the current or exact-version body reference for a `GetObject` read.
fn load_object_body_reference(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
) -> SqliteResult<Option<ObjectBodyReference>> {
    Ok(match version_id {
        Some(version_id) => Some(
            Sqlite::get_object_version_body_reference(transaction, key, version_id)?
                .ok_or(SqliteError::ObjectVersionNotFound)?,
        ),
        None => Sqlite::get_current_object_body_reference(transaction, key)?,
    })
}

/// Resolves range, part, and lifecycle metadata for a found object body.
fn load_object_body_read_found(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    object: ObjectBodyReference,
    part_number: Option<i32>,
    range: Option<&S3Range>,
) -> SqliteResult<ObjectBodyReadFound> {
    let object_size =
        u64::try_from(object.key_metadata.size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
    let (completed_part, completed_part_count) =
        load_completed_part_info(transaction, key, &object.version_id, part_number)?;
    let object_range = requested_object_range_sqlite(
        part_number,
        range,
        completed_part.as_ref(),
        completed_part_count,
        object_size,
    )?;
    let expiration = Sqlite::lifecycle_expiration_for_object(
        transaction,
        key,
        &object.version_id,
        object.key_metadata.size,
        object.key_metadata.last_modified,
    )?
    .map(LifecycleExpirationHeader::from);

    Ok(ObjectBodyReadFound {
        object,
        completed_part,
        completed_part_count,
        object_range,
        expiration,
    })
}

/// Resolves object metadata and body reads inside one storage transaction.
fn resolve_get_object_read(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
    part_number: Option<i32>,
    range: Option<&S3Range>,
    conditions: &ObjectReadConditions,
) -> SqliteResult<(ObjectBodyReadStart, Vec<BlobReadRequest>)> {
    let object = load_object_body_reference(transaction, key, version_id)?;
    let Some(object) = object else {
        return Ok((ObjectBodyReadStart::Missing, Vec::new()));
    };
    if object.key_metadata.is_delete_marker {
        let named = version_id.map(|_| {
            (
                object.key_metadata.version_id.clone(),
                object.key_metadata.last_modified,
            )
        });
        return Ok((ObjectBodyReadStart::DeleteMarker(named), Vec::new()));
    }
    if let Err(failure) = validate_read_conditions(&object.key_metadata, conditions) {
        return Ok((
            ObjectBodyReadStart::ConditionFailed {
                failure,
                md5: object.key_metadata.md5.clone(),
            },
            Vec::new(),
        ));
    }

    let read = load_object_body_read_found(transaction, key, object, part_number, range)?;
    let (chunks, requests) = if read.object.key_metadata.stored_sse.is_some() {
        let (chunks, requests) = Sqlite::encrypted_object_read_plan(
            transaction,
            read.object.sequence,
            read.object_range.clone(),
        )?;
        (Some(chunks), requests)
    } else {
        (
            None,
            Sqlite::object_blob_read_requests(
                transaction,
                read.object.sequence,
                read.object_range.clone(),
            )?,
        )
    };

    Ok((
        ObjectBodyReadStart::Found {
            read: Box::new(read),
            chunks,
        },
        requests,
    ))
}

/// Builds the `GetObject` read state after storage metadata has been resolved.
async fn finish_get_object_read(
    sqlite: &Sqlite,
    bucket: &str,
    key: &str,
    start: ObjectBodyReadStart,
    body: BlobReadReceiver,
) -> S3Result<ObjectBodyRead> {
    match start {
        ObjectBodyReadStart::Missing => Err(not_found_delete_marker_error(false)),
        ObjectBodyReadStart::DeleteMarker(None) => Err(not_found_delete_marker_error(true)),
        ObjectBodyReadStart::DeleteMarker(Some((version_id, last_modified))) => {
            Err(delete_marker_version_error(&version_id, last_modified))
        }
        ObjectBodyReadStart::ConditionFailed { failure, md5 } => Err(match failure {
            ObjectReadConditionFailure::PreconditionFailed => {
                s3_error(S3ErrorCode::PreconditionFailed)
            }
            ObjectReadConditionFailure::NotModified => not_modified_error(md5.as_deref())?,
        }),
        ObjectBodyReadStart::Found { read, chunks } => {
            let body = if let Some(chunks) = chunks {
                let stored = read
                    .object
                    .key_metadata
                    .stored_sse
                    .clone()
                    .ok_or_else(|| s3_error(S3ErrorCode::InternalError))?;
                let active = open_encryption(sqlite, bucket, key, stored).await?;
                ObjectBodyReadBody::Stream(decrypting_snapshot_stream(
                    body,
                    chunks,
                    read.object_range.clone(),
                    Arc::new(active),
                ))
            } else {
                ObjectBodyReadBody::Blob(body)
            };
            Ok(ObjectBodyRead {
                object: read.object,
                completed_part: read.completed_part,
                completed_part_count: read.completed_part_count,
                object_range: read.object_range,
                expiration: read.expiration,
                body,
            })
        }
    }
}

/// Loads metadata and completed-part state for `HeadObject`.
fn load_head_object(
    transaction: &rusqlite::Transaction<'_>,
    key: &str,
    version_id: Option<&str>,
    part_number: Option<i32>,
) -> SqliteResult<Option<ObjectMetadataRead>> {
    let metadata = match version_id {
        Some(version_id) => Some(
            // A named delete marker is returned so HEAD can answer 405.
            Sqlite::get_version_metadata(transaction, key, version_id)?
                .ok_or(SqliteError::ObjectVersionNotFound)?,
        ),
        None => Sqlite::get_current_metadata(transaction, key)?,
    };
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    if metadata.is_delete_marker {
        return Ok(Some(ObjectMetadataRead {
            metadata,
            completed_part: None,
            completed_part_count: 0,
            tag_count: 0,
            expiration: None,
        }));
    }
    let target = Sqlite::get_object_version_tagging_target(transaction, key, &metadata.version_id)?
        .ok_or(SqliteError::ObjectVersionNotFound)?;
    let tag_count = Sqlite::get_object_tag_count(transaction, target.sequence)?;
    let expiration = Sqlite::lifecycle_expiration_for_object(
        transaction,
        key,
        &metadata.version_id,
        metadata.size,
        metadata.last_modified,
    )?
    .map(LifecycleExpirationHeader::from);
    let (completed_part, completed_part_count) =
        load_completed_part_info(transaction, key, &metadata.version_id, part_number)?;
    Ok(Some(ObjectMetadataRead {
        metadata,
        completed_part,
        completed_part_count,
        tag_count,
        expiration,
    }))
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
            Sqlite::get_completed_object_part(transaction, key, version_id, part_number)?,
            Sqlite::count_completed_object_parts(transaction, key, version_id)?,
        )),
        None => Ok((None, 0)),
    }
}

/// Builds the streaming body for a `GetObject` response.
fn get_object_response_body(body: ObjectBodyReadBody, content_length: u64) -> StreamingBlob {
    match body {
        ObjectBodyReadBody::Blob(body) => StreamingBlob::wrap::<_, S3Error>(
            stream::unfold(
                (body, content_length, false),
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
                        Some(Err(error)) => {
                            Some((Err(s3_error_from_sqlite(error)), (receiver, 0, true)))
                        }
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
            .fuse(),
        ),
        ObjectBodyReadBody::Stream(stream) => stream,
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
    let body = get_object_response_body(read.body, content_length);
    let response_checksum = response_checksum(
        key_metadata.checksum.as_ref(),
        read.completed_part.as_ref(),
        part_number.is_some(),
        checksum_mode,
        range.is_some(),
    );
    let checksum_type = checksum_type_output(&response_checksum);
    let object_lock_legal_hold_status = object_lock_legal_hold_status(&key_metadata.object_lock);
    let object_lock_mode = object_lock_mode(&key_metadata.object_lock);
    let object_lock_retain_until_date = object_lock_retain_until_date(&key_metadata.object_lock);
    let expiration = lifecycle_expiration_response_header(read.expiration)?;

    let (server_side_encryption, ssekms_key_id) =
        encryption_headers(key_metadata.stored_sse.as_ref().map(StoredSse::mode));
    Ok(GetObjectOutput {
        body: Some(body),
        accept_ranges: Some("bytes".to_string()),
        cache_control: key_metadata.cache_control,
        checksum_crc32: checksum_crc32(&response_checksum),
        checksum_crc32c: checksum_crc32c(&response_checksum),
        checksum_crc64nvme: checksum_crc64nvme(&response_checksum),
        checksum_sha1: checksum_sha1(&response_checksum),
        checksum_sha256: checksum_sha256(&response_checksum),
        checksum_sha512: checksum_sha512(&response_checksum),
        checksum_md5: checksum_md5(&response_checksum),
        checksum_xxhash3: checksum_xxhash3(&response_checksum),
        checksum_xxhash64: checksum_xxhash64(&response_checksum),
        checksum_xxhash128: checksum_xxhash128(&response_checksum),
        checksum_type,
        content_encoding: key_metadata.content_encoding,
        content_disposition: key_metadata.content_disposition,
        content_language: key_metadata.content_language,
        content_range: content_range_header(
            range.is_some(),
            part_number.is_some(),
            &object_range,
            object_size,
        ),
        content_length: Some(content_length_i64),
        content_type: key_metadata.content_type,
        expires: key_metadata.expires.map(format_expires).transpose()?,
        last_modified: Some(key_metadata.last_modified.into()),
        metadata: key_metadata.metadata,
        e_tag: key_metadata.md5.map(ETag::Strong),
        expiration,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
        parts_count: (read.completed_part_count > 0).then_some(read.completed_part_count),
        server_side_encryption,
        ssekms_key_id,
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
    let object_lock_legal_hold_status = object_lock_legal_hold_status(&object.object_lock);
    let object_lock_mode = object_lock_mode(&object.object_lock);
    let object_lock_retain_until_date = object_lock_retain_until_date(&object.object_lock);
    let expiration = lifecycle_expiration_response_header(read.expiration)?;

    let (server_side_encryption, ssekms_key_id) =
        encryption_headers(object.stored_sse.as_ref().map(StoredSse::mode));
    Ok(HeadObjectOutput {
        accept_ranges: Some("bytes".to_string()),
        cache_control: object.cache_control,
        checksum_crc32: checksum_crc32(&response_checksum),
        checksum_crc32c: checksum_crc32c(&response_checksum),
        checksum_crc64nvme: checksum_crc64nvme(&response_checksum),
        checksum_sha1: checksum_sha1(&response_checksum),
        checksum_sha256: checksum_sha256(&response_checksum),
        checksum_sha512: checksum_sha512(&response_checksum),
        checksum_md5: checksum_md5(&response_checksum),
        checksum_xxhash3: checksum_xxhash3(&response_checksum),
        checksum_xxhash64: checksum_xxhash64(&response_checksum),
        checksum_xxhash128: checksum_xxhash128(&response_checksum),
        checksum_type,
        content_encoding: object.content_encoding,
        content_disposition: object.content_disposition,
        content_language: object.content_language,
        content_length: Some(content_length),
        content_range: content_range_header(
            range.is_some(),
            part_number.is_some(),
            &object_range,
            object_size,
        ),
        content_type: Some(content_type),
        e_tag: object.md5.map(ETag::Strong),
        expiration,
        expires: object.expires.map(format_expires).transpose()?,
        last_modified: Some(object.last_modified.into()),
        metadata: object.metadata,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
        parts_count: (read.completed_part_count > 0).then_some(read.completed_part_count),
        server_side_encryption,
        ssekms_key_id,
        version_id: Some(object.version_id),
        ..Default::default()
    })
}

/// Returns an object body and metadata, applying byte-range requests when present.
pub(super) async fn get_object(
    sqlite: &Sqlite,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let S3Request { input, headers, .. } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_no_sse_headers(&headers)?;
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
    } = input;
    validate_range_request(range.as_ref(), part_number)?;

    let range_for_read = range;
    let conditions = ObjectReadConditions {
        etag_matches: if_match,
        modified_since: if_modified_since.map(Into::into),
        etag_none_matches: if_none_match,
        unmodified_since: if_unmodified_since.map(Into::into),
    };
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let object_key = key.clone();
    let (start, body) = connection
        .read_blob(move |transaction| {
            resolve_get_object_read(
                transaction,
                &key,
                version_id.as_deref(),
                part_number,
                range_for_read.as_ref(),
                &conditions,
            )
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    let read = finish_get_object_read(sqlite, &bucket, &object_key, start, body).await?;
    let output = get_object_output(read, range.as_ref(), part_number, checksum_mode.as_ref())?;
    Ok(S3Response::new(output))
}

/// Returns object metadata without reading the object body.
pub(super) async fn head_object(
    sqlite: &Sqlite,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let S3Request { input, headers, .. } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_no_sse_headers(&headers)?;
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
    } = input;
    validate_range_request(range.as_ref(), part_number)?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let version_requested = version_id.is_some();
    let read = connection
        .read(move |transaction| {
            load_head_object(transaction, &key, version_id.as_deref(), part_number)
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    let Some(read) = read else {
        return Err(not_found_delete_marker_error(false));
    };
    if read.metadata.is_delete_marker {
        return Err(if version_requested {
            delete_marker_version_error(&read.metadata.version_id, read.metadata.last_modified)
        } else {
            not_found_delete_marker_error(true)
        });
    }
    validate_loaded_read_conditions(
        &read.metadata,
        &ObjectReadConditions {
            etag_matches: if_match,
            modified_since: if_modified_since.map(Into::into),
            etag_none_matches: if_none_match,
            unmodified_since: if_unmodified_since.map(Into::into),
        },
    )?;
    if checksum_mode_enabled(checksum_mode.as_ref()) {
        authorize_checksum(sqlite, read.metadata.stored_sse.as_ref()).await?;
    }
    let tag_count = read.tag_count;
    let output = head_object_output(read, range.as_ref(), part_number, checksum_mode.as_ref())?;
    let mut response = S3Response::new(output);
    if tag_count > 0 {
        response.headers.insert(
            X_AMZ_TAGGING_COUNT,
            HeaderValue::from_str(&tag_count.to_string()).map_err(S3Error::internal_error)?,
        );
    }
    Ok(response)
}

/// Checks KMS access before returning protected checksum metadata.
///
/// SSE-S3 checksums need no key check, so only KMS envelopes are opened.
async fn authorize_checksum(sqlite: &Sqlite, envelope: Option<&StoredSse>) -> S3Result<()> {
    if let Some(StoredSse::Kms {
        key_id,
        ciphertext,
        context,
    }) = envelope
    {
        super::kms::decrypt(&sqlite.config, key_id, ciphertext, context).await?;
    }
    Ok(())
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
            checksum_sha512: checksum_sha512(&part.checksum),
            checksum_md5: checksum_md5(&part.checksum),
            checksum_xxhash3: checksum_xxhash3(&part.checksum),
            checksum_xxhash64: checksum_xxhash64(&part.checksum),
            checksum_xxhash128: checksum_xxhash128(&part.checksum),
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
pub(super) async fn get_object_attributes(
    sqlite: &Sqlite,
    req: S3Request<GetObjectAttributesInput>,
) -> S3Result<S3Response<GetObjectAttributesOutput>> {
    let S3Request { input, headers, .. } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_no_sse_headers(&headers)?;
    let GetObjectAttributesInput {
        bucket,
        key,
        version_id,
        object_attributes,
        max_parts,
        part_number_marker,
        ..
    } = input;

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
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (object, parts, total_parts_count) = connection
        .read(move |transaction| {
            let object = match version_id.as_deref() {
                Some(version_id) => Some(
                    Sqlite::get_version_metadata(transaction, &key, version_id)?
                        .filter(|metadata| metadata.is_delete_marker.not())
                        .ok_or(SqliteError::ObjectVersionNotFound)?,
                ),
                None => Sqlite::get_current_metadata(transaction, &key)?,
            };
            let Some(object) = object else {
                return Ok((None, Vec::new(), 0));
            };
            if object.is_delete_marker {
                return Ok((Some(object), Vec::new(), 0));
            }

            let total_parts_count =
                Sqlite::count_completed_object_parts(transaction, &key, &object.version_id)?;
            let parts = if wants_object_parts && total_parts_count > 0 {
                Sqlite::list_completed_object_parts(
                    transaction,
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

    if wants_checksum || wants_object_parts {
        authorize_checksum(sqlite, object.stored_sse.as_ref()).await?;
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

    use crate::{
        Config,
        sqlite::{FullObjectChecksum, PartChecksum},
    };

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

    #[test]
    #[should_panic(expected = "unexpected error")]
    fn assert_s3_ok_panics_on_error() {
        assert_s3_ok::<()>(Err(s3_error(S3ErrorCode::InvalidRequest)), &());
    }

    fn completed_part(part_number: i32, start: i64, size: i64) -> CompletedObjectPart {
        CompletedObjectPart {
            part_number,
            start,
            size,
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

    /// Builds storage with an empty bucket registry for request validation tests.
    fn sqlite_without_buckets() -> Sqlite {
        Sqlite {
            root: PathBuf::new(),
            config: Config::default(),
            buckets: Arc::new(RwLock::new(HashMap::new())),
            bucket_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            blob_chunk_size: crate::sqlite::DEFAULT_OBJECT_BLOB_CHUNK_SIZE,
            requests: Arc::new(tokio::sync::Semaphore::new(1)),
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
        assert_error_code(
            requested_object_range(None, Some(&range("bytes=0-")), None, 0, 0),
            &S3ErrorCode::InvalidRange,
        );
        assert_error_code(
            requested_object_range(None, Some(&range("bytes=-1")), None, 0, 0),
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
            &S3ErrorCode::Custom("InvalidPartNumber".into()),
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
        assert!(matches!(
            requested_object_range_sqlite(None, Some(&range("bytes=0-")), None, 0, 0),
            Err(SqliteError::InvalidObjectRange)
        ));
        assert!(matches!(
            requested_object_range_sqlite(None, Some(&range("bytes=-1")), None, 0, 0),
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
            Err(SqliteError::PartNumberNotSatisfiable)
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
