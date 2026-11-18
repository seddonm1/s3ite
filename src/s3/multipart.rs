use super::sse::{WriteEncryption, multipart_kms_headers, validate_multipart_mode};
use crate::sqlite::{
    ChecksumAccumulator, ChecksumTypeKind, CompletedMultipartUploadOutcome, CompletedObjectPart,
    EncodedBlobChunk, EncryptionMode, KeyValue, Multipart, MultipartChecksumConfig,
    MultipartCompletion, MultipartCompletionPart, MultipartCompletionRequest, MultipartPartWrite,
    MultipartUploadListEntry, MultipartUploadState, PartChecksum, StoredChecksum, StoredSse,
};

use super::copy::load_copy_source_object;
use super::metadata::parse_expires;
use super::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ActiveSse, ByteRange, CONTENT_MD5,
    ChecksumInput, CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CompletedPart, CopyPartResult, CopySource, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, Digest, ETag, Initiator, InternalError, ListMultipartUploadsInput,
    ListMultipartUploadsOutput, ListPartsInput, ListPartsOutput, Md5, MultipartUpload, Not,
    ObjectAclRequest, ObjectReadConditions, OffsetDateTime, Owner, Part,
    S3_SINGLE_REQUEST_BODY_MAX, S3Error, S3ErrorCode, S3Range, S3Request, S3Response, S3Result,
    Sqlite, SqliteError, SqliteResult, StorageClass, StreamingBlob, TryStreamExt,
    UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput, UploadPartOutput, Uuid,
    checksum_algorithm_output, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_input,
    checksum_md5, checksum_sha1, checksum_sha256, checksum_sha512, checksum_type_output,
    checksum_xxhash3, checksum_xxhash64, checksum_xxhash128, configured_part_checksum, copy_bytes,
    decode_list_input_value, decrypt_stored_chunks, default_crc64nvme_full_object_checksum,
    encode_list_value, encrypt_value_chunks, encryption_headers, finish_part_checksum, hex,
    is_url_encoding, load_bucket_object_lock_config, multipart_checksum_config,
    normalize_content_encoding, normalize_metadata, object_lock_for_new_version,
    object_lock_write_headers, object_write_conditions, open_encryption, output_delimiter, owner,
    part_checksum_accumulator, read_part_body_encrypted, requested_encryption,
    resolve_write_encryption, s3_error, s3_error_from_sqlite, s3_error_with_message,
    stored_tags_from_header, validate_content_md5, validate_declared_object_write_length,
    validate_expected_bucket_owner, validate_no_sse_headers, validate_no_unsupported_sse_headers,
    validate_object_acl,
};

/// Normalizes a completed multipart part list while preserving S3 ordering rules.
pub(super) fn normalize_completion_parts(
    parts: Vec<CompletedPart>,
) -> S3Result<Vec<MultipartCompletionPart>> {
    let mut previous_distinct_part_number = 0;
    let mut normalized = Vec::<MultipartCompletionPart>::with_capacity(parts.len());

    for part in parts {
        let part_number = part
            .part_number
            .ok_or_else(|| s3_error(S3ErrorCode::InvalidPart))?;
        validate_part_number(part_number)?;

        let e_tag = part
            .e_tag
            .and_then(ETag::into_strong)
            .ok_or_else(|| s3_error(S3ErrorCode::InvalidPart))?;

        let checksum = checksum_input!(part, None, None).into_part_checksum()?;
        if let Some(previous) = normalized.last_mut()
            && previous.part_number == part_number
        {
            previous.md5 = e_tag;
            previous.checksum = checksum;
            continue;
        }

        if part_number <= previous_distinct_part_number {
            return Err(s3_error(S3ErrorCode::InvalidPartOrder));
        }
        previous_distinct_part_number = part_number;

        normalized.push(MultipartCompletionPart {
            part_number,
            md5: e_tag,
            checksum,
        });
    }

    Ok(normalized)
}

/// Parses a multipart upload ID into the internal UUID representation.
pub(super) fn parse_upload_id(upload_id: &str) -> S3Result<Uuid> {
    Uuid::parse_str(upload_id).map_err(|_| s3_error(S3ErrorCode::NoSuchUpload))
}

/// Validates an S3 multipart part number.
pub(super) fn validate_part_number(part_number: i32) -> S3Result<()> {
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
pub(super) fn parse_copy_source_range(range: Option<String>) -> S3Result<Option<S3Range>> {
    range
        .map(|range| {
            S3Range::parse(&range)
                .map_err(|_| s3_error_with_message(S3ErrorCode::InvalidArgument, "invalid range"))
        })
        .transpose()
}

/// Normalizes an optional source range against object size.
pub(super) fn checked_copy_source_range(
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

/// Rejects copy-part ranges that exceed S3's single-request upload limit.
fn validate_upload_part_copy_size(range: ByteRange<u64>) -> SqliteResult<i64> {
    let size = range
        .end
        .checked_sub(range.start)
        .ok_or(SqliteError::InvalidObjectRange)?;
    let size = i64::try_from(size).map_err(|_| SqliteError::SingleRequestObjectTooLarge)?;
    if size > S3_SINGLE_REQUEST_BODY_MAX {
        return Err(SqliteError::SingleRequestObjectTooLarge);
    }
    Ok(size)
}

/// Returns an object byte slice for upload-copy storage.
pub(super) fn copied_part_bytes(
    object: &KeyValue,
    range: ByteRange<u64>,
) -> Result<Vec<u8>, SqliteError> {
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
pub(super) fn part_md5(value: &[u8]) -> String {
    let mut md5_hash = Md5::new();
    md5_hash.update(value);
    hex(md5_hash.finalize())
}

/// Buffers an uploaded part body while validating MD5 and explicit checksums.
async fn read_upload_part_body(
    body: StreamingBlob,
    checksum_input: &ChecksumInput,
    content_md5_present: bool,
    content_md5: Option<&str>,
) -> S3Result<(Vec<u8>, i64, String, Option<PartChecksum>)> {
    let mut checksum_accumulator = part_checksum_accumulator(checksum_input)?;
    let mut md5_hash = Md5::new();
    let stream = body.inspect_ok(|bytes| {
        let bytes = bytes.as_ref();
        md5_hash.update(bytes);
        if let Some(checksum_accumulator) = &mut checksum_accumulator {
            checksum_accumulator.update(bytes);
        }
    });
    let mut value = Vec::new();
    copy_bytes(stream, &mut value).await?;
    let md5_bytes = md5_hash.finalize();
    let md5 = hex(md5_bytes);
    let size = i64::try_from(value.len()).map_err(S3Error::internal_error)?;

    validate_content_md5(content_md5_present, content_md5, md5_bytes.as_ref())?;
    let explicit_checksum = finish_part_checksum(checksum_input, checksum_accumulator)?;
    Ok((value, size, md5, explicit_checksum))
}

/// Loads active upload state after validating upload ownership.
fn load_multipart_upload_state(
    transaction: &rusqlite::Transaction<'_>,
    upload_id: Uuid,
    bucket: &str,
    key: &str,
    access_key: Option<&str>,
) -> SqliteResult<MultipartUploadState> {
    let upload_state = Sqlite::get_multipart_upload_state(transaction, upload_id, bucket, key)?
        .ok_or(SqliteError::MultipartUploadNotFound)?;
    Sqlite::validate_multipart_access(upload_state.access_key.as_ref(), access_key)?;
    Ok(upload_state)
}

/// Revalidates active upload state inside an `UploadPart` write transaction.
fn validate_upload_part_write_state(
    transaction: &rusqlite::Transaction<'_>,
    upload_id: Uuid,
    bucket: &str,
    key: &str,
    access_key: Option<&str>,
    checksum_config: Option<MultipartChecksumConfig>,
    expected_encryption: Option<&StoredSse>,
) -> SqliteResult<()> {
    let upload_state = Sqlite::get_multipart_upload_state(transaction, upload_id, bucket, key)?
        .ok_or(SqliteError::MultipartUploadNotFound)?;
    Sqlite::validate_multipart_access(upload_state.access_key.as_ref(), access_key)?;
    if upload_state.checksum != checksum_config {
        return Err(SqliteError::ChecksumMismatch);
    }
    if upload_state.stored_sse.as_ref() != expected_encryption {
        return Err(SqliteError::ObjectBodyInconsistent);
    }

    Sqlite::touch_multipart_upload(transaction, upload_id)?;
    Ok(())
}

/// Reopens the multipart upload data key for encrypted part storage.
async fn active_upload_encryption(
    sqlite: &Sqlite,
    stored: Option<StoredSse>,
) -> S3Result<Option<ActiveSse>> {
    let Some(stored) = stored else {
        return Ok(None);
    };
    open_encryption(sqlite, stored).await.map(Some)
}

struct UploadPartWriteContext {
    bucket: String,
    key: String,
    upload_id: Uuid,
    part_number: i32,
    access_key: Option<String>,
    checksum_config: Option<MultipartChecksumConfig>,
    expected_encryption: Option<StoredSse>,
    chunk_size: usize,
}

/// Converts a stored completed part boundary into an object byte range.
pub(super) fn completed_part_range(part: &CompletedObjectPart) -> S3Result<ByteRange<u64>> {
    let start = u64::try_from(part.start).map_err(S3Error::internal_error)?;
    let size = u64::try_from(part.size).map_err(S3Error::internal_error)?;
    let end = start
        .checked_add(size)
        .ok_or_else(|| S3Error::with_message(InternalError, "completed part boundary overflows"))?;
    Ok(start..end)
}

/// Resolves `partNumber` against recorded completed multipart boundaries.
pub(super) fn requested_part_range(
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

#[derive(Debug)]
pub(super) struct CopiedPart {
    value: Vec<u8>,
    size: i64,
    md5: String,
}

struct UploadPartCopyWrite<'a> {
    request: &'a UploadPartCopyRequest,
    copied_part: CopiedPart,
    checksum_config: Option<MultipartChecksumConfig>,
    expected_stored_sse: Option<&'a StoredSse>,
    encoded_chunks: Option<Vec<EncodedBlobChunk>>,
    checksum: Option<PartChecksum>,
    chunk_size: usize,
}

#[derive(Clone)]
struct UploadPartCopyRequest {
    bucket: String,
    key: String,
    upload_id: Uuid,
    part_number: i32,
    source_bucket: String,
    source_key: String,
    source_version_id: Option<String>,
    source_conditions: ObjectReadConditions,
    copy_source_range: Option<S3Range>,
    access_key: Option<String>,
}

/// Normalizes `UploadPartCopy` input into storage-neutral source and destination identifiers.
fn parse_upload_part_copy_request(
    input: UploadPartCopyInput,
    access_key: Option<String>,
) -> S3Result<UploadPartCopyRequest> {
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
        copy_source_sse_customer_algorithm,
        copy_source_sse_customer_key,
        copy_source_sse_customer_key_md5,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        ..
    } = input;

    let upload_id = parse_upload_id(&upload_id)?;
    validate_part_number(part_number)?;
    let copy_source_range = parse_copy_source_range(copy_source_range)?;
    if copy_source_sse_customer_algorithm.is_some()
        || copy_source_sse_customer_key.is_some()
        || copy_source_sse_customer_key_md5.is_some()
        || sse_customer_algorithm.is_some()
        || sse_customer_key.is_some()
        || sse_customer_key_md5.is_some()
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "SSE-C is not supported",
        ));
    }
    let (source_bucket, source_key, source_version_id) = match copy_source {
        CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => {
            return Err(s3_error(S3ErrorCode::NotImplemented));
        }
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

    Ok(UploadPartCopyRequest {
        bucket,
        key,
        upload_id,
        part_number,
        source_bucket: source_bucket.to_string(),
        source_key: source_key.to_string(),
        source_version_id,
        source_conditions: ObjectReadConditions {
            etag_matches: copy_source_if_match,
            modified_since: copy_source_if_modified_since.map(Into::into),
            etag_none_matches: copy_source_if_none_match,
            unmodified_since: copy_source_if_unmodified_since.map(Into::into),
        },
        copy_source_range,
        access_key,
    })
}

/// Loads, condition-checks, and slices the source object for `UploadPartCopy`.
fn slice_copy_part(object: &KeyValue, request: &UploadPartCopyRequest) -> SqliteResult<CopiedPart> {
    let object_size =
        u64::try_from(object.key_metadata.size).map_err(|_| SqliteError::InvalidObjectRange)?;
    let byte_range = checked_copy_source_range(request.copy_source_range, object_size)?;
    let size = validate_upload_part_copy_size(byte_range.clone())?;
    let value = copied_part_bytes(object, byte_range)?;
    let md5 = part_md5(&value);
    Ok(CopiedPart { value, size, md5 })
}

/// Stores a copied multipart part after revalidating upload state.
fn store_upload_part_copy(
    transaction: &rusqlite::Transaction<'_>,
    write: UploadPartCopyWrite<'_>,
) -> SqliteResult<CopyPartResult> {
    let UploadPartCopyWrite {
        request,
        copied_part,
        checksum_config,
        expected_stored_sse,
        encoded_chunks,
        checksum,
        chunk_size,
    } = write;
    let upload_state = Sqlite::get_multipart_upload_state(
        transaction,
        request.upload_id,
        &request.bucket,
        &request.key,
    )?
    .ok_or(SqliteError::MultipartUploadNotFound)?;
    Sqlite::validate_multipart_access(
        upload_state.access_key.as_ref(),
        request.access_key.as_deref(),
    )?;
    if upload_state.checksum != checksum_config {
        return Err(SqliteError::ChecksumMismatch);
    }
    if upload_state.stored_sse.as_ref() != expected_stored_sse {
        return Err(SqliteError::ObjectBodyInconsistent);
    }

    let now = OffsetDateTime::now_utc();
    let md5 = copied_part.md5;
    let checksum_crc32 = checksum_crc32(&checksum);
    let checksum_crc32c = checksum_crc32c(&checksum);
    let checksum_crc64nvme = checksum_crc64nvme(&checksum);
    let checksum_sha1 = checksum_sha1(&checksum);
    let checksum_sha256 = checksum_sha256(&checksum);
    let checksum_sha512 = checksum_sha512(&checksum);
    let checksum_md5 = checksum_md5(&checksum);
    let checksum_xxhash3 = checksum_xxhash3(&checksum);
    let checksum_xxhash64 = checksum_xxhash64(&checksum);
    let checksum_xxhash128 = checksum_xxhash128(&checksum);
    Sqlite::touch_multipart_upload(transaction, request.upload_id)?;
    if let Some(chunks) = encoded_chunks {
        Sqlite::put_multipart_with_encoded_chunks(
            transaction,
            &MultipartPartWrite {
                upload_id: request.upload_id,
                part_number: request.part_number,
                last_modified: now,
                size: copied_part.size,
                md5: Some(md5.clone()),
                checksum,
            },
            chunks,
        )?;
    } else {
        let multipart = Multipart {
            upload_id: request.upload_id,
            part_number: request.part_number,
            last_modified: now,
            value: copied_part.value,
            size: copied_part.size,
            md5: Some(md5.clone()),
            checksum,
        };
        Sqlite::put_multipart_with_chunk_size(transaction, &multipart, chunk_size)?;
    }

    Ok(CopyPartResult {
        checksum_crc32,
        checksum_crc32c,
        checksum_crc64nvme,
        checksum_sha1,
        checksum_sha256,
        checksum_sha512,
        checksum_md5,
        checksum_xxhash3,
        checksum_xxhash64,
        checksum_xxhash128,
        e_tag: Some(ETag::Strong(md5)),
        last_modified: Some(now.into()),
    })
}

/// Builds the S3 `UploadPart` response from stored part identity and checksum.
fn upload_part_output(
    md5: String,
    checksum: Option<&PartChecksum>,
    encryption: Option<EncryptionMode>,
) -> UploadPartOutput {
    let checksum = checksum.cloned();
    let (server_side_encryption, ssekms_key_id) = encryption_headers(encryption);
    UploadPartOutput {
        e_tag: Some(ETag::Strong(md5)),
        checksum_crc32: checksum_crc32(&checksum),
        checksum_crc32c: checksum_crc32c(&checksum),
        checksum_crc64nvme: checksum_crc64nvme(&checksum),
        checksum_sha1: checksum_sha1(&checksum),
        checksum_sha256: checksum_sha256(&checksum),
        checksum_sha512: checksum_sha512(&checksum),
        checksum_md5: checksum_md5(&checksum),
        checksum_xxhash3: checksum_xxhash3(&checksum),
        checksum_xxhash64: checksum_xxhash64(&checksum),
        checksum_xxhash128: checksum_xxhash128(&checksum),
        server_side_encryption,
        ssekms_key_id,
        ..Default::default()
    }
}

/// Reads the source snapshot and revalidates the destination upload in its write transaction.
async fn copy_part_data(
    sqlite: &Sqlite,
    request: UploadPartCopyRequest,
    chunk_size: usize,
) -> S3Result<(CopyPartResult, Option<EncryptionMode>)> {
    let source = load_copy_source_object(
        sqlite,
        &request.source_bucket,
        &request.source_key,
        request.source_version_id.clone(),
        request.source_conditions.clone(),
    )
    .await?;
    let copied_part = slice_copy_part(&source.object, &request).map_err(s3_error_from_sqlite)?;
    let upload_state = {
        let upload_id = request.upload_id;
        let bucket = request.bucket.clone();
        let key = request.key.clone();
        let access_key = request.access_key.clone();
        let connection = sqlite
            .get_connection(&request.bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .read(move |transaction| {
                load_multipart_upload_state(
                    transaction,
                    upload_id,
                    &bucket,
                    &key,
                    access_key.as_deref(),
                )
            })
            .await
            .map_err(s3_error_from_sqlite)?
    };
    let checksum_config = upload_state.checksum;
    let expected_encryption = upload_state.stored_sse.clone();
    let encryption = active_upload_encryption(sqlite, upload_state.stored_sse).await?;
    let encrypted = encryption.as_ref().map(|active| active.stored.mode());
    let checksum = configured_part_checksum(checksum_config, &copied_part.value);
    let encoded_chunks = encryption
        .as_ref()
        .map(|encryption| encrypt_value_chunks(&copied_part.value, chunk_size, encryption))
        .transpose()?;

    let connection = sqlite
        .get_connection(&request.bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            let copy_part_result = store_upload_part_copy(
                transaction,
                UploadPartCopyWrite {
                    request: &request,
                    copied_part,
                    checksum_config,
                    expected_stored_sse: expected_encryption.as_ref(),
                    encoded_chunks,
                    checksum,
                    chunk_size,
                },
            )?;
            Ok((copy_part_result, encrypted))
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Maps one storage multipart upload summary into the S3 listing DTO.
fn multipart_upload_from_summary(
    summary: crate::sqlite::MultipartUploadSummary,
    encode_keys: bool,
    owner: &Owner,
    initiator: &Initiator,
) -> MultipartUpload {
    MultipartUpload {
        checksum_algorithm: checksum_algorithm_output(summary.checksum),
        checksum_type: checksum_type_output(summary.checksum),
        initiated: Some(summary.initiated.into()),
        initiator: Some(initiator.clone()),
        key: Some(encode_list_value(summary.key, encode_keys)),
        owner: Some(owner.clone()),
        storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
        upload_id: Some(summary.upload_id.to_string()),
    }
}

/// Splits storage multipart listing entries into upload and common-prefix DTOs.
fn multipart_upload_listing_entries(
    entries: Vec<MultipartUploadListEntry>,
    encode_keys: bool,
    owner: &Owner,
    initiator: &Initiator,
) -> (Vec<MultipartUpload>, Vec<CommonPrefix>) {
    let mut uploads = Vec::new();
    let mut common_prefixes = Vec::new();
    for entry in entries {
        match entry {
            MultipartUploadListEntry::Upload(summary) => {
                uploads.push(multipart_upload_from_summary(
                    summary,
                    encode_keys,
                    owner,
                    initiator,
                ));
            }
            MultipartUploadListEntry::CommonPrefix { prefix } => {
                common_prefixes.push(CommonPrefix {
                    prefix: Some(encode_list_value(prefix, encode_keys)),
                });
            }
        }
    }
    (uploads, common_prefixes)
}

/// Lists active multipart uploads in a bucket.
pub(super) async fn list_multipart_uploads(
    sqlite: &Sqlite,
    req: S3Request<ListMultipartUploadsInput>,
) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
    let ListMultipartUploadsInput {
        bucket,
        expected_bucket_owner,
        mut prefix,
        mut delimiter,
        mut key_marker,
        upload_id_marker,
        max_uploads,
        encoding_type,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    prefix = prefix.map(decode_list_input_value);
    delimiter = delimiter.map(decode_list_input_value);
    key_marker = key_marker.map(decode_list_input_value);

    let max_uploads = list_limit(max_uploads, 1000, 1000)?;
    let prefix_filter = prefix.clone().unwrap_or_default();
    let encode_keys = is_url_encoding(encoding_type.as_ref());
    let delimiter_filter = delimiter
        .as_deref()
        .filter(|delimiter| delimiter.is_empty().not())
        .map(str::to_string);
    let delimiter = output_delimiter(delimiter);
    let after_upload_id = if key_marker.is_some() {
        upload_id_marker
            .as_deref()
            .map(parse_upload_id)
            .transpose()?
    } else {
        None
    };

    let listing = {
        let prefix_filter = prefix_filter.clone();
        let delimiter_filter = delimiter_filter.clone();
        let key_marker = key_marker.clone();
        let connection = sqlite
            .get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .read(move |transaction| {
                Sqlite::list_multipart_upload_entries(
                    transaction,
                    &prefix_filter,
                    delimiter_filter.as_deref(),
                    key_marker.as_deref(),
                    after_upload_id,
                    i64::from(max_uploads),
                )
            })
            .await
            .map_err(s3_error_from_sqlite)?
    };
    let owner = owner();
    let initiator = Initiator {
        display_name: owner.display_name.clone(),
        id: owner.id.clone(),
    };
    let (uploads, common_prefixes) =
        multipart_upload_listing_entries(listing.entries, encode_keys, &owner, &initiator);

    let output = ListMultipartUploadsOutput {
        bucket: Some(bucket),
        common_prefixes: delimiter.as_ref().map(|_| common_prefixes),
        delimiter,
        encoding_type,
        is_truncated: Some(listing.is_truncated),
        key_marker,
        max_uploads: Some(max_uploads),
        next_key_marker: listing
            .next_key_marker
            .map(|marker| encode_list_value(marker, encode_keys)),
        next_upload_id_marker: listing.next_upload_id_marker,
        prefix,
        upload_id_marker,
        uploads: Some(uploads),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

/// Validates that create-multipart ACL inputs are owner-equivalent no-ops.
fn validate_create_multipart_upload_acl(input: &CreateMultipartUploadInput) -> S3Result<()> {
    validate_object_acl(&ObjectAclRequest {
        acl: input.acl.as_ref(),
        has_access_control_policy: false,
        grant_header_count: [
            input.grant_full_control.is_some(),
            input.grant_read.is_some(),
            input.grant_read_acp.is_some(),
            input.grant_write_acp.is_some(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count(),
    })
}

/// Validates the encryption settings fixed at multipart initiation.
fn requested_upload_encryption(
    input: &CreateMultipartUploadInput,
) -> S3Result<Option<WriteEncryption>> {
    requested_encryption(
        input.server_side_encryption.as_ref(),
        input.ssekms_key_id.as_deref(),
        input.ssekms_encryption_context.as_deref(),
        input.sse_customer_algorithm.is_some()
            || input.sse_customer_key.is_some()
            || input.sse_customer_key_md5.is_some(),
        input.bucket_key_enabled.is_some(),
    )
}

/// Creates multipart upload state for a future object.
pub(super) async fn create_multipart_upload(
    sqlite: &Sqlite,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    validate_expected_bucket_owner(req.input.expected_bucket_owner.as_deref())?;
    validate_no_unsupported_sse_headers(&req.headers)?;
    validate_create_multipart_upload_acl(&req.input)?;
    let explicit_encryption = requested_upload_encryption(&req.input)?;
    let CreateMultipartUploadInput {
        bucket,
        key,
        checksum_algorithm,
        checksum_type,
        expires,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
        ssekms_encryption_context,
        tagging,
        ..
    } = req.input;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let upload_id = Uuid::new_v4();
    let tags = stored_tags_from_header(tagging)?;
    let object_lock_headers = object_lock_write_headers(
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    )?;
    let object_lock_config = load_bucket_object_lock_config(sqlite, &bucket).await?;
    let object_lock = object_lock_for_new_version(
        &object_lock_config,
        object_lock_headers,
        OffsetDateTime::now_utc(),
    )?;
    let expires = parse_expires(expires.as_deref())?;
    let checksum = multipart_checksum_config(checksum_algorithm.clone(), checksum_type)?;
    let encryption =
        resolve_write_encryption(sqlite, &bucket, &key, explicit_encryption.as_ref()).await?;
    let stored_sse = encryption.map(|active| active.stored);
    let mode = stored_sse.as_ref().map(StoredSse::mode);

    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            Sqlite::create_multipart_upload(
                transaction,
                upload_id,
                &bucket_clone,
                &key_clone,
                MultipartUploadState {
                    access_key: req.credentials.map(|credentials| credentials.access_key),
                    metadata: normalize_metadata(req.input.metadata),
                    content_type: req.input.content_type,
                    content_encoding: normalize_content_encoding(req.input.content_encoding),
                    content_disposition: req.input.content_disposition,
                    content_language: req.input.content_language,
                    cache_control: req.input.cache_control,
                    expires,
                    checksum,
                    stored_sse,
                    object_lock,
                },
            )?;
            Sqlite::put_multipart_upload_tags(transaction, upload_id, &tags)?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let (server_side_encryption, ssekms_key_id) = encryption_headers(mode);
    let output = CreateMultipartUploadOutput {
        bucket: Some(bucket),
        key: Some(key),
        upload_id: Some(upload_id.to_string()),
        ssekms_encryption_context,
        checksum_algorithm,
        checksum_type: checksum_type_output(checksum),
        server_side_encryption,
        ssekms_key_id,
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

/// Reads, encrypts, and stores one multipart upload part as encoded chunks.
async fn upload_encrypted_part(
    sqlite: &Sqlite,
    context: UploadPartWriteContext,
    body: StreamingBlob,
    checksum_input: &ChecksumInput,
    content_md5_present: bool,
    content_md5: Option<String>,
    encryption: ActiveSse,
) -> S3Result<S3Response<UploadPartOutput>> {
    let encrypted_body = read_part_body_encrypted(
        body,
        checksum_input,
        context.checksum_config,
        content_md5_present,
        content_md5.as_deref(),
        context.chunk_size,
        &encryption,
    )
    .await?;
    let size = encrypted_body.size;
    let md5 = encrypted_body.md5;
    let checksum = encrypted_body.checksum;
    let md5_clone = md5.clone();
    let checksum_write = checksum.clone();
    let chunks = encrypted_body.chunks;
    let UploadPartWriteContext {
        bucket,
        key,
        upload_id,
        part_number,
        access_key,
        checksum_config,
        expected_encryption,
        ..
    } = context;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            validate_upload_part_write_state(
                transaction,
                upload_id,
                &bucket,
                &key,
                access_key.as_deref(),
                checksum_config,
                expected_encryption.as_ref(),
            )?;
            let part = MultipartPartWrite {
                upload_id,
                part_number,
                last_modified: OffsetDateTime::now_utc(),
                size,
                md5: Some(md5_clone),
                checksum: checksum_write,
            };
            Sqlite::put_multipart_with_encoded_chunks(transaction, &part, chunks)?;

            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(upload_part_output(
        md5,
        checksum.as_ref(),
        Some(encryption.stored.mode()),
    )))
}

/// Reads and stores one plain multipart upload part.
async fn upload_plain_part(
    sqlite: &Sqlite,
    context: UploadPartWriteContext,
    body: StreamingBlob,
    checksum_input: &ChecksumInput,
    content_md5_present: bool,
    content_md5: Option<String>,
) -> S3Result<S3Response<UploadPartOutput>> {
    let (value, size, md5, explicit_checksum) = read_upload_part_body(
        body,
        checksum_input,
        content_md5_present,
        content_md5.as_deref(),
    )
    .await?;
    let checksum =
        explicit_checksum.or_else(|| configured_part_checksum(context.checksum_config, &value));
    let md5_clone = md5.clone();
    let checksum_write = checksum.clone();
    let UploadPartWriteContext {
        bucket,
        key,
        upload_id,
        part_number,
        access_key,
        checksum_config,
        expected_encryption,
        chunk_size,
    } = context;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            validate_upload_part_write_state(
                transaction,
                upload_id,
                &bucket,
                &key,
                access_key.as_deref(),
                checksum_config,
                expected_encryption.as_ref(),
            )?;
            let multipart = Multipart {
                upload_id,
                part_number,
                last_modified: OffsetDateTime::now_utc(),
                value,
                size,
                md5: Some(md5_clone),
                checksum: checksum_write,
            };
            Sqlite::put_multipart_with_chunk_size(transaction, &multipart, chunk_size)?;

            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(upload_part_output(
        md5,
        checksum.as_ref(),
        None,
    )))
}

/// Loads active multipart upload state before an `UploadPart` body is consumed.
async fn load_upload_part_state(
    sqlite: &Sqlite,
    bucket: &str,
    key: &str,
    upload_id: Uuid,
    access_key: Option<&str>,
) -> S3Result<MultipartUploadState> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    let access_key = access_key.map(str::to_string);
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |transaction| {
            load_multipart_upload_state(
                transaction,
                upload_id,
                &bucket,
                &key,
                access_key.as_deref(),
            )
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Stores or replaces a multipart upload part.
pub(super) async fn upload_part(
    sqlite: &Sqlite,
    req: S3Request<UploadPartInput>,
) -> S3Result<S3Response<UploadPartOutput>> {
    let S3Request {
        input,
        headers,
        credentials,
        ..
    } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    let declares_kms = multipart_kms_headers(&headers)?;
    let access_key = credentials.map(|credentials| credentials.access_key);
    let checksum_input =
        checksum_input!(input, input.checksum_algorithm, None).with_sdk_algorithm(&headers)?;
    let UploadPartInput {
        body,
        bucket,
        key,
        upload_id,
        part_number,
        content_length,
        content_md5,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        ..
    } = input;

    if sse_customer_algorithm.is_some()
        || sse_customer_key.is_some()
        || sse_customer_key_md5.is_some()
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "SSE-C is not supported",
        ));
    }

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let upload_id = parse_upload_id(&upload_id)?;
    validate_part_number(part_number)?;
    validate_declared_object_write_length(&headers, content_length)?;
    let body = body.ok_or_else(|| s3_error(S3ErrorCode::IncompleteBody))?;

    let content_md5_present = headers.contains_key(CONTENT_MD5);
    let chunk_size = sqlite.blob_chunk_size();
    let upload_state =
        load_upload_part_state(sqlite, &bucket, &key, upload_id, access_key.as_deref()).await?;
    validate_multipart_mode(
        declares_kms,
        matches!(upload_state.stored_sse, Some(StoredSse::Kms { .. })),
    )?;
    let checksum_config = upload_state.checksum;
    checksum_input.validate_upload_part(checksum_config)?;
    let expected_encryption = upload_state.stored_sse.clone();
    let encryption = active_upload_encryption(sqlite, upload_state.stored_sse).await?;
    let context = UploadPartWriteContext {
        bucket,
        key,
        upload_id,
        part_number,
        access_key,
        checksum_config,
        expected_encryption,
        chunk_size,
    };

    if let Some(encryption) = encryption {
        upload_encrypted_part(
            sqlite,
            context,
            body,
            &checksum_input,
            content_md5_present,
            content_md5,
            encryption,
        )
        .await
    } else {
        upload_plain_part(
            sqlite,
            context,
            body,
            &checksum_input,
            content_md5_present,
            content_md5,
        )
        .await
    }
}

/// Copies an existing object byte range into a multipart upload part.
pub(super) async fn upload_part_copy(
    sqlite: &Sqlite,
    req: S3Request<UploadPartCopyInput>,
) -> S3Result<S3Response<UploadPartCopyOutput>> {
    let S3Request {
        input,
        headers,
        credentials,
        ..
    } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_expected_bucket_owner(input.expected_source_bucket_owner.as_deref())?;
    validate_no_sse_headers(&headers)?;
    let access_key = credentials.map(|credentials| credentials.access_key);
    let request = parse_upload_part_copy_request(input, access_key)?;
    sqlite
        .validate_mutable_bucket(&request.bucket)
        .map_err(s3_error_from_sqlite)?;
    let (copy_part_result, encrypted) =
        copy_part_data(sqlite, request, sqlite.blob_chunk_size()).await?;
    let (server_side_encryption, ssekms_key_id) = encryption_headers(encrypted);
    let output = UploadPartCopyOutput {
        copy_part_result: Some(copy_part_result),
        server_side_encryption,
        ssekms_key_id,
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

/// Lists parts uploaded for an active multipart upload.
pub(super) async fn list_parts(
    sqlite: &Sqlite,
    req: S3Request<ListPartsInput>,
) -> S3Result<S3Response<ListPartsOutput>> {
    let ListPartsInput {
        bucket,
        expected_bucket_owner,
        key,
        upload_id,
        max_parts,
        part_number_marker,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
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
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (upload_checksum, parts) = connection
        .read(move |transaction| {
            let upload_state = Sqlite::get_multipart_upload_state(
                transaction,
                upload_id,
                &bucket_clone,
                &key_clone,
            )?
            .ok_or(SqliteError::MultipartUploadNotFound)?;
            Sqlite::validate_multipart_access(
                upload_state.access_key.as_ref(),
                req.credentials
                    .as_ref()
                    .map(|credentials| credentials.access_key.as_str()),
            )?;

            let parts = Sqlite::list_multipart(
                transaction,
                upload_id,
                part_number_marker,
                i64::from(max_parts) + 1,
            )?;

            Ok((upload_state.checksum, parts))
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
            e_tag: part.md5.map(ETag::Strong),
            last_modified: Some(part.last_modified.into()),
            part_number: Some(part.part_number),
            size: Some(part.size),
        })
        .collect::<Vec<_>>();

    let output = ListPartsOutput {
        bucket: Some(bucket),
        checksum_algorithm: checksum_algorithm_output(upload_checksum),
        checksum_type: checksum_type_output(upload_checksum),
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
pub(super) async fn abort_multipart_upload(
    sqlite: &Sqlite,
    req: S3Request<AbortMultipartUploadInput>,
) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
    let AbortMultipartUploadInput {
        bucket,
        expected_bucket_owner,
        key,
        upload_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error(S3ErrorCode::NoSuchUpload))?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            let upload_state =
                Sqlite::get_multipart_upload_state(transaction, upload_id, &bucket, &key)?
                    .ok_or(SqliteError::MultipartUploadNotFound)?;
            Sqlite::validate_multipart_access(
                upload_state.access_key.as_ref(),
                req.credentials
                    .as_ref()
                    .map(|credentials| credentials.access_key.as_str()),
            )?;

            let deleted = Sqlite::delete_multipart(transaction, upload_id)?;
            if deleted == 0 {
                return Err(SqliteError::MultipartUploadNotFound);
            }

            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(AbortMultipartUploadOutput::default()))
}

/// Unwraps the upload key before reading encrypted parts for the completion checksum.
async fn precompute_multipart_completion_checksum(
    sqlite: &Sqlite,
    request: &MultipartCompletionRequest,
) -> S3Result<Option<StoredChecksum>> {
    let bucket = request.bucket.clone();
    let key = request.key.clone();
    let access_key = request.access_key.clone();
    let upload_id = request.upload_id;
    let requested_parts = request
        .requested_parts
        .iter()
        .map(|part| (part.part_number, part.md5.clone()))
        .collect::<Vec<_>>();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let state = connection
        .read(move |transaction| {
            let state = Sqlite::get_multipart_upload_state(transaction, upload_id, &bucket, &key)?;
            if let Some(state) = &state {
                Sqlite::validate_multipart_access(
                    state.access_key.as_ref(),
                    access_key.as_deref(),
                )?;
            }
            Ok(state)
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    let Some(state) = state else { return Ok(None) };
    let Some(stored) = state.stored_sse else {
        return Ok(None);
    };
    let active = open_encryption(sqlite, stored).await?;
    let checksum_config = MultipartChecksumConfig::for_completion(
        state.checksum,
        &request.requested_checksum,
        request.default_checksum,
    )
    .map_err(s3_error_from_sqlite)?;
    if checksum_config.checksum_type != ChecksumTypeKind::FullObject {
        return Ok(None);
    }
    connection
        .read(move |transaction| {
            let limit = i64::try_from(requested_parts.len())
                .map_err(|_| SqliteError::MultipartObjectTooLarge)?
                .checked_add(1)
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
            let parts = Sqlite::list_multipart(transaction, upload_id, 0, limit)?;
            if parts.len() != requested_parts.len() {
                return Ok(None);
            }

            let mut accumulator = ChecksumAccumulator::new(checksum_config.algorithm);
            for ((number, etag), part) in requested_parts.iter().zip(&parts) {
                if *number != part.part_number || part.md5.as_ref() != Some(etag) {
                    return Err(SqliteError::InvalidMultipartPart);
                }
                let chunks = Sqlite::get_upload_part_blob_chunks(
                    transaction,
                    upload_id,
                    part.part_number,
                    part.size,
                )?;
                let plaintext = decrypt_stored_chunks(&active, chunks)?;
                accumulator.update(&plaintext);
            }

            accumulator
                .finish_stored(ChecksumTypeKind::FullObject)
                .ok_or(SqliteError::ChecksumMismatch)
                .map(Some)
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Normalizes and validates the S3 complete-multipart request shape.
fn normalize_multipart_completion_request(
    input: CompleteMultipartUploadInput,
    access_key: Option<String>,
) -> S3Result<MultipartCompletionRequest> {
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    let checksum_input = checksum_input!(input, None, input.checksum_type);
    let CompleteMultipartUploadInput {
        multipart_upload,
        bucket,
        key,
        upload_id,
        if_match,
        if_none_match,
        ..
    } = input;
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

    Ok(MultipartCompletionRequest {
        bucket,
        key,
        upload_id,
        requested_parts: normalize_completion_parts(requested_parts)?,
        requested_checksum: checksum_input.into_completion_checksum()?,
        default_checksum: default_crc64nvme_full_object_checksum(),
        access_key,
        write_conditions: object_write_conditions(if_match, if_none_match),
        precomputed_full_object_checksum: None,
    })
}

/// Prepares multipart completion against a read snapshot before the write.
async fn prepare_multipart_completion(
    sqlite: &Sqlite,
    request: MultipartCompletionRequest,
) -> S3Result<(MultipartCompletionRequest, MultipartCompletion)> {
    let bucket = request.bucket.clone();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |transaction| {
            let completion = Sqlite::prepare_multipart_upload_completion(transaction, &request)?;
            Ok((request, completion))
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Writes a prepared multipart completion or returns an idempotent retry result.
async fn write_multipart_completion(
    sqlite: &Sqlite,
    bucket: &str,
    request: MultipartCompletionRequest,
    completion: MultipartCompletion,
) -> S3Result<CompletedMultipartUploadOutcome> {
    match completion {
        MultipartCompletion::AlreadyCompleted(completed) => Ok(completed),
        MultipartCompletion::Prepared(prepared) => {
            let connection = sqlite
                .get_connection(bucket)
                .await
                .map_err(s3_error_from_sqlite)?;
            connection
                .write(move |transaction| {
                    Sqlite::put_prepared_multipart_upload(transaction, &request, prepared)
                })
                .await
                .map_err(s3_error_from_sqlite)
        }
    }
}

/// Builds the S3 completion response from stored completion metadata.
fn complete_multipart_upload_output(
    bucket: String,
    key: String,
    completed: CompletedMultipartUploadOutcome,
) -> CompleteMultipartUploadOutput {
    let (server_side_encryption, ssekms_key_id) = encryption_headers(completed.encryption);
    CompleteMultipartUploadOutput {
        bucket: Some(bucket),
        key: Some(key),
        e_tag: Some(ETag::Strong(completed.md5)),
        version_id: completed.returned_version_id,
        checksum_crc32: checksum_crc32(&completed.checksum),
        checksum_crc32c: checksum_crc32c(&completed.checksum),
        checksum_crc64nvme: checksum_crc64nvme(&completed.checksum),
        checksum_sha1: checksum_sha1(&completed.checksum),
        checksum_sha256: checksum_sha256(&completed.checksum),
        checksum_sha512: checksum_sha512(&completed.checksum),
        checksum_md5: checksum_md5(&completed.checksum),
        checksum_xxhash3: checksum_xxhash3(&completed.checksum),
        checksum_xxhash64: checksum_xxhash64(&completed.checksum),
        checksum_xxhash128: checksum_xxhash128(&completed.checksum),
        checksum_type: checksum_type_output(&completed.checksum),
        server_side_encryption,
        ssekms_key_id,
        ..Default::default()
    }
}

/// Completes a multipart upload and records the result for idempotent retries.
pub(super) async fn complete_multipart_upload(
    sqlite: &Sqlite,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    let declares_kms = multipart_kms_headers(&req.headers)?;
    let access_key = req.credentials.map(|credentials| credentials.access_key);
    let mut request = normalize_multipart_completion_request(req.input, access_key)?;

    sqlite
        .validate_mutable_bucket(&request.bucket)
        .map_err(s3_error_from_sqlite)?;

    request.precomputed_full_object_checksum =
        precompute_multipart_completion_checksum(sqlite, &request).await?;
    let bucket = request.bucket.clone();
    let key = request.key.clone();
    let (request, completion) = prepare_multipart_completion(sqlite, request).await?;
    let uses_kms = match &completion {
        MultipartCompletion::AlreadyCompleted(result) => {
            matches!(result.encryption, Some(EncryptionMode::Kms(_)))
        }
        MultipartCompletion::Prepared(result) => {
            matches!(result.stored_sse, Some(StoredSse::Kms { .. }))
        }
    };
    validate_multipart_mode(declares_kms, uses_kms)?;
    let completed = write_multipart_completion(sqlite, &bucket, request, completion).await?;

    Ok(S3Response::new(complete_multipart_upload_output(
        bucket, key, completed,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sqlite::{KeyMetadata, PartChecksum, StoredObjectLock};

    fn assert_error_code<T>(result: S3Result<T>, code: &S3ErrorCode) {
        let err = result.err();
        assert!(
            err.as_ref().is_some_and(|err| err.code() == code),
            "expected {code:?}"
        );
    }

    fn part(part_number: Option<i32>, e_tag: Option<ETag>) -> CompletedPart {
        CompletedPart {
            part_number,
            e_tag,
            ..CompletedPart::default()
        }
    }

    /// Builds a source object for multipart range and body checks.
    fn key_value(value: Option<Vec<u8>>, size: i64) -> KeyValue {
        KeyValue {
            key: "key".to_string(),
            value,
            key_metadata: KeyMetadata {
                version_id: "version".to_string(),
                size,
                metadata: None,
                content_type: None,
                content_encoding: None,
                content_disposition: None,
                content_language: None,
                cache_control: None,
                expires: None,
                last_modified: OffsetDateTime::UNIX_EPOCH,
                md5: Some("md5".to_string()),
                checksum: None,
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
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

    fn completed_part(part_number: i32, start: i64, size: i64) -> CompletedObjectPart {
        CompletedObjectPart {
            part_number,
            start,
            size,
            checksum: Some(PartChecksum::Sha256("sha".to_string())),
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

    #[test]
    fn validates_part_number_boundaries() {
        assert_error_code(validate_part_number(0), &S3ErrorCode::InvalidArgument);
        assert!(validate_part_number(1).is_ok());
        assert!(validate_part_number(10_000).is_ok());
        assert_error_code(validate_part_number(10_001), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn parses_upload_ids() {
        let uuid = Uuid::new_v4();
        assert_eq!(parse_upload_id(&uuid.to_string()).ok(), Some(uuid));
        assert_error_code(parse_upload_id("not-a-uuid"), &S3ErrorCode::NoSuchUpload);
    }

    #[test]
    fn list_limit_rejects_zero_and_negative_values() {
        assert_eq!(list_limit(None, 10, 100).ok(), Some(10));
        assert_eq!(list_limit(Some(200), 10, 100).ok(), Some(100));
        assert_error_code(list_limit(Some(0), 10, 100), &S3ErrorCode::InvalidArgument);
        assert_error_code(list_limit(Some(-1), 10, 100), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn normalizes_completion_parts_and_replaces_duplicate_part() {
        let normalized = match normalize_completion_parts(vec![
            CompletedPart {
                checksum_sha256: Some("old".to_string()),
                ..part(Some(1), Some(ETag::Strong("etag-1-old".to_string())))
            },
            CompletedPart {
                checksum_sha256: Some("new".to_string()),
                ..part(Some(1), Some(ETag::Strong("etag-1-new".to_string())))
            },
            part(Some(2), Some(ETag::Strong("etag-2".to_string()))),
        ]) {
            Ok(parts) => parts,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0].part_number, 1);
        assert_eq!(normalized[0].md5, "etag-1-new");
        assert_eq!(
            normalized[0].checksum,
            Some(PartChecksum::Sha256("new".to_string()))
        );
        assert_eq!(normalized[1].part_number, 2);
        assert_eq!(normalized[1].md5, "etag-2");
    }

    #[test]
    fn rejects_invalid_completion_parts() {
        assert_error_code(
            normalize_completion_parts(vec![part(None, Some(ETag::Strong("etag".to_string())))]),
            &S3ErrorCode::InvalidPart,
        );
        assert_error_code(
            normalize_completion_parts(vec![part(Some(1), None)]),
            &S3ErrorCode::InvalidPart,
        );
        assert_error_code(
            normalize_completion_parts(vec![part(Some(1), Some(ETag::Weak("etag".to_string())))]),
            &S3ErrorCode::InvalidPart,
        );
        assert_error_code(
            normalize_completion_parts(vec![
                part(Some(2), Some(ETag::Strong("etag-2".to_string()))),
                part(Some(1), Some(ETag::Strong("etag-1".to_string()))),
            ]),
            &S3ErrorCode::InvalidPartOrder,
        );
    }

    #[test]
    fn parses_and_checks_copy_source_ranges() {
        assert_eq!(parse_copy_source_range(None).ok().flatten(), None);

        let range = parse_copy_source_range(Some("bytes=2-5".to_string()))
            .ok()
            .flatten();
        let Some(range) = range else {
            unreachable!("valid copy-source range should parse");
        };
        assert_sqlite_ok(checked_copy_source_range(Some(range), 10), &(2..6));

        let range = parse_copy_source_range(Some("bytes=-4".to_string()))
            .ok()
            .flatten();
        let Some(range) = range else {
            unreachable!("valid suffix copy-source range should parse");
        };
        assert_sqlite_ok(checked_copy_source_range(Some(range), 10), &(6..10));

        let range = parse_copy_source_range(Some("bytes=8-20".to_string()))
            .ok()
            .flatten();
        let Some(range) = range else {
            unreachable!("syntactically valid out-of-bounds range should parse");
        };
        assert!(matches!(
            checked_copy_source_range(Some(range), 10),
            Err(SqliteError::InvalidObjectRange)
        ));

        let range = parse_copy_source_range(Some("bytes=-0".to_string()))
            .ok()
            .flatten();
        let Some(range) = range else {
            unreachable!("syntactically valid empty suffix range should parse");
        };
        assert!(matches!(
            checked_copy_source_range(Some(range), 10),
            Err(SqliteError::InvalidObjectRange)
        ));

        assert_error_code(
            parse_copy_source_range(Some("not-a-range".to_string())),
            &S3ErrorCode::InvalidArgument,
        );
    }

    #[test]
    fn validates_upload_part_copy_size_boundaries() {
        assert_eq!(
            validate_upload_part_copy_size(0..S3_SINGLE_REQUEST_BODY_MAX as u64).ok(),
            Some(S3_SINGLE_REQUEST_BODY_MAX)
        );
        assert!(matches!(
            validate_upload_part_copy_size(0..(S3_SINGLE_REQUEST_BODY_MAX as u64 + 1)),
            Err(SqliteError::SingleRequestObjectTooLarge)
        ));
        assert!(matches!(
            validate_upload_part_copy_size(ByteRange { start: 10, end: 5 }),
            Err(SqliteError::InvalidObjectRange)
        ));
    }

    #[test]
    fn upload_part_copy_rejects_access_point_sources() -> Result<(), s3s::dto::builders::BuildError>
    {
        let input = UploadPartCopyInput::builder()
            .bucket("bucket".to_string())
            .key("key".to_string())
            .upload_id(Uuid::new_v4().to_string())
            .part_number(1)
            .copy_source(access_point_source())
            .build()?;
        assert_error_code(
            parse_upload_part_copy_request(input, None),
            &S3ErrorCode::NotImplemented,
        );
        Ok(())
    }

    #[test]
    fn copies_part_byte_slices() {
        let object = key_value(Some(b"abcdef".to_vec()), 6);
        assert_sqlite_ok(copied_part_bytes(&object, 0..6), &b"abcdef".to_vec());
        assert_sqlite_ok(copied_part_bytes(&object, 2..5), &b"cde".to_vec());
        assert!(matches!(
            copied_part_bytes(&object, 5..7),
            Err(SqliteError::InvalidObjectRange)
        ));

        let empty = key_value(None, 0);
        assert_sqlite_ok(copied_part_bytes(&empty, 0..0), &Vec::new());

        let inconsistent = key_value(None, 1);
        assert!(matches!(
            copied_part_bytes(&inconsistent, 0..1),
            Err(SqliteError::Configuration(_))
        ));
    }

    #[test]
    fn resolves_completed_part_ranges() {
        let part = completed_part(2, 5, 3);
        assert_eq!(completed_part_range(&part).ok(), Some(5..8));
        assert_eq!(requested_part_range(2, Some(&part), 2, 9).ok(), Some(5..8));
        assert_eq!(requested_part_range(1, None, 0, 9).ok(), Some(0..9));
        assert_error_code(
            requested_part_range(2, None, 2, 9),
            &S3ErrorCode::InvalidPart,
        );
    }

    #[test]
    fn rejects_completed_part_range_invalid_boundaries() {
        let part = completed_part(1, -1, 1);
        assert_error_code(completed_part_range(&part), &S3ErrorCode::InternalError);

        let part = completed_part(1, 0, -1);
        assert_error_code(completed_part_range(&part), &S3ErrorCode::InternalError);
    }
}
