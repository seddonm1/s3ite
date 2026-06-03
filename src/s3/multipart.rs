use std::collections::HashSet;

use crate::database::BucketConnection;

use super::copy::validate_copy_source_conditions;
use super::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ByteRange, CONTENT_MD5, ChecksumInput,
    CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CompletedObjectPart,
    CompletedPart, CopyPartResult, CopySource, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, Digest, ETag, Initiator, InternalError, KeyValue, LIST_BATCH_SIZE,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListPartsInput, ListPartsOutput, Md5,
    Multipart, MultipartChecksumConfig, MultipartCompletion, MultipartCompletionPart,
    MultipartCompletionRequest, MultipartUpload, MultipartUploadState, MultipartUploadSummary, Not,
    ObjectAclRequest, ObjectReadConditions, OffsetDateTime, Part, PartChecksum, S3Error,
    S3ErrorCode, S3Range, S3Request, S3Response, S3Result, Sqlite, SqliteError, SqliteResult,
    StorageClass, StreamingBlob, TryStreamExt, UploadPartCopyInput, UploadPartCopyOutput,
    UploadPartInput, UploadPartOutput, Uuid, checksum_algorithm_output, checksum_crc32,
    checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type_output,
    common_prefix_for_key, configured_part_checksum, copy_bytes, debug, decode_list_input_value,
    default_crc64nvme_full_object_checksum, encode_list_value, finish_part_checksum, hex,
    is_url_encoding, multipart_checksum_config, normalize_content_encoding,
    object_write_conditions, output_delimiter, owner, part_checksum_accumulator, s3_error,
    s3_error_from_sqlite, s3_error_with_message, validate_content_md5, validate_no_sse_headers,
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

        if let Some(previous) = normalized.last_mut()
            && previous.part_number == part_number
        {
            previous.md5 = e_tag;
            previous.checksum = ChecksumInput {
                crc32: part.checksum_crc32,
                crc32c: part.checksum_crc32c,
                crc64nvme: part.checksum_crc64nvme,
                sha1: part.checksum_sha1,
                sha256: part.checksum_sha256,
                ..ChecksumInput::default()
            }
            .into_part_checksum()?;
            continue;
        }

        if part_number <= previous_distinct_part_number {
            return Err(s3_error(S3ErrorCode::InvalidPartOrder));
        }
        previous_distinct_part_number = part_number;

        normalized.push(MultipartCompletionPart {
            part_number,
            md5: e_tag,
            checksum: ChecksumInput {
                crc32: part.checksum_crc32,
                crc32c: part.checksum_crc32c,
                crc64nvme: part.checksum_crc64nvme,
                sha1: part.checksum_sha1,
                sha256: part.checksum_sha256,
                ..ChecksumInput::default()
            }
            .into_part_checksum()?,
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

/// Loads an active upload checksum configuration after validating upload ownership.
async fn read_multipart_checksum_config(
    connection: &BucketConnection,
    upload_id: Uuid,
    bucket: &str,
    key: &str,
    access_key: Option<&str>,
) -> S3Result<Option<MultipartChecksumConfig>> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    let access_key = access_key.map(str::to_string);
    connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            let upload_state =
                Sqlite::get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                    .ok_or(SqliteError::MultipartUploadNotFound)?;
            Sqlite::validate_multipart_access(
                upload_state.access_key.as_ref(),
                access_key.as_deref(),
            )?;
            Ok(upload_state.checksum)
        })
        .await
        .map_err(s3_error_from_sqlite)
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

/// Normalizes an `UploadPartCopy` input into the local copy-part request shape.
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
        ..
    } = input;

    let upload_id = parse_upload_id(&upload_id)?;
    validate_part_number(part_number)?;
    let copy_source_range = parse_copy_source_range(copy_source_range)?;
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
fn load_upload_part_copy_source(
    transaction: &rusqlite::Transaction<'_>,
    request: &UploadPartCopyRequest,
) -> SqliteResult<CopiedPart> {
    let object = Sqlite::get_copy_source_object(
        transaction,
        &request.source_key,
        request.source_version_id.as_deref(),
    )?;
    validate_copy_source_conditions(&object.key_metadata, &request.source_conditions)?;
    let object_size =
        u64::try_from(object.key_metadata.size).map_err(|_| SqliteError::InvalidObjectRange)?;
    let byte_range = checked_copy_source_range(request.copy_source_range, object_size)?;
    let value = copied_part_bytes(&object, byte_range)?;
    let size = i64::try_from(value.len()).map_err(|_| SqliteError::MultipartObjectTooLarge)?;
    let md5 = part_md5(&value);
    Ok(CopiedPart { value, size, md5 })
}

/// Stores a copied multipart part after revalidating upload state.
fn store_upload_part_copy(
    transaction: &rusqlite::Transaction<'_>,
    request: &UploadPartCopyRequest,
    copied_part: CopiedPart,
    checksum_config: Option<MultipartChecksumConfig>,
    checksum: Option<PartChecksum>,
) -> SqliteResult<CopyPartResult> {
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

    let now = OffsetDateTime::now_utc();
    let md5 = copied_part.md5;
    let checksum_crc32 = checksum_crc32(&checksum);
    let checksum_crc32c = checksum_crc32c(&checksum);
    let checksum_crc64nvme = checksum_crc64nvme(&checksum);
    let checksum_sha1 = checksum_sha1(&checksum);
    let checksum_sha256 = checksum_sha256(&checksum);
    Sqlite::touch_multipart_upload(transaction, request.upload_id)?;
    Sqlite::put_multipart(
        transaction,
        Multipart {
            upload_id: request.upload_id,
            part_number: request.part_number,
            last_modified: now,
            value: copied_part.value,
            size: copied_part.size,
            md5: Some(md5.clone()),
            checksum,
        },
    )?;

    Ok(CopyPartResult {
        checksum_crc32,
        checksum_crc32c,
        checksum_crc64nvme,
        checksum_sha1,
        checksum_sha256,
        e_tag: Some(ETag::Strong(md5)),
        last_modified: Some(now.into()),
    })
}

/// Builds the S3 `UploadPart` response from stored part identity and checksum.
fn upload_part_output(md5: String, checksum: Option<&PartChecksum>) -> UploadPartOutput {
    let checksum = checksum.cloned();
    UploadPartOutput {
        e_tag: Some(ETag::Strong(md5)),
        checksum_crc32: checksum_crc32(&checksum),
        checksum_crc32c: checksum_crc32c(&checksum),
        checksum_crc64nvme: checksum_crc64nvme(&checksum),
        checksum_sha1: checksum_sha1(&checksum),
        checksum_sha256: checksum_sha256(&checksum),
        ..Default::default()
    }
}

/// Copies a part within one bucket while keeping source reads out of the writer.
async fn upload_part_copy_same_bucket(
    sqlite: &Sqlite,
    request: UploadPartCopyRequest,
) -> S3Result<CopyPartResult> {
    let connection = sqlite
        .get_connection(&request.bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let source_request = request.clone();
    let copied_part = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            load_upload_part_copy_source(&transaction, &source_request)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let checksum_config = read_multipart_checksum_config(
        &connection,
        request.upload_id,
        &request.bucket,
        &request.key,
        request.access_key.as_deref(),
    )
    .await?;
    let checksum = configured_part_checksum(checksum_config, &copied_part.value);

    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let copy_part_result = store_upload_part_copy(
                &transaction,
                &request,
                copied_part,
                checksum_config,
                checksum,
            )?;
            transaction.commit()?;
            Ok(copy_part_result)
        })
        .await
        .map_err(s3_error_from_sqlite)
}

/// Copies a part across buckets by reading source state before writing the target.
async fn upload_part_copy_cross_bucket(
    sqlite: &Sqlite,
    request: UploadPartCopyRequest,
) -> S3Result<CopyPartResult> {
    let source_connection = sqlite
        .get_connection(&request.source_bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let source_request = UploadPartCopyRequest {
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        upload_id: request.upload_id,
        part_number: request.part_number,
        source_bucket: request.source_bucket.clone(),
        source_key: request.source_key.clone(),
        source_version_id: request.source_version_id.clone(),
        source_conditions: request.source_conditions.clone(),
        copy_source_range: request.copy_source_range,
        access_key: request.access_key.clone(),
    };
    let copied_part = source_connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            load_upload_part_copy_source(&transaction, &source_request)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let connection = sqlite
        .get_connection(&request.bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let checksum_config = read_multipart_checksum_config(
        &connection,
        request.upload_id,
        &request.bucket,
        &request.key,
        request.access_key.as_deref(),
    )
    .await?;
    let checksum = configured_part_checksum(checksum_config, &copied_part.value);

    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let copy_part_result = store_upload_part_copy(
                &transaction,
                &request,
                copied_part,
                checksum_config,
                checksum,
            )?;
            transaction.commit()?;
            Ok(copy_part_result)
        })
        .await
        .map_err(s3_error_from_sqlite)
}

pub(super) struct MultipartUploadListing {
    uploads: Vec<MultipartUploadSummary>,
    common_prefixes: Vec<CommonPrefix>,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
    is_truncated: bool,
}

impl Sqlite {
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
            .get_connection(bucket)
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
                        Self::list_multipart_uploads(
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

/// Lists active multipart uploads in a bucket.
pub(super) async fn list_multipart_uploads(
    sqlite: &Sqlite,
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
    let delimiter = output_delimiter(delimiter);
    let after_upload_id = if key_marker.is_some() {
        upload_id_marker
            .as_deref()
            .map(parse_upload_id)
            .transpose()?
    } else {
        None
    };

    let listing = sqlite
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
            checksum_algorithm: checksum_algorithm_output(summary.checksum),
            checksum_type: checksum_type_output(summary.checksum),
            initiated: Some(summary.initiated.into()),
            initiator: Some(initiator.clone()),
            key: Some(encode_list_value(summary.key, encode_keys)),
            owner: Some(owner.clone()),
            storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
            upload_id: Some(summary.upload_id.to_string()),
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

/// Creates multipart upload state for a future object.
pub(super) async fn create_multipart_upload(
    sqlite: &Sqlite,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    validate_no_sse_headers(&req.headers)?;
    let CreateMultipartUploadInput {
        acl,
        bucket,
        key,
        cache_control,
        content_encoding,
        content_type,
        checksum_algorithm,
        checksum_type,
        expires,
        grant_full_control,
        grant_read,
        grant_read_acp,
        grant_write_acp,
        metadata,
        ..
    } = req.input;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
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

    let upload_id = Uuid::new_v4();
    let checksum = multipart_checksum_config(checksum_algorithm.clone(), checksum_type)?;

    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            Sqlite::create_multipart_upload(
                &transaction,
                upload_id,
                &bucket_clone,
                &key_clone,
                MultipartUploadState {
                    access_key: req.credentials.map(|credentials| credentials.access_key),
                    metadata,
                    content_type,
                    content_encoding: normalize_content_encoding(content_encoding),
                    cache_control,
                    expires: expires.map(Into::into),
                    checksum,
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
        checksum_algorithm,
        checksum_type: checksum_type_output(checksum),
        ..Default::default()
    };

    Ok(S3Response::new(output))
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
    validate_no_sse_headers(&headers)?;
    let access_key = credentials.map(|credentials| credentials.access_key);
    let UploadPartInput {
        body,
        bucket,
        key,
        upload_id,
        part_number,
        content_md5,
        checksum_algorithm,
        checksum_crc32: request_checksum_crc32,
        checksum_crc32c: request_checksum_crc32c,
        checksum_crc64nvme: request_checksum_crc64nvme,
        checksum_sha1: request_checksum_sha1,
        checksum_sha256: request_checksum_sha256,
        ..
    } = input;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let body = body.ok_or_else(|| s3_error(S3ErrorCode::IncompleteBody))?;
    let upload_id = parse_upload_id(&upload_id)?;
    validate_part_number(part_number)?;

    let checksum_input = ChecksumInput {
        algorithm: checksum_algorithm,
        crc32: request_checksum_crc32,
        crc32c: request_checksum_crc32c,
        crc64nvme: request_checksum_crc64nvme,
        sha1: request_checksum_sha1,
        sha256: request_checksum_sha256,
        ..ChecksumInput::default()
    };
    let (value, size, md5, explicit_checksum) = read_upload_part_body(
        body,
        &checksum_input,
        headers.contains_key(CONTENT_MD5),
        content_md5.as_deref(),
    )
    .await?;

    let md5_clone = md5.clone();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let checksum_config = read_multipart_checksum_config(
        &connection,
        upload_id,
        &bucket,
        &key,
        access_key.as_deref(),
    )
    .await?;
    let checksum = explicit_checksum.or_else(|| configured_part_checksum(checksum_config, &value));
    let checksum_write = checksum.clone();
    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;

            let upload_state =
                Sqlite::get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                    .ok_or(SqliteError::MultipartUploadNotFound)?;
            Sqlite::validate_multipart_access(
                upload_state.access_key.as_ref(),
                access_key.as_deref(),
            )?;
            if upload_state.checksum != checksum_config {
                return Err(SqliteError::ChecksumMismatch);
            }

            Sqlite::touch_multipart_upload(&transaction, upload_id)?;
            Sqlite::put_multipart(
                &transaction,
                Multipart {
                    upload_id,
                    part_number,
                    last_modified: OffsetDateTime::now_utc(),
                    value,
                    size,
                    md5: Some(md5_clone),
                    checksum: checksum_write,
                },
            )?;

            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(upload_part_output(md5, checksum.as_ref())))
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
    validate_no_sse_headers(&headers)?;
    let access_key = credentials.map(|credentials| credentials.access_key);
    let request = parse_upload_part_copy_request(input, access_key)?;
    sqlite
        .validate_mutable_bucket(&request.bucket)
        .map_err(s3_error_from_sqlite)?;
    let same_bucket = request.source_bucket == request.bucket;
    let copy_part_result = if same_bucket {
        upload_part_copy_same_bucket(sqlite, request).await?
    } else {
        upload_part_copy_cross_bucket(sqlite, request).await?
    };

    let output = UploadPartCopyOutput {
        copy_part_result: Some(copy_part_result),
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
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (upload_checksum, parts) = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;

            let upload_state = Sqlite::get_multipart_upload_state(
                &transaction,
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
                &transaction,
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
        key,
        upload_id,
        ..
    } = req.input;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error(S3ErrorCode::NoSuchUpload))?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let upload_state =
                Sqlite::get_multipart_upload_state(&transaction, upload_id, &bucket, &key)?
                    .ok_or(SqliteError::MultipartUploadNotFound)?;
            Sqlite::validate_multipart_access(
                upload_state.access_key.as_ref(),
                req.credentials
                    .as_ref()
                    .map(|credentials| credentials.access_key.as_str()),
            )?;

            let deleted = Sqlite::delete_multipart(&transaction, upload_id)?;
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
pub(super) async fn complete_multipart_upload(
    sqlite: &Sqlite,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    validate_no_sse_headers(&req.headers)?;
    let CompleteMultipartUploadInput {
        multipart_upload,
        bucket,
        key,
        upload_id,
        if_match,
        if_none_match,
        checksum_crc32: request_checksum_crc32,
        checksum_crc32c: request_checksum_crc32c,
        checksum_crc64nvme: request_checksum_crc64nvme,
        checksum_sha1: request_checksum_sha1,
        checksum_sha256: request_checksum_sha256,
        checksum_type,
        ..
    } = req.input;
    let write_conditions = object_write_conditions(if_match, if_none_match);

    sqlite
        .validate_mutable_bucket(&bucket)
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

    let requested_parts = normalize_completion_parts(requested_parts)?;
    let requested_checksum = ChecksumInput {
        checksum_type,
        crc32: request_checksum_crc32,
        crc32c: request_checksum_crc32c,
        crc64nvme: request_checksum_crc64nvme,
        sha1: request_checksum_sha1,
        sha256: request_checksum_sha256,
        ..ChecksumInput::default()
    }
    .into_completion_checksum()?;

    let request = MultipartCompletionRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id,
        requested_parts,
        requested_checksum,
        default_checksum: default_crc64nvme_full_object_checksum(),
        access_key: req.credentials.map(|credentials| credentials.access_key),
        write_conditions,
    };

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (request, completion) = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            let completion = Sqlite::prepare_multipart_upload_completion(&transaction, &request)?;
            Ok((request, completion))
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    let completed = match completion {
        MultipartCompletion::AlreadyCompleted(completed) => completed,
        MultipartCompletion::Prepared(prepared) => connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let completed =
                    Sqlite::put_prepared_multipart_upload(&transaction, &request, prepared)?;
                transaction.commit()?;
                Ok(completed)
            })
            .await
            .map_err(s3_error_from_sqlite)?,
    };
    debug!(version_id = %completed.version_id, "completed multipart upload");

    let completed_checksum_type = checksum_type_output(&completed.checksum);
    let output = CompleteMultipartUploadOutput {
        bucket: Some(bucket),
        key: Some(key),
        e_tag: Some(ETag::Strong(completed.md5)),
        version_id: completed.returned_version_id,
        checksum_crc32: checksum_crc32(&completed.checksum),
        checksum_crc32c: checksum_crc32c(&completed.checksum),
        checksum_crc64nvme: checksum_crc64nvme(&completed.checksum),
        checksum_sha1: checksum_sha1(&completed.checksum),
        checksum_sha256: checksum_sha256(&completed.checksum),
        checksum_type: completed_checksum_type,
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sqlite::{KeyMetadata, PartChecksum};

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
                cache_control: None,
                expires: None,
                last_modified: OffsetDateTime::UNIX_EPOCH,
                md5: Some("md5".to_string()),
                checksum: None,
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
            md5: Some("md5".to_string()),
            checksum: Some(PartChecksum::Sha256("sha".to_string())),
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
