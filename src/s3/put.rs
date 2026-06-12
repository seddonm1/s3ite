//! S3 `PutObject` request handling.

use std::collections::HashMap;

use super::sse::ActiveSse;
use super::{
    CONTENT_MD5, ChecksumInput, ChecksumPolicy, ChecksumTypeKind, Digest, ETag, KeyMetadata,
    KeyValue, Md5, Not, ObjectAclRequest, ObjectWriteConditions, OffsetDateTime, PutObjectInput,
    PutObjectOutput, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, Sqlite, StorageClass,
    StoredChecksum, StoredObjectLock, StoredTag, StreamingBlob, TryStreamExt, checksum_crc32,
    checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256, checksum_type_output,
    copy_bytes, finish_payload_checksum, hex, load_bucket_object_lock_config,
    normalize_content_encoding, normalize_metadata, object_lock_for_new_version,
    object_lock_write_headers, object_write_conditions, payload_checksum_accumulator,
    read_object_body_encrypted, requested_sse_s3, resolve_write_encryption, s3_error,
    s3_error_from_sqlite, sse_s3_output, stored_tags_from_header, validate_content_md5,
    validate_declared_object_write_length, validate_expected_bucket_owner,
    validate_no_unsupported_sse_headers, validate_object_acl,
};

/// Fully buffered object body plus hashes needed by the storage write.
struct BufferedObjectBody {
    value: Vec<u8>,
    size: i64,
    md5: String,
    checksum: StoredChecksum,
}

#[derive(Clone, Copy)]
struct ContentMd5Input<'a> {
    header_present: bool,
    value: Option<&'a str>,
}

/// Normalized object write state shared by plain and encrypted `PutObject`.
struct PutObjectWriteContext {
    bucket: String,
    key: String,
    metadata: Option<HashMap<String, String>>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    cache_control: Option<String>,
    expires: Option<OffsetDateTime>,
    write_conditions: ObjectWriteConditions,
    tags: Vec<StoredTag>,
    object_lock: StoredObjectLock,
}

/// Reads the request body while accumulating MD5 and the selected S3 checksum.
async fn read_put_object_body(
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
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
        content_md5.header_present,
        content_md5.value,
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

/// Validates the limited storage classes supported by this backend.
fn validate_put_storage_class(storage_class: Option<&StorageClass>) -> S3Result<()> {
    if let Some(storage_class) = storage_class {
        let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
        if is_valid.not() {
            return Err(s3_error(S3ErrorCode::InvalidStorageClass));
        }
    }
    Ok(())
}

/// Ensures the destination bucket exists and can be mutated.
async fn validate_put_object_bucket(sqlite: &Sqlite, bucket: &str) -> S3Result<()> {
    sqlite
        .validate_mutable_bucket(bucket)
        .map_err(s3_error_from_sqlite)?;
    if sqlite.buckets.read().await.contains_key(bucket).not() {
        return Err(s3_error(S3ErrorCode::NoSuchBucket));
    }
    Ok(())
}

/// Validates that `PutObject` ACL headers are owner-equivalent no-ops.
fn validate_put_object_acl_input(input: &PutObjectInput) -> S3Result<()> {
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

/// Returns whether the request explicitly asks for SSE-S3 encryption.
fn requested_put_object_sse_s3(input: &PutObjectInput) -> S3Result<bool> {
    requested_sse_s3(
        input.server_side_encryption.as_ref(),
        input.ssekms_key_id.is_some() || input.ssekms_encryption_context.is_some(),
        input.sse_customer_algorithm.is_some()
            || input.sse_customer_key.is_some()
            || input.sse_customer_key_md5.is_some(),
        input.bucket_key_enabled.is_some(),
    )
}

/// Builds the S3 `PutObject` response from the stored object identity.
fn put_object_output(
    md5: String,
    checksum: &StoredChecksum,
    version_id: Option<String>,
    encrypted: bool,
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
        server_side_encryption: sse_s3_output(encrypted),
        ..Default::default()
    }
}

/// Reads, encrypts, and writes a `PutObject` body as encoded encrypted chunks.
async fn put_encrypted_object(
    sqlite: &Sqlite,
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
    checksum_input: ChecksumInput,
    chunk_size: usize,
    encryption: ActiveSse,
    context: PutObjectWriteContext,
) -> S3Result<PutObjectOutput> {
    let encrypted_body = read_object_body_encrypted(
        body,
        content_md5.header_present,
        content_md5.value,
        checksum_input,
        chunk_size,
        &encryption,
    )
    .await?;

    let size = encrypted_body.size;
    let md5 = encrypted_body.md5;
    let checksum = encrypted_body.checksum;
    let md5_clone = md5.clone();
    let checksum_clone = checksum.clone();
    let stored_encryption = Some(encryption.stored.clone());
    let chunks = encrypted_body.chunks;
    let PutObjectWriteContext {
        bucket,
        key,
        metadata,
        content_type,
        content_encoding,
        cache_control,
        expires,
        write_conditions,
        tags,
        object_lock,
    } = context;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let write_outcome = connection
        .write(move |transaction| {
            Sqlite::check_object_write_conditions(transaction, &key, &write_conditions)?;
            let outcome = Sqlite::put_object_with_encoded_chunks(
                transaction,
                &key,
                KeyMetadata {
                    version_id: String::new(),
                    size,
                    metadata,
                    content_type,
                    content_encoding,
                    cache_control,
                    expires,
                    last_modified: OffsetDateTime::now_utc(),
                    md5: Some(md5_clone),
                    checksum: Some(checksum_clone),
                    stored_sse: stored_encryption,
                    object_lock,
                    is_latest: true,
                    is_delete_marker: false,
                },
                chunks,
            )?;
            Sqlite::replace_object_tags(transaction, outcome.sequence, &tags)?;
            Ok(outcome)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(put_object_output(
        md5,
        &checksum,
        write_outcome.returned_version_id,
        true,
    ))
}

/// Reads and writes a plain buffered `PutObject` body.
async fn put_plain_object(
    sqlite: &Sqlite,
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
    checksum_input: ChecksumInput,
    chunk_size: usize,
    context: PutObjectWriteContext,
) -> S3Result<PutObjectOutput> {
    let BufferedObjectBody {
        value,
        size,
        md5,
        checksum,
    } = read_put_object_body(body, content_md5, checksum_input).await?;

    let md5_clone = md5.clone();
    let checksum_clone = checksum.clone();
    let PutObjectWriteContext {
        bucket,
        key,
        metadata,
        content_type,
        content_encoding,
        cache_control,
        expires,
        write_conditions,
        tags,
        object_lock,
    } = context;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let write_outcome = connection
        .write(move |transaction| {
            Sqlite::check_object_write_conditions(transaction, &key, &write_conditions)?;
            let outcome = Sqlite::put_object_with_chunk_size(
                transaction,
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
                        expires,
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5_clone),
                        checksum: Some(checksum_clone),
                        stored_sse: None,
                        object_lock,
                        is_latest: true,
                        is_delete_marker: false,
                    },
                },
                chunk_size,
            )?;
            Sqlite::replace_object_tags(transaction, outcome.sequence, &tags)?;
            Ok(outcome)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(put_object_output(
        md5,
        &checksum,
        write_outcome.returned_version_id,
        false,
    ))
}

/// Dispatches a normalized object write to the encrypted or plain storage path.
async fn write_put_object(
    sqlite: &Sqlite,
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
    checksum_input: ChecksumInput,
    context: PutObjectWriteContext,
    explicit_sse_s3: bool,
) -> S3Result<PutObjectOutput> {
    let chunk_size = sqlite.blob_chunk_size();
    let encryption = resolve_write_encryption(sqlite, &context.bucket, explicit_sse_s3).await?;
    if let Some(encryption) = encryption {
        put_encrypted_object(
            sqlite,
            body,
            content_md5,
            checksum_input,
            chunk_size,
            encryption,
            context,
        )
        .await
    } else {
        put_plain_object(
            sqlite,
            body,
            content_md5,
            checksum_input,
            chunk_size,
            context,
        )
        .await
    }
}

/// Stores an object body and metadata.
pub(super) async fn put_object(
    sqlite: &Sqlite,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let S3Request { input, headers, .. } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_no_unsupported_sse_headers(&headers)?;
    validate_put_object_bucket(sqlite, &input.bucket).await?;
    validate_put_object_acl_input(&input)?;
    let explicit_sse_s3 = requested_put_object_sse_s3(&input)?;

    let PutObjectInput {
        body,
        bucket,
        key,
        metadata,
        cache_control,
        content_encoding,
        content_length,
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
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
        storage_class,
        tagging,
        ..
    } = input;
    let write_conditions = object_write_conditions(if_match, if_none_match);
    let content_encoding = normalize_content_encoding(content_encoding);
    let metadata = normalize_metadata(metadata);
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
    validate_put_storage_class(storage_class.as_ref())?;
    validate_declared_object_write_length(&headers, content_length)?;

    let Some(body) = body else {
        return Err(s3_error(S3ErrorCode::IncompleteBody));
    };

    let content_md5 = ContentMd5Input {
        header_present: headers.contains_key(CONTENT_MD5),
        value: content_md5.as_deref(),
    };
    let checksum_input = ChecksumInput::write_request(
        checksum_algorithm,
        checksum_crc32,
        checksum_crc32c,
        checksum_crc64nvme,
        checksum_sha1,
        checksum_sha256,
    );
    let context = PutObjectWriteContext {
        bucket,
        key,
        metadata,
        content_type,
        content_encoding,
        cache_control,
        expires: expires.map(Into::into),
        write_conditions,
        tags,
        object_lock,
    };
    let output = write_put_object(
        sqlite,
        body,
        content_md5,
        checksum_input,
        context,
        explicit_sse_s3,
    )
    .await?;
    Ok(S3Response::new(output))
}
