//! S3 `PutObject` request handling.

use std::{collections::HashMap, sync::Arc};

use hyper::{HeaderMap, Method};

use crate::database::BucketConnection;
use crate::sqlite::{
    ChecksumTypeKind, EncryptionMode, KeyMetadata, ObjectWriteConditions, StoredChecksum,
    StoredObjectLock, StoredTag,
};

use super::metadata::parse_expires;
use super::sse::ActiveSse;
use super::stage::{StagedBody, stage_streaming_body, with_client_idle_timeout};
use super::{
    CONTENT_MD5, ChecksumInput, ChecksumPolicy, Digest, ETag, LifecycleExpirationHeader, Md5, Not,
    ObjectAclRequest, OffsetDateTime, PutObjectInput, PutObjectOutput, S3ErrorCode, S3Request,
    S3Response, S3Result, Sqlite, StorageClass, StreamingBlob, WriteEncryption, checksum_crc32,
    checksum_crc32c, checksum_crc64nvme, checksum_input, checksum_md5, checksum_sha1,
    checksum_sha256, checksum_sha512, checksum_type_output, checksum_xxhash3, checksum_xxhash64,
    checksum_xxhash128, encryption_headers, finish_payload_checksum, hex,
    load_bucket_object_lock_config, normalize_content_encoding, normalize_metadata,
    object_lock_for_new_version, object_lock_write_headers, object_write_conditions,
    payload_checksum_accumulator, requested_encryption, resolve_write_encryption, s3_error,
    s3_error_from_sqlite, s3_error_with_message, stored_tags_from_header, validate_content_md5,
    validate_declared_object_write_length, validate_expected_bucket_owner,
    validate_no_unsupported_sse_headers, validate_object_acl,
};

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
    content_disposition: Option<String>,
    content_language: Option<String>,
    cache_control: Option<String>,
    expires: Option<OffsetDateTime>,
    write_conditions: ObjectWriteConditions,
    tags: Vec<StoredTag>,
    object_lock: StoredObjectLock,
    /// Body length the request declared; the staged body must match it exactly.
    declared_size: i64,
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

/// Extracts explicit encryption settings from the object write.
fn requested_put_object_encryption(input: &PutObjectInput) -> S3Result<Option<WriteEncryption>> {
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

/// Builds the S3 `PutObject` response from the stored object identity.
fn put_object_output(
    md5: String,
    checksum: &StoredChecksum,
    version_id: Option<String>,
    expiration: Option<LifecycleExpirationHeader>,
    encryption: Option<EncryptionMode>,
) -> S3Result<PutObjectOutput> {
    let checksum_type = checksum_type_output(checksum);
    let expiration = expiration
        .map(|expiration| expiration.to_header_value())
        .transpose()?;
    let (server_side_encryption, ssekms_key_id) = encryption_headers(encryption);
    Ok(PutObjectOutput {
        e_tag: Some(ETag::Strong(md5)),
        version_id,
        expiration,
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
        server_side_encryption,
        ssekms_key_id,
        ..Default::default()
    })
}

/// A `PutObject` body staged in storage together with its validated digests.
struct StagedObjectBody {
    staged: StagedBody,
    md5: String,
    checksum: StoredChecksum,
}

/// Streams a `PutObject` body into staged chunks and validates its MD5 and checksum.
///
/// Validation runs after the body ends because trailer checksums and the
/// digests are only known then. A mismatch discards the staged chunks.
async fn stage_put_object_body(
    connection: Arc<BucketConnection>,
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
    checksum_input: &ChecksumInput,
    chunk_size: usize,
    declared_size: i64,
    encryption: Option<&ActiveSse>,
) -> S3Result<StagedObjectBody> {
    let mut md5_hash = Md5::new();
    let mut checksum_accumulator =
        payload_checksum_accumulator(checksum_input, ChecksumPolicy::DefaultCrc64NvmeFullObject)?;
    let staged = stage_streaming_body(
        connection,
        with_client_idle_timeout(body),
        chunk_size,
        declared_size,
        1,
        encryption,
        |bytes| {
            md5_hash.update(bytes);
            checksum_accumulator.update(bytes);
        },
    )
    .await?;
    let md5_bytes = md5_hash.finalize();
    let validated = validate_content_md5(
        content_md5.header_present,
        content_md5.value,
        md5_bytes.as_ref(),
    )
    .and_then(|()| {
        finish_payload_checksum(
            checksum_input,
            checksum_accumulator,
            ChecksumTypeKind::FullObject,
        )
    });
    match validated {
        Ok(checksum) => Ok(StagedObjectBody {
            staged,
            md5: hex(md5_bytes),
            checksum,
        }),
        Err(err) => {
            staged.discard();
            Err(err)
        }
    }
}

/// Streams a `PutObject` body into staged chunks, validates it, then commits the version.
///
/// The body is staged before any write transaction, so a slow upload never
/// holds the bucket writer. Write conditions are checked in the same
/// transaction that claims the chunks, so the version is created only if the
/// conditions still hold at commit. A failed commit discards the staged body.
async fn put_streamed_object(
    sqlite: &Sqlite,
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
    checksum_input: ChecksumInput,
    encryption: Option<ActiveSse>,
    context: PutObjectWriteContext,
) -> S3Result<PutObjectOutput> {
    let connection = sqlite
        .get_connection(&context.bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let StagedObjectBody {
        staged,
        md5,
        checksum,
    } = stage_put_object_body(
        connection.clone(),
        body,
        content_md5,
        &checksum_input,
        sqlite.blob_chunk_size(),
        context.declared_size,
        encryption.as_ref(),
    )
    .await?;

    let size = staged.size;
    let staging_id = staged.staging_id();
    let md5_write = md5.clone();
    let checksum_write = checksum.clone();
    let stored_sse = encryption
        .as_ref()
        .map(|encryption| encryption.stored.clone());
    let PutObjectWriteContext {
        key,
        metadata,
        content_type,
        content_encoding,
        content_disposition,
        content_language,
        cache_control,
        expires,
        write_conditions,
        tags,
        object_lock,
        ..
    } = context;
    let written = connection
        .write(move |transaction| {
            Sqlite::check_object_write_conditions(transaction, &key, &write_conditions)?;
            let last_modified = OffsetDateTime::now_utc();
            let outcome = Sqlite::put_object_with_staged_chunks(
                transaction,
                &key,
                KeyMetadata {
                    version_id: String::new(),
                    size,
                    metadata,
                    content_type,
                    content_encoding,
                    content_disposition,
                    content_language,
                    cache_control,
                    expires,
                    last_modified,
                    md5: Some(md5_write),
                    checksum: Some(checksum_write),
                    stored_sse,
                    object_lock,
                    is_latest: true,
                    is_delete_marker: false,
                },
                staging_id,
            )?;
            Sqlite::replace_object_tags(transaction, outcome.sequence, &tags)?;
            let expiration = Sqlite::lifecycle_expiration_for_object(
                transaction,
                &key,
                &outcome.version_id,
                size,
                last_modified,
            )?
            .map(LifecycleExpirationHeader::from);
            Ok((outcome, expiration))
        })
        .await
        .map_err(s3_error_from_sqlite);
    let (write_outcome, expiration) = match written {
        Ok(written) => {
            staged.claimed();
            written
        }
        Err(err) => {
            staged.discard();
            return Err(err);
        }
    };

    put_object_output(
        md5,
        &checksum,
        write_outcome.returned_version_id,
        expiration,
        encryption.map(|encryption| encryption.stored.mode()),
    )
}

/// Resolves the write encryption for an object write and streams its body to storage.
async fn write_put_object(
    sqlite: &Sqlite,
    body: StreamingBlob,
    content_md5: ContentMd5Input<'_>,
    checksum_input: ChecksumInput,
    context: PutObjectWriteContext,
    explicit_encryption: Option<WriteEncryption>,
) -> S3Result<PutObjectOutput> {
    let encryption = resolve_write_encryption(
        sqlite,
        &context.bucket,
        &context.key,
        explicit_encryption.as_ref(),
    )
    .await?;
    put_streamed_object(
        sqlite,
        body,
        content_md5,
        checksum_input,
        encryption,
        context,
    )
    .await
}

/// Requires a client integrity header for a `PutObject` that applies Object Lock.
///
/// `Content-MD5` or any checksum that [`ChecksumInput::supplies_checksum`]
/// recognizes satisfies it. An unrecognized `x-amz-checksum-*` header does not.
fn validate_lock_integrity_headers(
    headers: &HeaderMap,
    checksum_input: &ChecksumInput,
    lock: &StoredObjectLock,
) -> S3Result<()> {
    // PutObject and Object Lock considerations require an integrity header
    // for retained writes. AWS's Object Lock compliance assessment explicitly
    // covers retention inherited from the bucket, as well as explicit retention.
    // https://d1.awsstatic.com/r2018/b/S3-Object-Lock/Amazon-S3-Compliance-Assessment.pdf
    // The AWS rejection message names "Content-MD5 OR x-amz-checksum-", so a
    // checksum value header satisfies it as well as the SDK algorithm.
    let has_checksum = headers.contains_key(CONTENT_MD5) || checksum_input.supplies_checksum();
    if (lock.legal_hold.is_some() || lock.retention.is_some()) && !has_checksum {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters",
        ));
    }
    Ok(())
}

/// Stores an object body and metadata.
pub(super) async fn put_object(
    sqlite: &Sqlite,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let S3Request {
        input,
        method,
        headers,
        trailing_headers,
        ..
    } = req;
    validate_expected_bucket_owner(input.expected_bucket_owner.as_deref())?;
    validate_no_unsupported_sse_headers(&headers)?;
    validate_put_object_bucket(sqlite, &input.bucket).await?;
    validate_put_object_acl_input(&input)?;
    let explicit_encryption = requested_put_object_encryption(&input)?;

    let checksum_input = checksum_input!(input, input.checksum_algorithm, None)
        .with_sdk_algorithm(&headers)?
        .with_trailer(&headers, trailing_headers)?;
    let PutObjectInput {
        body,
        bucket,
        key,
        metadata,
        cache_control,
        content_encoding,
        content_disposition,
        content_language,
        content_length,
        content_md5,
        content_type,
        expires,
        if_match,
        if_none_match,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
        ssekms_encryption_context,
        storage_class,
        tagging,
        ..
    } = input;
    let write_conditions =
        object_write_conditions(if_match, if_none_match, method == Method::POST)?;
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
    if method == Method::PUT {
        validate_lock_integrity_headers(&headers, &checksum_input, &object_lock)?;
    }
    validate_put_storage_class(storage_class.as_ref())?;
    let declared_size = validate_declared_object_write_length(&headers, content_length)?;

    let Some(body) = body else {
        return Err(s3_error(S3ErrorCode::IncompleteBody));
    };

    let content_md5 = ContentMd5Input {
        header_present: headers.contains_key(CONTENT_MD5),
        value: content_md5.as_deref(),
    };
    let context = PutObjectWriteContext {
        bucket,
        key,
        metadata,
        content_type,
        content_encoding,
        content_disposition,
        content_language,
        cache_control,
        expires: parse_expires(expires.as_deref())?,
        write_conditions,
        tags,
        object_lock,
        declared_size,
    };
    let mut output = write_put_object(
        sqlite,
        body,
        content_md5,
        checksum_input,
        context,
        explicit_encryption,
    )
    .await?;
    output.ssekms_encryption_context = ssekms_encryption_context;
    Ok(S3Response::new(output))
}
