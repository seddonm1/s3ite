//! S3 `PutObject` request handling.

use super::{
    CONTENT_MD5, ChecksumInput, ChecksumPolicy, ChecksumTypeKind, Digest, ETag, KeyMetadata,
    KeyValue, Md5, Not, ObjectAclRequest, OffsetDateTime, PutObjectInput, PutObjectOutput, S3Error,
    S3ErrorCode, S3Request, S3Response, S3Result, Sqlite, StorageClass, StoredChecksum,
    StreamingBlob, TryStreamExt, checksum_crc32, checksum_crc32c, checksum_crc64nvme,
    checksum_sha1, checksum_sha256, checksum_type_output, copy_bytes, finish_payload_checksum, hex,
    normalize_content_encoding, normalize_metadata, object_write_conditions,
    payload_checksum_accumulator, read_object_body_encrypted, requested_sse_s3,
    resolve_write_encryption, s3_error, s3_error_from_sqlite, sse_s3_output,
    stored_tags_from_header, validate_content_md5, validate_declared_object_write_length,
    validate_no_unsupported_sse_headers, validate_object_acl,
};

/// Fully buffered object body plus hashes needed by the storage write.
struct BufferedObjectBody {
    value: Vec<u8>,
    size: i64,
    md5: String,
    checksum: StoredChecksum,
}

/// Reads the request body while accumulating MD5 and the selected S3 checksum.
async fn read_put_object_body(
    body: StreamingBlob,
    content_md5_header_present: bool,
    content_md5: Option<&str>,
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

    validate_content_md5(content_md5_header_present, content_md5, md5_bytes.as_ref())?;

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

/// Stores an object body and metadata.
#[allow(clippy::too_many_lines)]
pub(super) async fn put_object(
    sqlite: &Sqlite,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let S3Request { input, headers, .. } = req;
    validate_no_unsupported_sse_headers(&headers)?;
    validate_put_object_bucket(sqlite, &input.bucket).await?;
    validate_put_object_acl_input(&input)?;

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
        bucket_key_enabled,
        checksum_algorithm,
        checksum_crc32,
        checksum_crc32c,
        checksum_crc64nvme,
        checksum_sha1,
        checksum_sha256,
        expires,
        if_match,
        if_none_match,
        server_side_encryption,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        ssekms_encryption_context,
        ssekms_key_id,
        storage_class,
        tagging,
        ..
    } = input;
    let explicit_sse_s3 = requested_sse_s3(
        server_side_encryption.as_ref(),
        ssekms_key_id.is_some() || ssekms_encryption_context.is_some(),
        sse_customer_algorithm.is_some()
            || sse_customer_key.is_some()
            || sse_customer_key_md5.is_some(),
        bucket_key_enabled.is_some(),
    )?;
    let write_conditions = object_write_conditions(if_match, if_none_match);
    let content_encoding = normalize_content_encoding(content_encoding);
    let metadata = normalize_metadata(metadata);
    let tags = stored_tags_from_header(tagging)?;
    validate_put_storage_class(storage_class.as_ref())?;
    validate_declared_object_write_length(&headers, content_length)?;

    let Some(body) = body else {
        return Err(s3_error(S3ErrorCode::IncompleteBody));
    };

    let chunk_size = sqlite.blob_chunk_size();
    let encryption = resolve_write_encryption(sqlite, &bucket, explicit_sse_s3).await?;
    if let Some(encryption) = encryption {
        let encrypted_body = read_object_body_encrypted(
            body,
            headers.contains_key(CONTENT_MD5),
            content_md5.as_deref(),
            ChecksumInput::write_request(
                checksum_algorithm,
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256,
            ),
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
        let tags = tags.clone();
        let connection = sqlite
            .get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let write_outcome = connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Sqlite::check_object_write_conditions(&transaction, &key, &write_conditions)?;
                let outcome = Sqlite::put_object_with_encoded_chunks(
                    &transaction,
                    &key,
                    KeyMetadata {
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
                        stored_sse: stored_encryption,
                        is_latest: true,
                        is_delete_marker: false,
                    },
                    chunks,
                )?;
                Sqlite::replace_object_tags(&transaction, outcome.sequence, &tags)?;
                transaction.commit()?;
                Ok(outcome)
            })
            .await
            .map_err(s3_error_from_sqlite)?;

        let output = put_object_output(md5, &checksum, write_outcome.returned_version_id, true);
        return Ok(S3Response::new(output));
    }

    let BufferedObjectBody {
        value,
        size,
        md5,
        checksum,
    } = read_put_object_body(
        body,
        headers.contains_key(CONTENT_MD5),
        content_md5.as_deref(),
        ChecksumInput::write_request(
            checksum_algorithm,
            checksum_crc32,
            checksum_crc32c,
            checksum_crc64nvme,
            checksum_sha1,
            checksum_sha256,
        ),
    )
    .await?;

    let md5_clone = md5.clone();
    let checksum_clone = checksum.clone();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let write_outcome = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            Sqlite::check_object_write_conditions(&transaction, &key, &write_conditions)?;
            let outcome = Sqlite::put_object_with_chunk_size(
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
                        stored_sse: None,
                        is_latest: true,
                        is_delete_marker: false,
                    },
                },
                chunk_size,
            )?;
            Sqlite::replace_object_tags(&transaction, outcome.sequence, &tags)?;
            transaction.commit()?;
            Ok(outcome)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let output = put_object_output(md5, &checksum, write_outcome.returned_version_id, false);
    Ok(S3Response::new(output))
}
