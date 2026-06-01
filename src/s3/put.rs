//! S3 `PutObject` request handling.

use super::{
    CONTENT_MD5, ChecksumInput, ChecksumPolicy, ChecksumTypeKind, Digest, ETag, KeyMetadata,
    KeyValue, Md5, Not, OffsetDateTime, PutObjectInput, PutObjectOutput, S3Error, S3ErrorCode,
    S3Request, S3Response, S3Result, Sqlite, StorageClass, StoredChecksum, StreamingBlob,
    TryStreamExt, checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1,
    checksum_sha256, checksum_type_output, copy_bytes, debug, finish_payload_checksum, hex,
    normalize_content_encoding, object_write_conditions, payload_checksum_accumulator, s3_error,
    s3_error_from_sqlite, validate_content_md5,
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

/// Builds the S3 `PutObject` response from the stored object identity.
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
