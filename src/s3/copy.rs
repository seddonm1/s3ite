use std::collections::HashMap;

use super::{
    ChecksumAlgorithmKind, CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource, ETag,
    KeyValue, MetadataDirective, Not, ObjectReadConditions, OffsetDateTime, S3ErrorCode, S3Request,
    S3Response, S3Result, Sqlite, SqliteResult, StoredChecksum, checksum_algorithm_from_request,
    checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_sha1, checksum_sha256,
    checksum_type_output, normalize_content_encoding, object_write_checksum_from_value, s3_error,
    s3_error_from_sqlite, s3_error_with_message, validate_copy_source_conditions,
};

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
    Sqlite::try_get_copy_source_object(transaction, key, version_id)
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
