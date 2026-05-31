use super::{
    CONTENT_TYPE, ETAG, ETag, InternalError, S3Error, S3ErrorCode, S3Response, S3Result,
    SqliteError, Uuid, X_AMZ_DELETE_MARKER, X_AMZ_REQUEST_ID,
};

/// Maps storage-layer failures into S3 protocol errors at the service boundary.
pub(super) fn s3_error_from_sqlite(error: SqliteError) -> S3Error {
    with_request_id(match error {
        SqliteError::BucketNotFound => S3Error::new(S3ErrorCode::NoSuchBucket),
        SqliteError::BucketReadOnly => S3Error::with_message(
            S3ErrorCode::MethodNotAllowed,
            "database is in read-only mode",
        ),
        SqliteError::ObjectNotFound => S3Error::new(S3ErrorCode::NoSuchKey),
        SqliteError::ObjectVersionNotFound => S3Error::new(S3ErrorCode::NoSuchVersion),
        SqliteError::InvalidObjectRange => S3Error::new(S3ErrorCode::InvalidRange),
        SqliteError::PreconditionFailed => S3Error::new(S3ErrorCode::PreconditionFailed),
        SqliteError::ChecksumMismatch => S3Error::new(S3ErrorCode::BadDigest),
        SqliteError::MultipartUploadNotFound => S3Error::new(S3ErrorCode::NoSuchUpload),
        SqliteError::MultipartAccessDenied => S3Error::new(S3ErrorCode::AccessDenied),
        SqliteError::InvalidMultipartPart => S3Error::new(S3ErrorCode::InvalidPart),
        SqliteError::MultipartPartTooSmall => S3Error::new(S3ErrorCode::EntityTooSmall),
        SqliteError::MultipartObjectTooLarge => {
            S3Error::with_message(InternalError, "multipart object is too large")
        }
        SqliteError::Configuration(message) => S3Error::with_message(InternalError, message),
        error => S3Error::internal_error(error),
    })
}

/// Builds an S3 protocol error with no custom message.
pub(super) fn s3_error(code: S3ErrorCode) -> S3Error {
    with_request_id(S3Error::new(code))
}

/// Builds an S3 protocol error with a custom message.
pub(super) fn s3_error_with_message(code: S3ErrorCode, message: &'static str) -> S3Error {
    with_request_id(S3Error::with_message(code, message))
}

/// Adds a request ID to an error body and response headers.
fn with_request_id(mut error: S3Error) -> S3Error {
    let request_id = Uuid::new_v4().simple().to_string();
    error.set_request_id(request_id.clone());

    let mut headers = error
        .headers()
        .cloned()
        .unwrap_or_else(|| S3Response::new(()).headers);
    if let Ok(request_id) = request_id.parse() {
        headers.insert(X_AMZ_REQUEST_ID, request_id);
    }
    error.set_headers(headers);
    error
}

/// Builds a missing-object error with the S3 delete-marker header.
pub(super) fn not_found_delete_marker_error(is_delete_marker: bool) -> S3Error {
    let mut error = s3_error(S3ErrorCode::NoSuchKey);
    let mut headers = error
        .headers()
        .cloned()
        .unwrap_or_else(|| S3Response::new(()).headers);
    let delete_marker = if is_delete_marker { "true" } else { "false" };
    if let Ok(value) = delete_marker.parse() {
        headers.insert(X_AMZ_DELETE_MARKER, value);
    }
    error.set_headers(headers);
    error
}

/// Builds a 304 response error with object headers required by S3 clients.
pub(super) fn not_modified_error(md5: Option<&str>) -> S3Result<S3Error> {
    let mut error = s3_error(S3ErrorCode::NotModified);
    let Some(md5) = md5 else {
        return Ok(error);
    };

    let mut headers = error
        .headers()
        .cloned()
        .unwrap_or_else(|| S3Response::new(()).headers);
    headers.insert(
        ETAG,
        ETag::Strong(md5.to_string())
            .to_http_header()
            .map_err(|err| {
                S3Error::with_message(
                    InternalError,
                    format!("invalid not-modified ETag header: {err}"),
                )
            })?,
    );
    headers.insert(
        CONTENT_TYPE,
        "application/xml".parse().map_err(|err| {
            S3Error::with_message(
                InternalError,
                format!("invalid not-modified content type: {err}"),
            )
        })?,
    );
    error.set_headers(headers);
    Ok(error)
}
