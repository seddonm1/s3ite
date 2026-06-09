use super::{
    CONTENT_TYPE, ETAG, ETag, InternalError, S3Error, S3ErrorCode, S3Response, S3Result,
    SqliteError, Uuid, X_AMZ_DELETE_MARKER, X_AMZ_REQUEST_ID,
};

/// Maps storage-layer failures into S3 protocol errors at the service boundary.
pub(super) fn s3_error_from_sqlite(error: SqliteError) -> S3Error {
    with_request_id(match error {
        SqliteError::BucketNotFound => S3Error::new(S3ErrorCode::NoSuchBucket),
        SqliteError::BucketNotEmpty => S3Error::new(S3ErrorCode::BucketNotEmpty),
        SqliteError::BucketReadOnly => S3Error::with_message(
            S3ErrorCode::MethodNotAllowed,
            "database is in read-only mode",
        ),
        SqliteError::ObjectNotFound => S3Error::new(S3ErrorCode::NoSuchKey),
        SqliteError::ObjectVersionNotFound => S3Error::new(S3ErrorCode::NoSuchVersion),
        SqliteError::InvalidObjectRange => S3Error::new(S3ErrorCode::InvalidRange),
        SqliteError::ObjectBodyInconsistent => S3Error::with_message(
            InternalError,
            "object body is inconsistent with object metadata",
        ),
        SqliteError::ObjectBodyReadTimeout => {
            S3Error::with_message(InternalError, "object body read timed out")
        }
        SqliteError::PreconditionFailed => S3Error::new(S3ErrorCode::PreconditionFailed),
        SqliteError::ChecksumMismatch => S3Error::new(S3ErrorCode::BadDigest),
        SqliteError::MultipartUploadNotFound => S3Error::new(S3ErrorCode::NoSuchUpload),
        SqliteError::MultipartAccessDenied => S3Error::new(S3ErrorCode::AccessDenied),
        SqliteError::InvalidMultipartPart => S3Error::new(S3ErrorCode::InvalidPart),
        SqliteError::MultipartPartTooSmall => S3Error::new(S3ErrorCode::EntityTooSmall),
        SqliteError::SingleRequestObjectTooLarge => S3Error::new(S3ErrorCode::EntityTooLarge),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_request_id(error: &S3Error) {
        assert!(
            error
                .headers()
                .is_some_and(|headers| headers.get(X_AMZ_REQUEST_ID).is_some()),
            "S3 error should include request id header"
        );
    }

    fn assert_maps(error: SqliteError, code: &S3ErrorCode) {
        let error = s3_error_from_sqlite(error);
        assert_eq!(error.code(), code);
        assert_request_id(&error);
    }

    #[test]
    fn bucket_read_only_maps_to_method_not_allowed_message() {
        let error = s3_error_from_sqlite(SqliteError::BucketReadOnly);

        assert_eq!(error.code(), &S3ErrorCode::MethodNotAllowed);
        assert!(
            format!("{error:?}").contains("database is in read-only mode"),
            "read-only error should preserve the user-facing message"
        );
        assert_request_id(&error);
    }

    #[test]
    fn maps_storage_errors_to_s3_error_codes() {
        assert_maps(SqliteError::BucketNotFound, &S3ErrorCode::NoSuchBucket);
        assert_maps(SqliteError::BucketNotEmpty, &S3ErrorCode::BucketNotEmpty);
        assert_maps(SqliteError::ObjectNotFound, &S3ErrorCode::NoSuchKey);
        assert_maps(
            SqliteError::ObjectVersionNotFound,
            &S3ErrorCode::NoSuchVersion,
        );
        assert_maps(SqliteError::InvalidObjectRange, &S3ErrorCode::InvalidRange);
        assert_maps(
            SqliteError::PreconditionFailed,
            &S3ErrorCode::PreconditionFailed,
        );
        assert_maps(SqliteError::ChecksumMismatch, &S3ErrorCode::BadDigest);
        assert_maps(
            SqliteError::MultipartUploadNotFound,
            &S3ErrorCode::NoSuchUpload,
        );
        assert_maps(
            SqliteError::MultipartAccessDenied,
            &S3ErrorCode::AccessDenied,
        );
        assert_maps(SqliteError::InvalidMultipartPart, &S3ErrorCode::InvalidPart);
        assert_maps(
            SqliteError::MultipartPartTooSmall,
            &S3ErrorCode::EntityTooSmall,
        );
        assert_maps(
            SqliteError::SingleRequestObjectTooLarge,
            &S3ErrorCode::EntityTooLarge,
        );
    }

    #[test]
    fn maps_internal_storage_errors_with_messages() {
        for error in [
            SqliteError::ObjectBodyInconsistent,
            SqliteError::ObjectBodyReadTimeout,
            SqliteError::MultipartObjectTooLarge,
            SqliteError::Configuration("bad config".to_string()),
            SqliteError::DatabaseQueueFull,
            SqliteError::DatabaseQueueClosed,
            SqliteError::UnsupportedSchemaVersion {
                found: 1,
                expected: 5,
            },
            SqliteError::Random("random failed".to_string()),
            SqliteError::ThreadJoin,
            SqliteError::Rusqlite(rusqlite::Error::InvalidQuery),
            SqliteError::Io(std::io::Error::other("io failed")),
        ] {
            let error = s3_error_from_sqlite(error);
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
            assert_request_id(&error);
        }
    }

    #[test]
    fn helper_errors_include_request_ids() {
        let error = s3_error(S3ErrorCode::NoSuchBucket);
        assert_eq!(error.code(), &S3ErrorCode::NoSuchBucket);
        assert_request_id(&error);

        let error = s3_error_with_message(S3ErrorCode::InvalidRequest, "bad request");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_request_id(&error);
        assert!(
            format!("{error:?}").contains("bad request"),
            "custom message should be preserved"
        );
    }

    #[test]
    fn delete_marker_not_found_errors_include_delete_marker_header() {
        for (is_delete_marker, expected) in [(true, "true"), (false, "false")] {
            let error = not_found_delete_marker_error(is_delete_marker);
            assert_eq!(error.code(), &S3ErrorCode::NoSuchKey);
            assert_request_id(&error);
            assert_eq!(
                error.headers().and_then(|headers| headers
                    .get(X_AMZ_DELETE_MARKER)
                    .and_then(|value| value.to_str().ok())),
                Some(expected)
            );
        }
    }

    #[test]
    fn not_modified_without_md5_keeps_request_id_only() -> S3Result<()> {
        let error = not_modified_error(None)?;
        assert_eq!(error.code(), &S3ErrorCode::NotModified);
        assert_request_id(&error);
        assert!(error.headers().is_some_and(|headers| {
            headers.get(ETAG).is_none() && headers.get(CONTENT_TYPE).is_none()
        }));
        Ok(())
    }

    #[test]
    fn not_modified_with_md5_includes_object_headers() -> S3Result<()> {
        let error = not_modified_error(Some("abc"))?;
        assert_eq!(error.code(), &S3ErrorCode::NotModified);
        assert_request_id(&error);
        assert_eq!(
            error
                .headers()
                .and_then(|headers| headers.get(ETAG))
                .and_then(|value| value.to_str().ok()),
            Some("\"abc\"")
        );
        assert_eq!(
            error.headers().and_then(|headers| headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())),
            Some("application/xml")
        );
        Ok(())
    }

    #[test]
    fn not_modified_rejects_invalid_etag_header_values() {
        let err = not_modified_error(Some("bad\netag")).err();
        assert!(err.is_some_and(|err| err.code() == &InternalError));
    }
}
