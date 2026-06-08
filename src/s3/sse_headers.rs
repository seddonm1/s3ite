//! S3 SSE-S3 request header validation and write-encryption resolution.

use std::ops::Not;

use hyper::http::HeaderMap;
use s3s::{S3ErrorCode, S3Result, dto::ServerSideEncryption};

use crate::{
    sqlite::Sqlite,
    sse::{self, ActiveSse},
};

use super::{InternalError, s3_error_from_sqlite, s3_error_with_message};

const SSE_HEADER: &str = "x-amz-server-side-encryption";
const UNSUPPORTED_SSE_HEADERS: &[&str] = &[
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-copy-source-server-side-encryption-customer-algorithm",
    "x-amz-copy-source-server-side-encryption-customer-key",
    "x-amz-copy-source-server-side-encryption-customer-key-md5",
];

/// Rejects request-level SSE headers for operations where they do not apply.
pub(super) fn validate_no_sse_headers(headers: &HeaderMap) -> S3Result<()> {
    if headers.contains_key(SSE_HEADER) {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "server-side encryption headers are not valid for this operation",
        ));
    }
    validate_no_unsupported_sse_headers(headers)
}

/// Rejects SSE variants this backend does not implement.
pub(super) fn validate_no_unsupported_sse_headers(headers: &HeaderMap) -> S3Result<()> {
    if UNSUPPORTED_SSE_HEADERS
        .iter()
        .any(|header| headers.contains_key(*header))
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "only SSE-S3 with AES256 is supported",
        ));
    }
    Ok(())
}

/// Validates object-write SSE settings and returns whether AES256 was requested.
pub(super) fn requested_sse_s3(
    server_side_encryption: Option<&ServerSideEncryption>,
    has_kms_settings: bool,
    has_sse_customer_settings: bool,
    has_bucket_key_setting: bool,
) -> S3Result<bool> {
    if has_sse_customer_settings || has_kms_settings || has_bucket_key_setting {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "only SSE-S3 with AES256 is supported",
        ));
    }

    match server_side_encryption {
        Some(algorithm) if algorithm.as_str() == sse::SSE_S3_ALGORITHM => Ok(true),
        Some(_) => Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "server-side encryption must be AES256",
        )),
        None => Ok(false),
    }
}

/// Resolves explicit SSE-S3 and bucket default encryption for an object write.
pub(super) async fn resolve_write_encryption(
    sqlite: &Sqlite,
    bucket: &str,
    explicit_sse_s3: bool,
) -> S3Result<Option<ActiveSse>> {
    let use_sse_s3 = if explicit_sse_s3 {
        true
    } else {
        let connection = sqlite
            .get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .read(|connection| {
                let transaction = connection.transaction()?;
                Sqlite::get_bucket_default_sse_s3(&transaction)
            })
            .await
            .map_err(s3_error_from_sqlite)?
    };

    if use_sse_s3.not() {
        return Ok(None);
    }

    let master_key = sqlite.config.sse_s3_master_key().ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidRequest, "SSE-S3 is not configured")
    })?;
    sse::new_active_sse(&master_key)
        .map(Some)
        .map_err(|_| s3_error_with_message(InternalError, "SSE-S3 encryption failed"))
}

/// Returns the response enum for encrypted object outputs.
pub(super) fn sse_s3_output(encrypted: bool) -> Option<ServerSideEncryption> {
    encrypted.then(|| ServerSideEncryption::from_static(ServerSideEncryption::AES256))
}

/// Returns the configured SSE-S3 master key or a protocol error.
pub(super) fn require_sse_s3_master_key(sqlite: &Sqlite) -> S3Result<[u8; 32]> {
    sqlite.config.sse_s3_master_key().ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidRequest, "SSE-S3 is not configured")
    })
}

#[cfg(test)]
mod tests {
    use hyper::http::HeaderValue;
    use s3s::S3Error;

    use super::*;

    #[test]
    fn rejects_server_side_encryption_headers() {
        let mut headers = HeaderMap::new();
        assert!(validate_no_sse_headers(&headers).is_ok());

        headers.insert(SSE_HEADER, HeaderValue::from_static("AES256"));
        let error = validate_no_sse_headers(&headers).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
        assert!(
            format!("{error:?}").contains("not valid for this operation"),
            "error should explain operation-level SSE rejection"
        );

        for header in UNSUPPORTED_SSE_HEADERS {
            headers.clear();
            headers.insert(*header, HeaderValue::from_static("AES256"));
            let error = validate_no_sse_headers(&headers).err();
            assert!(matches!(
                error.as_ref().map(S3Error::code),
                Some(&S3ErrorCode::InvalidArgument)
            ));
            assert!(
                format!("{error:?}").contains("only SSE-S3 with AES256 is supported"),
                "error should explain unsupported SSE header {header}"
            );
        }
    }
}
