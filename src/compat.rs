//! Protocol compatibility layers that run around the `s3s` service.
//!
//! `s3s` handles the normal S3 protocol surface. This module keeps the small
//! set of HTTP and client-compatibility shims that need to happen before `s3s`
//! parses a request or after it serializes a response, so operation handlers can
//! stay focused on storage behavior rather than wire-format edge cases.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use http_body_util::BodyExt;
use hyper::http::{HeaderValue, Method, Request, StatusCode, header};
use s3s::{Body, HttpError, HttpResponse};
use tower::{Layer, Service};

use crate::s3::decode_rfc2047_word;

/// Applies wire-level adjustments needed for the supported S3 response shapes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct S3CompatLayer;

impl<S> Layer<S> for S3CompatLayer {
    type Service = S3CompatService<S>;

    /// Wraps the S3 service once per connection.
    fn layer(&self, inner: S) -> Self::Service {
        S3CompatService { inner }
    }
}

/// Service wrapper that applies [`S3CompatLayer`] per request.
#[derive(Clone, Debug)]
pub(crate) struct S3CompatService<S> {
    inner: S,
}

impl<S, B> Service<Request<B>> for S3CompatService<S>
where
    S: Service<Request<B>, Response = HttpResponse, Error = HttpError> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = HttpResponse;
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Applies compatibility checks before dispatch and adjusts selected responses.
    fn call(&mut self, req: Request<B>) -> Self::Future {
        if let Some(response) = presigned_expiry_compat(&req) {
            return Box::pin(async move { Ok(response) });
        }

        let rewrite_post_policy_condition = is_post_object_form(&req);
        let future = self.inner.call(req);
        Box::pin(async move {
            let mut response = future.await?;
            response = rewrite_metadata_headers(response);
            if rewrite_post_policy_condition {
                response = rewrite_post_policy_condition_status(response).await?;
            }
            Ok(response)
        })
    }
}

/// Detects browser-form `POST Object` requests before `s3s` routes them.
fn is_post_object_form<B>(req: &Request<B>) -> bool {
    if req.method() != Method::POST {
        return false;
    }
    req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|media_type| media_type.eq_ignore_ascii_case("multipart/form-data"))
        })
}

/// Decodes RFC 2047 user metadata headers before SDK clients observe them.
fn rewrite_metadata_headers(mut response: HttpResponse) -> HttpResponse {
    let updates = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            name.as_str().starts_with("x-amz-meta-").then(|| {
                let value = value.to_str().ok()?;
                let decoded = decode_rfc2047_word(value)?;
                let decoded = HeaderValue::from_bytes(decoded.as_bytes()).ok()?;
                Some((name.clone(), decoded))
            })?
        })
        .collect::<Vec<_>>();

    for (name, value) in updates {
        response.headers_mut().insert(name, value);
    }
    response
}

/// Maps POST Object response statuses to the Ceph/AWS-compatible shape.
///
/// Malformed policy documents still return `400 InvalidPolicyDocument`; this
/// only rewrites parsed policies whose conditions reject the submitted form.
async fn rewrite_post_policy_condition_status(
    response: HttpResponse,
) -> Result<HttpResponse, HttpError> {
    if response.status() != StatusCode::BAD_REQUEST && response.status() != StatusCode::FORBIDDEN {
        return Ok(response);
    }

    let (mut parts, body) = response.into_parts();
    let bytes = body
        .collect()
        .await
        .map_err(HttpError::from_std_error)?
        .to_bytes();
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(HttpResponse::from_parts(parts, Body::from(bytes)));
    };
    if text.contains("<Code>InvalidPolicyDocument</Code>") && text.contains("Policy condition") {
        parts.status = StatusCode::FORBIDDEN;
    } else if parts.status == StatusCode::FORBIDDEN
        && text.contains("<Code>AccessDenied</Code>")
        && !text.contains("Request has expired")
        && !text.contains("Each form field")
    {
        parts.status = StatusCode::BAD_REQUEST;
    }
    Ok(HttpResponse::from_parts(parts, Body::from(bytes)))
}

/// Applies AWS-compatible presigned URL expiry rejection before auth parsing.
///
/// Presigned `SigV4` URLs must use `X-Amz-Expires` in the inclusive range
/// `1..=604800`. Rejecting invalid values here gives clients the same broad
/// status/code shape as AWS and prevents invalid preflight-like `OPTIONS`
/// requests from falling through as ordinary S3 operations.
fn presigned_expiry_compat<B>(req: &Request<B>) -> Option<HttpResponse> {
    let query = req.uri().query()?;
    let expires = query_value(query, "X-Amz-Expires")?;
    if req.method() == Method::OPTIONS {
        return Some(s3_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
        ));
    }

    let Ok(expires) = expires.parse::<i64>() else {
        return Some(s3_error_response(StatusCode::FORBIDDEN, "AccessDenied"));
    };

    if !(1..=604_800).contains(&expires) {
        return Some(s3_error_response(StatusCode::FORBIDDEN, "AccessDenied"));
    }

    None
}

/// Finds an exact query-pair value without percent-decoding.
///
/// The presigned expiry check only needs to identify the raw
/// `X-Amz-Expires=value` pair. Leaving the rest of query parsing to `s3s`
/// avoids changing signature inputs.
fn query_value<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then_some(value)
    })
}

/// Builds the minimal XML S3 error responses emitted by compatibility checks.
///
/// These responses are intentionally small because they only cover cases that
/// are rejected before the request reaches the normal `s3s` error machinery.
fn s3_error_response(status: StatusCode, code: &str) -> HttpResponse {
    let body =
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code></Error>");
    let mut response = HttpResponse::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::BodyExt;

    fn request(method: Method, uri: &str) -> Result<Request<()>, hyper::http::Error> {
        Request::builder().method(method).uri(uri).body(())
    }

    /// Collects a test response body as UTF-8.
    async fn response_body_text(response: HttpResponse) -> Result<String, HttpError> {
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(HttpError::from_std_error)?
            .to_bytes();
        String::from_utf8(bytes.to_vec()).map_err(|err| HttpError::from_std_error(Box::new(err)))
    }

    fn error_code(response: HttpResponse) -> Result<String, HttpError> {
        let status = response.status();
        let body = futures::executor::block_on(response_body_text(response))?;
        Ok(format!("{status} {body}"))
    }

    #[test]
    fn rewrites_rfc2047_user_metadata_headers() {
        let mut response = HttpResponse::new(Body::empty());
        response.headers_mut().insert(
            "x-amz-meta-meta1",
            HeaderValue::from_static("=?UTF-8?B?SGVsbG8gV29ybGTDqQ==?="),
        );

        let response = rewrite_metadata_headers(response);
        assert_eq!(
            response
                .headers()
                .get("x-amz-meta-meta1")
                .map(HeaderValue::as_bytes),
            Some("Hello World\u{e9}".as_bytes())
        );
    }

    /// Covers supported RFC 2047 Q words and unsupported metadata encodings.
    #[test]
    fn decodes_rfc2047_q_metadata_and_rejects_unsupported_words() {
        assert_eq!(
            decode_rfc2047_word("=?UTF-8?Q?Hello_World=C3=A9?=").as_deref(),
            Some("Hello World\u{e9}")
        );
        assert_eq!(
            decode_rfc2047_word("=?utf-8?q?=c3=a9?=").as_deref(),
            Some("\u{e9}")
        );
        assert_eq!(
            decode_rfc2047_word("=?us-ascii?Q?AZaz09=21?=").as_deref(),
            Some("AZaz09!")
        );
        assert_eq!(decode_rfc2047_word("=?iso-8859-1?Q?caf=E9?="), None);
        assert_eq!(decode_rfc2047_word("=?UTF-8?X?abc?="), None);
        assert_eq!(decode_rfc2047_word("=?UTF-8?Q?bad=GG?="), None);
        assert_eq!(decode_rfc2047_word("plain"), None);
    }

    /// Covers POST Object status compatibility rewrites for parsed policy errors.
    #[tokio::test]
    async fn rewrites_post_policy_condition_statuses() -> Result<(), HttpError> {
        let mut invalid_policy = HttpResponse::new(Body::from(
            "<Error><Code>InvalidPolicyDocument</Code><Message>Policy condition failed</Message></Error>"
                .to_string(),
        ));
        *invalid_policy.status_mut() = StatusCode::BAD_REQUEST;
        let invalid_policy = rewrite_post_policy_condition_status(invalid_policy).await?;
        assert_eq!(invalid_policy.status(), StatusCode::FORBIDDEN);

        let mut access_denied = HttpResponse::new(Body::from(
            "<Error><Code>AccessDenied</Code><Message>condition mismatch</Message></Error>"
                .to_string(),
        ));
        *access_denied.status_mut() = StatusCode::FORBIDDEN;
        let access_denied = rewrite_post_policy_condition_status(access_denied).await?;
        assert_eq!(access_denied.status(), StatusCode::BAD_REQUEST);

        let mut expired = HttpResponse::new(Body::from(
            "<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>"
                .to_string(),
        ));
        *expired.status_mut() = StatusCode::FORBIDDEN;
        let expired = rewrite_post_policy_condition_status(expired).await?;
        assert_eq!(expired.status(), StatusCode::FORBIDDEN);

        let mut field_missing = HttpResponse::new(Body::from(
            "<Error><Code>AccessDenied</Code><Message>Each form field must appear</Message></Error>"
                .to_string(),
        ));
        *field_missing.status_mut() = StatusCode::FORBIDDEN;
        let field_missing = rewrite_post_policy_condition_status(field_missing).await?;
        assert_eq!(field_missing.status(), StatusCode::FORBIDDEN);

        let mut non_utf8 = HttpResponse::new(Body::from(Bytes::from_static(&[0xff, b'a'])));
        *non_utf8.status_mut() = StatusCode::FORBIDDEN;
        let non_utf8 = rewrite_post_policy_condition_status(non_utf8).await?;
        assert_eq!(non_utf8.status(), StatusCode::FORBIDDEN);
        let bytes = non_utf8
            .into_body()
            .collect()
            .await
            .map_err(HttpError::from_std_error)?
            .to_bytes();
        assert_eq!(bytes.as_ref(), &[0xff, b'a']);

        Ok(())
    }

    #[test]
    fn presigned_expiry_validation_matches_compat_contract()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        assert!(presigned_expiry_compat(&request(Method::GET, "/bucket/key")?).is_none());
        assert!(
            presigned_expiry_compat(&request(Method::GET, "/bucket/key?X-Amz-Expires=1")?)
                .is_none()
        );
        assert!(
            presigned_expiry_compat(&request(Method::GET, "/bucket/key?X-Amz-Expires=604800")?)
                .is_none()
        );

        let options =
            presigned_expiry_compat(&request(Method::OPTIONS, "/bucket/key?X-Amz-Expires=60")?);
        let Some(response) = options else {
            panic!("OPTIONS presigned request should be rejected");
        };
        assert!(error_code(response)?.contains("400 Bad Request"));
        let Some(response) =
            presigned_expiry_compat(&request(Method::GET, "/bucket/key?X-Amz-Expires=bad")?)
        else {
            panic!("invalid expiry should be rejected");
        };
        assert!(error_code(response)?.contains("AccessDenied"));

        let Some(response) =
            presigned_expiry_compat(&request(Method::GET, "/bucket/key?X-Amz-Expires=0")?)
        else {
            panic!("zero expiry should be rejected");
        };
        assert!(error_code(response)?.contains("AccessDenied"));

        let Some(response) =
            presigned_expiry_compat(&request(Method::GET, "/bucket/key?X-Amz-Expires=604801")?)
        else {
            panic!("oversized expiry should be rejected");
        };
        assert!(error_code(response)?.contains("AccessDenied"));

        Ok(())
    }

    #[test]
    fn query_lookup_requires_exact_named_pair() {
        assert_eq!(
            query_value("prefix=1&X-Amz-Expires=60&other=2", "X-Amz-Expires"),
            Some("60")
        );
        assert_eq!(query_value("X-Amz-Expires", "X-Amz-Expires"), None);
        assert_eq!(query_value("Other-X-Amz-Expires=60", "X-Amz-Expires"), None);
    }

    #[tokio::test]
    async fn builds_xml_error_responses() -> Result<(), HttpError> {
        let response = s3_error_response(StatusCode::FORBIDDEN, "AccessDenied");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/xml"))
        );
        assert_eq!(
            response_body_text(response).await?,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>AccessDenied</Code></Error>"
        );
        Ok(())
    }
}
