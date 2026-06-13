//! Protocol compatibility layers that run around the `s3s` service.
//!
//! `s3s` handles the normal S3 protocol surface. This module keeps the small
//! set of HTTP and client-compatibility shims that need to happen before `s3s`
//! parses a request or after it serializes a response, so operation handlers can
//! stay focused on storage behavior rather than wire-format edge cases.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use http_body_util::BodyExt;
use hyper::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use s3s::{
    Body, HttpError, HttpResponse,
    crypto::{Checksum, Sha1},
    header::CONTENT_LENGTH,
};
use tower::{Layer, Service};

/// Handles authentication and response-shape compatibility around `s3s`.
///
/// This layer keeps client quirks that are independent of storage operations in
/// one place: presigned expiry bounds, `SigV2` header edge cases, the
/// `CreateBucket` `SigV2` trailing-slash canonical-resource mismatch, user
/// metadata header decoding, and small serialized response-shape fixes. `s3s`
/// remains the primary request router and signer; this layer only adjusts cases
/// where supported clients or conformance tests expect AWS-compatible behavior
/// before or after that path.
#[derive(Clone, Debug)]
pub(crate) struct HeaderAuthCompatLayer {
    secret_key: Option<Arc<str>>,
}

impl HeaderAuthCompatLayer {
    /// Creates a compatibility layer with the configured owner secret key.
    ///
    /// The secret is only used for the `SigV2` `CreateBucket` rewrite. When
    /// auth is disabled, malformed `SigV2` headers and presigned expiry checks
    /// still use their compatibility responses, but signatures are not
    /// rewritten.
    pub(crate) fn new(secret_key: Option<String>) -> Self {
        Self {
            secret_key: secret_key.map(Arc::from),
        }
    }
}

impl<S> Layer<S> for HeaderAuthCompatLayer {
    type Service = HeaderAuthCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HeaderAuthCompatService {
            inner,
            secret_key: self.secret_key.clone(),
        }
    }
}

/// Service wrapper that applies [`HeaderAuthCompatLayer`] per request.
#[derive(Clone, Debug)]
pub(crate) struct HeaderAuthCompatService<S> {
    inner: S,
    secret_key: Option<Arc<str>>,
}

impl<S, B> Service<Request<B>> for HeaderAuthCompatService<S>
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

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        if let Some(response) = presigned_expiry_compat(&req) {
            return Box::pin(async move { Ok(response) });
        }

        let rewrite_object_attributes = is_get_object_attributes(&req);
        let rewrite_post_policy_condition = is_post_object_form(&req);
        let rewrite_object_lock_invalid_retention = is_put_object_lock_configuration(&req);
        if let Some(response) = sig_v2_header_compat(&mut req, self.secret_key.as_deref()) {
            return Box::pin(async move { Ok(response) });
        }

        let future = self.inner.call(req);
        Box::pin(async move {
            let mut response = future.await?;
            response = rewrite_metadata_headers(response);
            if rewrite_post_policy_condition {
                response = rewrite_post_policy_condition_status(response).await?;
            }
            if rewrite_object_lock_invalid_retention {
                response = rewrite_invalid_retention_period_code(response).await?;
            }
            if rewrite_object_attributes {
                rewrite_get_object_attributes_etag(response).await
            } else {
                Ok(response)
            }
        })
    }
}

/// Detects `GetObjectAttributes` before `s3s` routes the request.
///
/// The response rewrite needs to run after `s3s` serializes the operation, but
/// the operation signal lives in request headers rather than in the final HTTP
/// response.
fn is_get_object_attributes<B>(req: &Request<B>) -> bool {
    req.method() == Method::GET && req.headers().contains_key("x-amz-object-attributes")
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

/// Detects `PutObjectLockConfiguration` requests before `s3s` routes them.
fn is_put_object_lock_configuration<B>(req: &Request<B>) -> bool {
    req.method() == Method::PUT
        && req.uri().query().is_some_and(|query| {
            query.split('&').any(|pair| {
                pair == "object-lock"
                    || pair
                        .split_once('=')
                        .is_some_and(|(name, _)| name == "object-lock")
            })
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
                let decoded = decode_rfc2047_header_value(value)?;
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

/// Decodes one supported RFC 2047 header word.
fn decode_rfc2047_header_value(value: &str) -> Option<String> {
    let encoded = value.strip_prefix("=?")?.strip_suffix("?=")?;
    let mut parts = encoded.splitn(3, '?');
    let charset = parts.next()?;
    let encoding = parts.next()?;
    let payload = parts.next()?;
    if !charset.eq_ignore_ascii_case("utf-8") && !charset.eq_ignore_ascii_case("us-ascii") {
        return None;
    }

    let bytes = match encoding {
        encoding if encoding.eq_ignore_ascii_case("b") => base64_decode(payload)?,
        encoding if encoding.eq_ignore_ascii_case("q") => decode_rfc2047_q(payload)?,
        _ => return None,
    };
    String::from_utf8(bytes).ok()
}

/// Decodes the RFC 2047 quoted-printable subset used by S3 SDK metadata.
fn decode_rfc2047_q(payload: &str) -> Option<Vec<u8>> {
    let mut bytes = Vec::with_capacity(payload.len());
    let mut iter = payload.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'_' => bytes.push(b' '),
            b'=' => {
                let high = hex_digit(iter.next()?)?;
                let low = hex_digit(iter.next()?)?;
                bytes.push((high << 4) | low);
            }
            byte => bytes.push(byte),
        }
    }
    Some(bytes)
}

/// Converts one ASCII hexadecimal digit into its numeric value.
fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Rewrites the serialized Object Lock default-retention error code.
async fn rewrite_invalid_retention_period_code(
    response: HttpResponse,
) -> Result<HttpResponse, HttpError> {
    rewrite_xml_error_code(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "InvalidRetentionPeriod",
        "InvalidRetentionPeriod",
    )
    .await
}

/// Rewrites a targeted serialized S3 XML error code while preserving the body otherwise.
async fn rewrite_xml_error_code(
    response: HttpResponse,
    status: StatusCode,
    from: &str,
    to: &str,
    marker: &str,
) -> Result<HttpResponse, HttpError> {
    if response.status() != status {
        return Ok(response);
    }

    let (mut parts, body) = response.into_parts();
    let bytes = body.collect().await.map_err(HttpError::new)?.to_bytes();
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(HttpResponse::from_parts(parts, Body::from(bytes)));
    };
    if !text.contains(marker) {
        return Ok(HttpResponse::from_parts(parts, Body::from(bytes)));
    }

    let text = text.replace(
        &format!("<Code>{from}</Code>"),
        &format!("<Code>{to}</Code>"),
    );
    let content_length = text
        .len()
        .to_string()
        .parse()
        .map_err(|err| HttpError::new(Box::new(err)))?;
    parts.headers.insert(CONTENT_LENGTH, content_length);
    Ok(HttpResponse::from_parts(parts, Body::from(text)))
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
    let bytes = body.collect().await.map_err(HttpError::new)?.to_bytes();
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

/// Removes XML quotes around `GetObjectAttributes` `ETag` values.
///
/// S3 object `ETags` are quoted in ordinary object APIs, but AWS serializes the
/// `GetObjectAttributes` XML field without those surrounding quote characters.
/// The model-level value is shared, so this small post-serialization rewrite
/// keeps the wire response compatible without special-casing storage output.
async fn rewrite_get_object_attributes_etag(
    response: HttpResponse,
) -> Result<HttpResponse, HttpError> {
    if !response.status().is_success() {
        return Ok(response);
    }

    let (mut parts, body) = response.into_parts();
    let bytes = body.collect().await.map_err(HttpError::new)?.to_bytes();
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(HttpResponse::from_parts(parts, Body::from(bytes)));
    };
    let text = text
        .replace("<ETag>\"", "<ETag>")
        .replace("\"</ETag>", "</ETag>");
    let content_length = text
        .len()
        .to_string()
        .parse()
        .map_err(|err| HttpError::new(Box::new(err)))?;
    parts.headers.insert(CONTENT_LENGTH, content_length);
    Ok(HttpResponse::from_parts(parts, Body::from(text)))
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

#[derive(Debug)]
struct AuthorizationV2 {
    access_key: String,
    signature: String,
}

/// Applies `SigV2` header compatibility before `s3s` validates the signature.
///
/// `s3s` handles supported `SigV2` requests, but this shim supplies AWS-like
/// errors for malformed Authorization headers and bad `x-amz-date` values. It
/// also rewrites one `CreateBucket` form that some clients sign as `/bucket/`
/// while sending `/bucket`, allowing `s3s` to verify the request it receives.
fn sig_v2_header_compat<B>(req: &mut Request<B>, secret_key: Option<&str>) -> Option<HttpResponse> {
    let auth = req
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .to_string();
    if !auth.starts_with("AWS ") {
        return None;
    }

    let Some(auth) = parse_authorization_v2(&auth) else {
        return Some(s3_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
        ));
    };

    if let Some(response) = validate_sig_v2_date(req.headers()) {
        return Some(response);
    }

    let secret_key = secret_key?;
    rewrite_create_bucket_sig_v2(req, secret_key, &auth);
    None
}

/// Parses the legacy `AWS access-key:signature` Authorization header.
fn parse_authorization_v2(value: &str) -> Option<AuthorizationV2> {
    let value = value.strip_prefix("AWS ")?;
    let (access_key, signature) = value.split_once(':')?;
    if access_key.is_empty() || signature.is_empty() {
        return None;
    }
    Some(AuthorizationV2 {
        access_key: access_key.to_string(),
        signature: signature.to_string(),
    })
}

/// Rejects `SigV2` dates that AWS treats as invalid or too skewed.
///
/// Missing dates are left to downstream signature handling. Present but empty,
/// malformed, pre-Unix-epoch, or more-than-15-minute-skewed dates are rejected
/// here to match the expected public error surface.
fn validate_sig_v2_date(headers: &HeaderMap) -> Option<HttpResponse> {
    let date = headers.get("x-amz-date")?;
    let date = date.to_str().ok().unwrap_or_default();
    if date.is_empty() {
        return Some(s3_error_response(StatusCode::FORBIDDEN, "AccessDenied"));
    }

    let Ok(date) =
        time::OffsetDateTime::parse(date, &time::format_description::well_known::Rfc2822)
    else {
        return Some(s3_error_response(StatusCode::FORBIDDEN, "AccessDenied"));
    };

    if date.year() < 1970 {
        return Some(s3_error_response(StatusCode::FORBIDDEN, "AccessDenied"));
    }

    let skew = date - time::OffsetDateTime::now_utc();
    if skew.abs() > time::Duration::minutes(15) {
        return Some(s3_error_response(
            StatusCode::FORBIDDEN,
            "RequestTimeTooSkewed",
        ));
    }

    None
}

/// Rewrites `CreateBucket` `SigV2` signatures from `/bucket/` to `/bucket`.
///
/// Some `SigV2` clients canonicalize `CreateBucket` with a trailing slash even
/// when the HTTP request path omits it. When that trailing-slash signature is
/// valid, this recalculates the equivalent signature for the actual request
/// path so the normal `s3s` `SigV2` verifier can process the request.
fn rewrite_create_bucket_sig_v2<B>(req: &mut Request<B>, secret_key: &str, auth: &AuthorizationV2) {
    if req.method() != Method::PUT || req.uri().query().is_some() {
        return;
    }
    let path = req.uri().path();
    if path == "/" || path[1..].contains('/') {
        return;
    }

    let current_resource = path.to_string();
    let signed_resource = format!("{path}/");
    let signed_string = sig_v2_string_to_sign(req.method(), req.headers(), &signed_resource);
    if sig_v2_signature(secret_key, &signed_string) != auth.signature {
        return;
    }

    let current_string = sig_v2_string_to_sign(req.method(), req.headers(), &current_resource);
    let signature = sig_v2_signature(secret_key, &current_string);
    let header_value = format!("AWS {}:{signature}", auth.access_key);
    if let Ok(header_value) = HeaderValue::from_str(&header_value) {
        req.headers_mut()
            .insert(header::AUTHORIZATION, header_value);
    }
}

/// Builds the `SigV2` string-to-sign for the limited compatibility rewrite.
fn sig_v2_string_to_sign(method: &Method, headers: &HeaderMap, resource: &str) -> String {
    let mut value = String::with_capacity(256);
    value.push_str(method.as_str());
    value.push('\n');
    value.push_str(header_value(headers, "content-md5").unwrap_or_default());
    value.push('\n');
    value.push_str(header_value(headers, "content-type").unwrap_or_default());
    value.push('\n');
    if headers.contains_key("x-amz-date") {
        value.push('\n');
    } else {
        value.push_str(header_value(headers, "date").unwrap_or_default());
        value.push('\n');
    }
    value.push_str(&canonical_amz_headers(headers));
    value.push_str(resource);
    value
}

/// Reads a UTF-8 header value used by `SigV2` signing.
fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// Canonicalizes `x-amz-*` headers for `SigV2` signing.
fn canonical_amz_headers(headers: &HeaderMap) -> String {
    let mut values = headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().to_ascii_lowercase();
            if name.starts_with("x-amz-") {
                Some((name, value.to_str().ok()?.trim().to_string()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut output = String::new();
    let mut index = 0;
    while index < values.len() {
        let name = &values[index].0;
        output.push_str(name);
        output.push(':');
        output.push_str(&values[index].1);
        index += 1;
        while index < values.len() && values[index].0 == *name {
            output.push(',');
            output.push_str(&values[index].1);
            index += 1;
        }
        output.push('\n');
    }
    output
}

/// Signs a `SigV2` string-to-sign with HMAC-SHA1 and base64 encoding.
fn sig_v2_signature(secret_key: &str, string_to_sign: &str) -> String {
    base64_encode(&hmac_sha1(secret_key.as_bytes(), string_to_sign.as_bytes()))
}

/// Computes HMAC-SHA1 without adding another dependency.
fn hmac_sha1(key: &[u8], data: &[u8]) -> [u8; 20] {
    let mut key_block = [0_u8; 64];
    if key.len() > key_block.len() {
        key_block[..20].copy_from_slice(&Sha1::checksum(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_key = [0x36_u8; 64];
    let mut outer_key = [0x5c_u8; 64];
    for index in 0..key_block.len() {
        inner_key[index] ^= key_block[index];
        outer_key[index] ^= key_block[index];
    }

    let mut inner = Sha1::new();
    inner.update(&inner_key);
    inner.update(data);
    let inner = inner.finalize();

    let mut outer = Sha1::new();
    outer.update(&outer_key);
    outer.update(&inner);
    outer.finalize()
}

/// Decodes bytes with the standard base64 alphabet used by RFC 2047 metadata words.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(input).ok()
}

/// Encodes bytes with the standard base64 alphabet used by `SigV2`.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(char::from(TABLE[usize::from(first >> 2)]));
        output.push(char::from(
            TABLE[usize::from(((first & 0b0000_0011) << 4) | (second >> 4))],
        ));
        if chunk.len() > 1 {
            output.push(char::from(
                TABLE[usize::from(((second & 0b0000_1111) << 2) | (third >> 6))],
            ));
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(char::from(TABLE[usize::from(third & 0b0011_1111)]));
        } else {
            output.push('=');
        }
    }
    output
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

    async fn response_body_text(response: HttpResponse) -> Result<String, HttpError> {
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(HttpError::new)?
            .to_bytes();
        String::from_utf8(bytes.to_vec()).map_err(|err| HttpError::new(Box::new(err)))
    }

    fn error_code(response: HttpResponse) -> Result<String, HttpError> {
        let status = response.status();
        let body = futures::executor::block_on(response_body_text(response))?;
        Ok(format!("{status} {body}"))
    }

    #[tokio::test]
    async fn rewrites_successful_get_object_attributes_etags() -> Result<(), HttpError> {
        let response = HttpResponse::new(Body::from(
            "<GetObjectAttributesResult><ETag>\"abc\"</ETag></GetObjectAttributesResult>"
                .to_string(),
        ));

        let response = rewrite_get_object_attributes_etag(response).await?;
        let expected = "<GetObjectAttributesResult><ETag>abc</ETag></GetObjectAttributesResult>";
        let expected_length = expected.len().to_string();
        assert_eq!(
            response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some(expected_length.as_str())
        );
        let body = response_body_text(response).await?;
        assert_eq!(body, expected);

        Ok(())
    }

    #[tokio::test]
    async fn leaves_unsuccessful_and_non_utf8_attribute_bodies_alone() -> Result<(), HttpError> {
        let mut unsuccessful = HttpResponse::new(Body::from("<ETag>\"abc\"</ETag>".to_string()));
        *unsuccessful.status_mut() = StatusCode::NOT_FOUND;
        let unsuccessful = rewrite_get_object_attributes_etag(unsuccessful).await?;
        assert_eq!(
            response_body_text(unsuccessful).await?,
            "<ETag>\"abc\"</ETag>"
        );

        let non_utf8 = HttpResponse::new(Body::from(Bytes::from_static(&[0xff, b'"'])));
        let non_utf8 = rewrite_get_object_attributes_etag(non_utf8).await?;
        let bytes = non_utf8
            .into_body()
            .collect()
            .await
            .map_err(HttpError::new)?
            .to_bytes();
        assert_eq!(bytes.as_ref(), &[0xff, b'"']);

        Ok(())
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

    #[tokio::test]
    async fn rewrites_object_lock_invalid_retention_error_code() -> Result<(), HttpError> {
        let mut response = HttpResponse::new(Body::from(
            "<Error><Code>InvalidRequest</Code><Message>InvalidRetentionPeriod: retention period must be positive</Message></Error>"
                .to_string(),
        ));
        *response.status_mut() = StatusCode::BAD_REQUEST;

        let response = rewrite_invalid_retention_period_code(response).await?;
        let body = response_body_text(response).await?;
        assert!(body.contains("<Code>InvalidRetentionPeriod</Code>"));
        assert!(!body.contains("<Code>InvalidRequest</Code>"));

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

    #[test]
    fn parses_sig_v2_authorization_header() {
        let Some(auth) = parse_authorization_v2("AWS access:signature") else {
            panic!("valid SigV2 authorization should parse");
        };
        assert_eq!(auth.access_key, "access");
        assert_eq!(auth.signature, "signature");
        assert!(parse_authorization_v2("AWS :signature").is_none());
        assert!(parse_authorization_v2("AWS access:").is_none());
        assert!(parse_authorization_v2("Bearer token").is_none());
    }

    #[test]
    fn validates_sig_v2_dates() {
        let mut headers = HeaderMap::new();
        assert!(validate_sig_v2_date(&headers).is_none());

        headers.insert("x-amz-date", HeaderValue::from_static(""));
        let Some(response) = validate_sig_v2_date(&headers) else {
            panic!("empty date should fail");
        };
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        headers.insert("x-amz-date", HeaderValue::from_static("not a date"));
        let Some(response) = validate_sig_v2_date(&headers) else {
            panic!("invalid date should fail");
        };
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        headers.insert(
            "x-amz-date",
            HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 +0000"),
        );
        let Some(response) = validate_sig_v2_date(&headers) else {
            panic!("skewed date should fail");
        };
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn canonicalizes_sig_v2_headers_and_string_to_sign() -> Result<(), hyper::http::Error> {
        let mut headers = HeaderMap::new();
        headers.insert("content-md5", HeaderValue::from_static("md5"));
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 19:36:42 +0000"),
        );
        headers.insert("x-amz-meta-z", HeaderValue::from_static(" zed "));
        headers.append("x-amz-meta-a", HeaderValue::from_static("one"));
        headers.append("x-amz-meta-a", HeaderValue::from_static("two"));
        headers.insert("not-amz", HeaderValue::from_static("ignored"));

        assert_eq!(
            canonical_amz_headers(&headers),
            "x-amz-meta-a:one,two\nx-amz-meta-z:zed\n"
        );
        assert_eq!(
            sig_v2_string_to_sign(&Method::PUT, &headers, "/bucket/key"),
            "PUT\nmd5\ntext/plain\nTue, 27 Mar 2007 19:36:42 +0000\nx-amz-meta-a:one,two\nx-amz-meta-z:zed\n/bucket/key"
        );

        headers.insert("x-amz-date", HeaderValue::from_static("date wins"));
        assert_eq!(
            sig_v2_string_to_sign(&Method::GET, &headers, "/bucket"),
            "GET\nmd5\ntext/plain\n\nx-amz-date:date wins\nx-amz-meta-a:one,two\nx-amz-meta-z:zed\n/bucket"
        );

        let req = request(Method::GET, "/")?;
        assert_eq!(header_value(req.headers(), "missing"), None);
        Ok(())
    }

    #[test]
    fn signs_hmac_sha1_and_encodes_base64() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(
            sig_v2_signature("key", "The quick brown fox jumps over the lazy dog"),
            "3nybhbi3iqa8ino29wqQcBydtNk="
        );
        assert_eq!(
            sig_v2_signature(
                "key",
                "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
            ),
            "EpELyoQzABuh9lHQ+KwND1xJ+Uc="
        );
    }

    #[test]
    fn rewrites_create_bucket_sig_v2_trailing_slash_signature() -> Result<(), hyper::http::Error> {
        let secret = "secret";
        let mut signed_headers = HeaderMap::new();
        signed_headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 19:36:42 +0000"),
        );
        let signed = sig_v2_string_to_sign(&Method::PUT, &signed_headers, "/bucket/");
        let trailing_slash_signature = sig_v2_signature(secret, &signed);
        let auth = AuthorizationV2 {
            access_key: "access".to_string(),
            signature: trailing_slash_signature,
        };
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri("/bucket")
            .header("date", "Tue, 27 Mar 2007 19:36:42 +0000")
            .body(())?;

        rewrite_create_bucket_sig_v2(&mut req, secret, &auth);

        let Some(value) = req.headers().get(header::AUTHORIZATION) else {
            panic!("authorization header should be rewritten");
        };
        let Ok(value) = value.to_str() else {
            panic!("authorization header should be UTF-8");
        };
        assert!(value.starts_with("AWS access:"));
        assert_ne!(value, format!("AWS access:{}", auth.signature));

        Ok(())
    }

    #[test]
    fn skips_create_bucket_sig_v2_rewrite_for_non_bucket_requests() -> Result<(), hyper::http::Error>
    {
        let auth = AuthorizationV2 {
            access_key: "access".to_string(),
            signature: "signature".to_string(),
        };
        for (method, uri) in [
            (Method::GET, "/bucket"),
            (Method::PUT, "/"),
            (Method::PUT, "/bucket/key"),
            (Method::PUT, "/bucket?versioning"),
        ] {
            let mut req = request(method, uri)?;
            rewrite_create_bucket_sig_v2(&mut req, "secret", &auth);
            assert!(req.headers().get(header::AUTHORIZATION).is_none());
        }
        Ok(())
    }

    #[test]
    fn sig_v2_header_compat_rejects_malformed_auth_and_rewrites_valid_bucket_request()
    -> Result<(), hyper::http::Error> {
        let mut malformed = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(header::AUTHORIZATION, "AWS missing-colon")
            .body(())?;
        let Some(response) = sig_v2_header_compat(&mut malformed, Some("secret")) else {
            panic!("malformed SigV2 auth should be rejected");
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let secret = "secret";
        let mut headers = HeaderMap::new();
        headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 19:36:42 +0000"),
        );
        let signature = sig_v2_signature(
            secret,
            &sig_v2_string_to_sign(&Method::PUT, &headers, "/bucket/"),
        );
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri("/bucket")
            .header("date", "Tue, 27 Mar 2007 19:36:42 +0000")
            .header(header::AUTHORIZATION, format!("AWS access:{signature}"))
            .body(())?;

        assert!(sig_v2_header_compat(&mut req, Some(secret)).is_none());
        let Some(value) = req.headers().get(header::AUTHORIZATION) else {
            panic!("valid bucket SigV2 request should keep authorization");
        };
        let Ok(value) = value.to_str() else {
            panic!("authorization header should be UTF-8");
        };
        assert!(value.starts_with("AWS access:"));
        assert!(!value.ends_with(&signature));

        Ok(())
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

    #[test]
    fn detects_get_object_attributes_requests() -> Result<(), hyper::http::Error> {
        let req = request(Method::GET, "/bucket/key")?;
        assert!(!is_get_object_attributes(&req));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("x-amz-object-attributes", "ETag")
            .body(())?;
        assert!(is_get_object_attributes(&req));

        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/bucket/key")
            .header("x-amz-object-attributes", "ETag")
            .body(())?;
        assert!(!is_get_object_attributes(&req));

        Ok(())
    }
}
