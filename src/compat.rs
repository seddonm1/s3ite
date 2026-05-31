//! Protocol compatibility layers that run before the `s3s` service.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{
    body::Incoming,
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
};
use s3s::{
    Body, HttpError, HttpResponse,
    crypto::{Checksum, Sha1},
    header::{
        CONTENT_ENCODING, CONTENT_LENGTH, X_AMZ_CONTENT_SHA256, X_AMZ_DECODED_CONTENT_LENGTH,
    },
};
use tower::{Layer, Service};

type CompatBody = UnsyncBoxBody<Bytes, hyper::Error>;

/// Normalizes decoded HTTP chunked uploads into fixed-length bodies for `s3s`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChunkedBodyCompatLayer;

impl<S> Layer<S> for ChunkedBodyCompatLayer {
    type Service = ChunkedBodyCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ChunkedBodyCompatService { inner }
    }
}

/// Service wrapper for [`ChunkedBodyCompatLayer`].
#[derive(Clone, Debug)]
pub(crate) struct ChunkedBodyCompatService<S> {
    inner: S,
}

impl<S> Service<Request<Incoming>> for ChunkedBodyCompatService<S>
where
    S: Service<Request<CompatBody>, Response = HttpResponse, Error = HttpError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse;
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if should_preserve_body_stream(&req) {
            let future = self.inner.call(req.map(BodyExt::boxed_unsync));
            return Box::pin(future);
        }

        let mut inner = {
            let clone = self.inner.clone();
            std::mem::replace(&mut self.inner, clone)
        };
        Box::pin(async move {
            let (mut parts, body) = req.into_parts();
            let bytes = body
                .collect()
                .await
                .map_err(|err| HttpError::new(Box::new(err)))?
                .to_bytes();
            let content_length = bytes
                .len()
                .to_string()
                .parse()
                .map_err(|err| HttpError::new(Box::new(err)))?;
            parts.headers.insert(CONTENT_LENGTH, content_length);
            parts.headers.remove(header::TRANSFER_ENCODING);
            inner
                .call(Request::from_parts(
                    parts,
                    Full::new(bytes)
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                ))
                .await
        })
    }
}

fn should_preserve_body_stream(req: &Request<Incoming>) -> bool {
    if !matches!(*req.method(), Method::PUT | Method::POST) {
        return true;
    }

    let headers = req.headers();
    headers.contains_key(CONTENT_LENGTH)
        || headers.contains_key(X_AMZ_DECODED_CONTENT_LENGTH)
        || headers
            .get(X_AMZ_CONTENT_SHA256)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("STREAMING-"))
        || headers
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("aws-chunked"))
            })
}

/// Handles request-header compatibility checks before `s3s` normalization.
#[derive(Clone, Debug)]
pub(crate) struct HeaderAuthCompatLayer {
    secret_key: Option<Arc<str>>,
}

impl HeaderAuthCompatLayer {
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
        if let Some(response) = sig_v2_header_compat(&mut req, self.secret_key.as_deref()) {
            return Box::pin(async move { Ok(response) });
        }

        let future = self.inner.call(req);
        Box::pin(async move {
            let response = future.await?;
            if rewrite_object_attributes {
                rewrite_get_object_attributes_etag(response).await
            } else {
                Ok(response)
            }
        })
    }
}

fn is_get_object_attributes<B>(req: &Request<B>) -> bool {
    req.method() == Method::GET && req.headers().contains_key("x-amz-object-attributes")
}

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

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

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

fn sig_v2_signature(secret_key: &str, string_to_sign: &str) -> String {
    base64_encode(&hmac_sha1(secret_key.as_bytes(), string_to_sign.as_bytes()))
}

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
