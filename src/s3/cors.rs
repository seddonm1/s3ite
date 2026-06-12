//! S3 bucket CORS request handling, validation, and runtime matching.

use std::{
    ops::Not,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use hyper::{
    Method, Request, StatusCode,
    body::Incoming,
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
        ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
        HOST, ORIGIN, VARY,
    },
};
use s3s::{
    Body, HttpError, HttpResponse,
    dto::{
        CORSConfiguration, CORSRule, CORSRules, DeleteBucketCorsInput, DeleteBucketCorsOutput,
        GetBucketCorsInput, GetBucketCorsOutput, PutBucketCorsInput, PutBucketCorsOutput,
    },
};
use tower::{Layer, Service};

use crate::{
    Config, Sqlite,
    config::{CorsConfig, CorsMode},
};

use super::{
    S3ErrorCode, S3Request, S3Response, S3Result, StoredCorsRule, s3_error, s3_error_from_sqlite,
    s3_error_with_message, validate_expected_bucket_owner,
};

const MAX_CORS_RULES: usize = 100;
const MAX_CORS_RULE_ID_LENGTH: usize = 255;
const ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK: &str = "access-control-allow-private-network";
const ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK: &str = "access-control-request-private-network";
const TRUE: &str = "true";
const VARY_VALUE: &str = "origin, access-control-request-method, access-control-request-headers";

/// Bucket-aware CORS layer used outside the S3 service.
#[derive(Clone, Debug)]
pub(crate) struct BucketCorsLayer {
    sqlite: Sqlite,
    fallback: FallbackCors,
    domain_name: Option<String>,
}

impl BucketCorsLayer {
    /// Builds the runtime CORS layer from service configuration.
    #[must_use]
    pub(crate) fn new(sqlite: Sqlite, config: &Config) -> Self {
        Self {
            sqlite,
            fallback: FallbackCors::from_mode(config.cors_mode()),
            domain_name: config.domain_name.clone(),
        }
    }
}

impl<S> Layer<S> for BucketCorsLayer {
    type Service = BucketCorsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BucketCorsService {
            inner,
            sqlite: self.sqlite.clone(),
            fallback: self.fallback.clone(),
            domain_name: self.domain_name.clone(),
        }
    }
}

/// Service that applies bucket CORS before auth for preflight and after inner calls otherwise.
#[derive(Clone, Debug)]
pub(crate) struct BucketCorsService<S> {
    inner: S,
    sqlite: Sqlite,
    fallback: FallbackCors,
    domain_name: Option<String>,
}

impl<S> Service<Request<Incoming>> for BucketCorsService<S>
where
    S: Service<Request<Incoming>, Response = HttpResponse, Error = HttpError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse;
    type Error = HttpError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        let sqlite = self.sqlite.clone();
        let fallback = self.fallback.clone();
        let domain_name = self.domain_name.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            handle_cors_request(&sqlite, &fallback, domain_name.as_deref(), &mut inner, req).await
        })
    }
}

/// Deletes CORS rules from an existing bucket.
pub(super) async fn delete_bucket_cors(
    sqlite: &Sqlite,
    req: S3Request<DeleteBucketCorsInput>,
) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
    let DeleteBucketCorsInput {
        bucket,
        expected_bucket_owner,
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            Sqlite::replace_bucket_cors_rules(transaction, &[])?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteBucketCorsOutput::default()))
}

/// Reads CORS rules from an existing bucket.
pub(super) async fn get_bucket_cors(
    sqlite: &Sqlite,
    req: S3Request<GetBucketCorsInput>,
) -> S3Result<S3Response<GetBucketCorsOutput>> {
    let GetBucketCorsInput {
        bucket,
        expected_bucket_owner,
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let rules = connection
        .read(Sqlite::get_bucket_cors_rules)
        .await
        .map_err(s3_error_from_sqlite)?;
    if rules.is_empty() {
        return Err(s3_error(S3ErrorCode::NoSuchCORSConfiguration));
    }

    Ok(S3Response::new(GetBucketCorsOutput {
        cors_rules: Some(cors_rules_from_stored(rules)),
    }))
}

/// Replaces CORS rules on an existing bucket.
pub(super) async fn put_bucket_cors(
    sqlite: &Sqlite,
    req: S3Request<PutBucketCorsInput>,
) -> S3Result<S3Response<PutBucketCorsOutput>> {
    let PutBucketCorsInput {
        bucket,
        cors_configuration,
        expected_bucket_owner,
        ..
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let rules = stored_cors_rules_from_configuration(cors_configuration)?;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            Sqlite::replace_bucket_cors_rules(transaction, &rules)?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(PutBucketCorsOutput::default()))
}

fn stored_cors_rules_from_configuration(
    configuration: CORSConfiguration,
) -> S3Result<Vec<StoredCorsRule>> {
    let mut rules = Vec::with_capacity(configuration.cors_rules.len());
    for rule in configuration.cors_rules {
        rules.push(StoredCorsRule {
            id: rule.id,
            allowed_origins: rule.allowed_origins,
            allowed_methods: rule.allowed_methods,
            allowed_headers: rule.allowed_headers.unwrap_or_default(),
            expose_headers: rule.expose_headers.unwrap_or_default(),
            max_age_seconds: rule.max_age_seconds,
        });
    }
    validate_stored_cors_rules(&rules)?;
    Ok(rules)
}

fn cors_rules_from_stored(rules: Vec<StoredCorsRule>) -> CORSRules {
    rules
        .into_iter()
        .map(|rule| CORSRule {
            allowed_headers: optional_list(rule.allowed_headers),
            allowed_methods: rule.allowed_methods,
            allowed_origins: rule.allowed_origins,
            expose_headers: optional_list(rule.expose_headers),
            id: rule.id,
            max_age_seconds: rule.max_age_seconds,
        })
        .collect()
}

fn optional_list(values: Vec<String>) -> Option<Vec<String>> {
    values.is_empty().not().then_some(values)
}

fn validate_stored_cors_rules(rules: &[StoredCorsRule]) -> S3Result<()> {
    if rules.len() > MAX_CORS_RULES {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "CORS configuration cannot contain more than 100 rules",
        ));
    }

    for rule in rules {
        validate_cors_rule(rule)?;
    }
    Ok(())
}

fn validate_cors_rule(rule: &StoredCorsRule) -> S3Result<()> {
    if rule
        .id
        .as_ref()
        .is_some_and(|id| id.len() > MAX_CORS_RULE_ID_LENGTH)
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "CORS rule ID cannot be longer than 255 characters",
        ));
    }
    if rule.allowed_origins.is_empty() || rule.allowed_methods.is_empty() {
        return Err(s3_error(S3ErrorCode::MalformedXML));
    }

    validate_nonempty_values(&rule.allowed_origins, "CORS allowed origin cannot be empty")?;
    validate_nonempty_values(&rule.allowed_methods, "CORS allowed method cannot be empty")?;
    validate_nonempty_values(&rule.allowed_headers, "CORS allowed header cannot be empty")?;
    validate_nonempty_values(&rule.expose_headers, "CORS expose header cannot be empty")?;

    for method in &rule.allowed_methods {
        validate_cors_method(method)?;
    }
    for header in &rule.allowed_headers {
        validate_header_pattern(header)?;
    }
    for header in &rule.expose_headers {
        validate_header_name(header)?;
    }

    Ok(())
}

fn validate_nonempty_values(values: &[String], message: &'static str) -> S3Result<()> {
    if values.iter().any(String::is_empty) {
        return Err(s3_error_with_message(S3ErrorCode::InvalidRequest, message));
    }
    Ok(())
}

fn validate_cors_method(method: &str) -> S3Result<()> {
    if matches!(method, "GET" | "PUT" | "HEAD" | "POST" | "DELETE") {
        return Ok(());
    }
    Err(s3_error_with_message(
        S3ErrorCode::InvalidRequest,
        "CORS allowed method must be GET, PUT, HEAD, POST, or DELETE",
    ))
}

fn validate_header_pattern(pattern: &str) -> S3Result<()> {
    if pattern == "*" {
        return Ok(());
    }
    for part in pattern.split('*').filter(|part| part.is_empty().not()) {
        validate_header_name(part)?;
    }
    Ok(())
}

fn validate_header_name(name: &str) -> S3Result<()> {
    hyper::header::HeaderName::from_bytes(name.as_bytes())
        .map(|_| ())
        .map_err(|_| {
            s3_error_with_message(S3ErrorCode::InvalidRequest, "CORS header name is invalid")
        })
}

async fn handle_cors_request<S>(
    sqlite: &Sqlite,
    fallback: &FallbackCors,
    domain_name: Option<&str>,
    inner: &mut S,
    req: Request<Incoming>,
) -> Result<HttpResponse, HttpError>
where
    S: Service<Request<Incoming>, Response = HttpResponse, Error = HttpError>,
{
    if req.method() == Method::OPTIONS {
        let Some(origin) = req.headers().get(ORIGIN).cloned() else {
            return Ok(empty_response(StatusCode::BAD_REQUEST));
        };
        if req
            .headers()
            .contains_key(ACCESS_CONTROL_REQUEST_METHOD)
            .not()
        {
            return Ok(empty_response(StatusCode::BAD_REQUEST));
        }

        let bucket = request_bucket(&req, domain_name);
        let bucket_rules = bucket_cors_rules(sqlite, bucket.as_deref()).await;
        return Ok(handle_preflight(
            fallback,
            &req,
            &origin,
            bucket_rules.as_deref(),
        ));
    }

    let Some(origin) = req.headers().get(ORIGIN).cloned() else {
        return inner.call(req).await;
    };

    let bucket = request_bucket(&req, domain_name);
    let bucket_rules = bucket_cors_rules(sqlite, bucket.as_deref()).await;

    let match_source = CorsMatchSource::select(fallback, bucket_rules.as_deref());
    let method = actual_cors_method(&req);
    let actual_headers = match_source.and_then(|source| source.actual_headers(&origin, method));
    let mut response = inner.call(req).await?;
    if let Some(headers) = actual_headers {
        headers.insert_actual(response.headers_mut());
    }
    Ok(response)
}

fn actual_cors_method(req: &Request<Incoming>) -> &str {
    req.headers()
        .get(ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| req.method().as_str())
}

fn handle_preflight(
    fallback: &FallbackCors,
    req: &Request<Incoming>,
    origin: &hyper::header::HeaderValue,
    bucket_rules: Option<&[StoredCorsRule]>,
) -> HttpResponse {
    let source = CorsMatchSource::select(fallback, bucket_rules);
    match source.and_then(|source| source.preflight_headers(origin, req.headers())) {
        Some(headers) => preflight_response(headers),
        None if bucket_rules.is_some_and(|rules| rules.is_empty().not()) => {
            empty_response(StatusCode::FORBIDDEN)
        }
        None if fallback.is_disabled() => empty_response(StatusCode::FORBIDDEN),
        None => empty_response(StatusCode::OK),
    }
}

fn preflight_response(headers: CorsResponseHeaders) -> HttpResponse {
    let mut response = empty_response(StatusCode::OK);
    headers.insert_preflight(response.headers_mut());
    response
}

fn empty_response(status: StatusCode) -> HttpResponse {
    let mut response = HttpResponse::default();
    *response.status_mut() = status;
    *response.body_mut() = Body::empty();
    response
}

async fn bucket_cors_rules(sqlite: &Sqlite, bucket: Option<&str>) -> Option<Vec<StoredCorsRule>> {
    let bucket = bucket?;
    let connection = sqlite.get_connection(bucket).await.ok()?;
    connection.read(Sqlite::get_bucket_cors_rules).await.ok()
}

fn request_bucket(req: &Request<Incoming>, domain_name: Option<&str>) -> Option<String> {
    if let Some(bucket) = virtual_host_bucket(req, domain_name) {
        return Some(bucket);
    }

    req.uri()
        .path()
        .trim_start_matches('/')
        .split('/')
        .next()
        .filter(|bucket| bucket.is_empty().not())
        .map(str::to_string)
}

fn virtual_host_bucket(req: &Request<Incoming>, domain_name: Option<&str>) -> Option<String> {
    let domain_name = domain_name?;
    let host = req.headers().get(HOST)?.to_str().ok()?;
    host.strip_suffix(domain_name)
        .and_then(|prefix| prefix.strip_suffix('.'))
        .filter(|bucket| bucket.is_empty().not())
        .map(str::to_string)
}

#[derive(Clone, Debug)]
enum FallbackCors {
    Disabled,
    Permissive,
    Custom(ConfiguredCors),
}

impl FallbackCors {
    fn from_mode(mode: CorsMode) -> Self {
        match mode {
            CorsMode::Disabled => Self::Disabled,
            CorsMode::Permissive => Self::Permissive,
            CorsMode::Custom(config) => Self::Custom(ConfiguredCors::from_config(config)),
        }
    }

    fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

#[derive(Clone, Debug)]
struct ConfiguredCors {
    rule: StoredCorsRule,
    allow_credentials: bool,
    allow_private_network: bool,
}

impl ConfiguredCors {
    fn from_config(config: CorsConfig) -> Self {
        Self {
            rule: StoredCorsRule {
                id: None,
                allowed_origins: config.allow_origins,
                allowed_methods: config.allow_methods,
                allowed_headers: config.allow_headers,
                expose_headers: config.expose_headers,
                max_age_seconds: config
                    .max_age_seconds
                    .and_then(|seconds| i32::try_from(seconds).ok()),
            },
            allow_credentials: config.allow_credentials,
            allow_private_network: config.allow_private_network,
        }
    }
}

enum CorsMatchSource<'a> {
    Bucket(&'a [StoredCorsRule]),
    Permissive,
    Custom(&'a ConfiguredCors),
}

impl<'a> CorsMatchSource<'a> {
    fn select(
        fallback: &'a FallbackCors,
        bucket_rules: Option<&'a [StoredCorsRule]>,
    ) -> Option<Self> {
        if let Some(rules) = bucket_rules
            && rules.is_empty().not()
        {
            return Some(Self::Bucket(rules));
        }

        match fallback {
            FallbackCors::Disabled => None,
            FallbackCors::Permissive => Some(Self::Permissive),
            FallbackCors::Custom(config) => Some(Self::Custom(config)),
        }
    }

    fn preflight_headers(
        &self,
        origin: &hyper::header::HeaderValue,
        headers: &hyper::HeaderMap,
    ) -> CorsDecision {
        match self {
            Self::Bucket(rules) => bucket_preflight_headers(rules, origin, headers),
            Self::Permissive => Some(permissive_preflight_headers(origin, headers)),
            Self::Custom(config) => custom_preflight_headers(config, origin, headers),
        }
    }

    fn actual_headers(&self, origin: &hyper::header::HeaderValue, method: &str) -> CorsDecision {
        match self {
            Self::Bucket(rules) => bucket_actual_headers(rules, origin, method),
            Self::Permissive => Some(permissive_actual_headers(origin)),
            Self::Custom(config) => custom_actual_headers(config, origin, method),
        }
    }
}

type CorsDecision = Option<CorsResponseHeaders>;

#[derive(Clone, Debug)]
struct CorsResponseHeaders {
    allow_origin: hyper::header::HeaderValue,
    allow_methods: Option<hyper::header::HeaderValue>,
    allow_headers: Option<hyper::header::HeaderValue>,
    expose_headers: Option<hyper::header::HeaderValue>,
    max_age: Option<hyper::header::HeaderValue>,
    allow_credentials: bool,
    allow_private_network: bool,
}

impl CorsResponseHeaders {
    fn insert_preflight(self, headers: &mut hyper::HeaderMap) {
        self.insert_common(headers);
        if let Some(value) = self.allow_methods {
            headers.insert(ACCESS_CONTROL_ALLOW_METHODS, value);
        }
        if let Some(value) = self.allow_headers {
            headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, value);
        }
        if let Some(value) = self.max_age {
            headers.insert(ACCESS_CONTROL_MAX_AGE, value);
        }
    }

    fn insert_actual(self, headers: &mut hyper::HeaderMap) {
        self.insert_common(headers);
        if let Some(value) = self.allow_methods {
            headers.insert(ACCESS_CONTROL_ALLOW_METHODS, value);
        }
        if let Some(value) = self.expose_headers {
            headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, value);
        }
    }

    fn insert_common(&self, headers: &mut hyper::HeaderMap) {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, self.allow_origin.clone());
        headers.insert(VARY, header_value(VARY_VALUE));
        if self.allow_credentials {
            headers.insert(ACCESS_CONTROL_ALLOW_CREDENTIALS, header_value(TRUE));
        }
        if self.allow_private_network {
            headers.insert(
                hyper::header::HeaderName::from_static(ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK),
                header_value(TRUE),
            );
        }
    }
}

fn bucket_preflight_headers(
    rules: &[StoredCorsRule],
    origin: &hyper::header::HeaderValue,
    headers: &hyper::HeaderMap,
) -> CorsDecision {
    let requested_method = headers.get(ACCESS_CONTROL_REQUEST_METHOD)?.to_str().ok()?;
    let requested_headers = requested_headers(headers)?;
    let origin = origin.to_str().ok()?;
    let rule = rules.iter().find(|rule| {
        origin_allowed(&rule.allowed_origins, origin)
            && method_allowed(&rule.allowed_methods, requested_method)
            && headers_allowed(&rule.allowed_headers, &requested_headers)
    })?;

    Some(CorsResponseHeaders {
        allow_origin: allowed_origin_header(&rule.allowed_origins, origin),
        allow_methods: Some(header_value(requested_method)),
        allow_headers: requested_header_value(headers, &rule.allowed_headers),
        expose_headers: None,
        max_age: rule
            .max_age_seconds
            .map(|seconds| header_value(&seconds.to_string())),
        allow_credentials: false,
        allow_private_network: false,
    })
}

fn bucket_actual_headers(
    rules: &[StoredCorsRule],
    origin: &hyper::header::HeaderValue,
    method: &str,
) -> CorsDecision {
    let origin = origin.to_str().ok()?;
    let rule = rules.iter().find(|rule| {
        origin_allowed(&rule.allowed_origins, origin)
            && method_allowed(&rule.allowed_methods, method)
    })?;

    Some(CorsResponseHeaders {
        allow_origin: allowed_origin_header(&rule.allowed_origins, origin),
        allow_methods: configured_header_value(&rule.allowed_methods),
        allow_headers: None,
        expose_headers: comma_header_value(&rule.expose_headers),
        max_age: None,
        allow_credentials: false,
        allow_private_network: false,
    })
}

fn permissive_preflight_headers(
    origin: &hyper::header::HeaderValue,
    headers: &hyper::HeaderMap,
) -> CorsResponseHeaders {
    CorsResponseHeaders {
        allow_origin: origin.clone(),
        allow_methods: headers.get(ACCESS_CONTROL_REQUEST_METHOD).cloned(),
        allow_headers: headers.get(ACCESS_CONTROL_REQUEST_HEADERS).cloned(),
        expose_headers: None,
        max_age: None,
        allow_credentials: true,
        allow_private_network: private_network_requested(headers),
    }
}

fn permissive_actual_headers(origin: &hyper::header::HeaderValue) -> CorsResponseHeaders {
    CorsResponseHeaders {
        allow_origin: origin.clone(),
        allow_methods: None,
        allow_headers: None,
        expose_headers: None,
        max_age: None,
        allow_credentials: true,
        allow_private_network: false,
    }
}

fn custom_preflight_headers(
    config: &ConfiguredCors,
    origin: &hyper::header::HeaderValue,
    headers: &hyper::HeaderMap,
) -> CorsDecision {
    let requested_method = headers.get(ACCESS_CONTROL_REQUEST_METHOD)?.to_str().ok()?;
    let origin_str = origin.to_str().ok()?;
    if origin_allowed(&config.rule.allowed_origins, origin_str).not()
        || method_allowed(&config.rule.allowed_methods, requested_method).not()
    {
        return None;
    }

    Some(CorsResponseHeaders {
        allow_origin: config_origin_header(&config.rule.allowed_origins, origin_str),
        allow_methods: configured_header_value(&config.rule.allowed_methods),
        allow_headers: configured_header_value(&config.rule.allowed_headers),
        expose_headers: None,
        max_age: config
            .rule
            .max_age_seconds
            .map(|seconds| header_value(&seconds.to_string())),
        allow_credentials: config.allow_credentials,
        allow_private_network: config.allow_private_network && private_network_requested(headers),
    })
}

fn custom_actual_headers(
    config: &ConfiguredCors,
    origin: &hyper::header::HeaderValue,
    method: &str,
) -> CorsDecision {
    let origin_str = origin.to_str().ok()?;
    if origin_allowed(&config.rule.allowed_origins, origin_str).not()
        || method_allowed(&config.rule.allowed_methods, method).not()
    {
        return None;
    }

    Some(CorsResponseHeaders {
        allow_origin: config_origin_header(&config.rule.allowed_origins, origin_str),
        allow_methods: None,
        allow_headers: None,
        expose_headers: configured_header_value(&config.rule.expose_headers),
        max_age: None,
        allow_credentials: config.allow_credentials,
        allow_private_network: false,
    })
}

fn requested_headers(headers: &hyper::HeaderMap) -> Option<Vec<String>> {
    let Some(value) = headers.get(ACCESS_CONTROL_REQUEST_HEADERS) else {
        return Some(Vec::new());
    };
    Some(
        value
            .to_str()
            .ok()?
            .split(',')
            .map(str::trim)
            .filter(|header| header.is_empty().not())
            .map(str::to_ascii_lowercase)
            .collect(),
    )
}

fn requested_header_value(
    headers: &hyper::HeaderMap,
    allowed_headers: &[String],
) -> Option<hyper::header::HeaderValue> {
    let requested = headers.get(ACCESS_CONTROL_REQUEST_HEADERS)?;
    if allowed_headers.is_empty() {
        return None;
    }
    Some(requested.clone())
}

fn headers_allowed(allowed_headers: &[String], requested_headers: &[String]) -> bool {
    if requested_headers.is_empty() {
        return true;
    }
    requested_headers.iter().all(|requested| {
        allowed_headers
            .iter()
            .any(|allowed| wildcard_match(&allowed.to_ascii_lowercase(), requested))
    })
}

fn origin_allowed(allowed_origins: &[String], origin: &str) -> bool {
    allowed_origins
        .iter()
        .any(|allowed| wildcard_match(allowed, origin))
}

fn method_allowed(allowed_methods: &[String], method: &str) -> bool {
    allowed_methods
        .iter()
        .any(|allowed| allowed == method || allowed == "*")
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.contains('*').not() {
        return pattern == value;
    }

    let mut remainder = value;
    let mut parts = pattern.split('*').peekable();
    let first = parts.next().unwrap_or_default();
    if first.is_empty().not() {
        let Some(stripped) = remainder.strip_prefix(first) else {
            return false;
        };
        remainder = stripped;
    }

    while let Some(part) = parts.next() {
        if part.is_empty() {
            continue;
        }
        if parts.peek().is_none() {
            return remainder.ends_with(part);
        }
        let Some(index) = remainder.find(part) else {
            return false;
        };
        remainder = &remainder[index + part.len()..];
    }

    pattern.ends_with('*') || remainder.is_empty()
}

fn allowed_origin_header(allowed_origins: &[String], origin: &str) -> hyper::header::HeaderValue {
    if allowed_origins.iter().any(|allowed| allowed == "*") {
        return header_value("*");
    }
    header_value(origin)
}

fn config_origin_header(allowed_origins: &[String], origin: &str) -> hyper::header::HeaderValue {
    if allowed_origins.len() == 1 && allowed_origins[0] == "*" {
        return header_value("*");
    }
    header_value(origin)
}

fn configured_header_value(values: &[String]) -> Option<hyper::header::HeaderValue> {
    comma_header_value(values)
}

fn comma_header_value(values: &[String]) -> Option<hyper::header::HeaderValue> {
    values
        .is_empty()
        .not()
        .then(|| header_value(&values.join(",")))
}

fn header_value(value: &str) -> hyper::header::HeaderValue {
    hyper::header::HeaderValue::from_str(value)
        .unwrap_or_else(|_| hyper::header::HeaderValue::from_static(""))
}

fn private_network_requested(headers: &hyper::HeaderMap) -> bool {
    headers
        .get(hyper::header::HeaderName::from_static(
            ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK,
        ))
        .is_some_and(|value| value == TRUE)
}

#[cfg(test)]
mod tests {
    use hyper::{HeaderMap, header::HeaderValue};

    use super::*;

    fn valid_rule() -> StoredCorsRule {
        StoredCorsRule {
            id: Some("rule".to_string()),
            allowed_origins: vec!["https://app.example.test".to_string()],
            allowed_methods: vec!["GET".to_string(), "HEAD".to_string()],
            allowed_headers: vec!["authorization".to_string(), "x-amz-*".to_string()],
            expose_headers: vec!["ETag".to_string()],
            max_age_seconds: Some(600),
        }
    }

    #[test]
    fn validates_cors_rule_required_fields_and_limits() {
        assert!(validate_stored_cors_rules(&[valid_rule()]).is_ok());

        let mut too_many = vec![valid_rule(); MAX_CORS_RULES + 1];
        assert!(validate_stored_cors_rules(&too_many).is_err());
        too_many.truncate(MAX_CORS_RULES);
        assert!(validate_stored_cors_rules(&too_many).is_ok());

        let mut missing_origin = valid_rule();
        missing_origin.allowed_origins.clear();
        assert!(validate_stored_cors_rules(&[missing_origin]).is_err());

        let mut missing_method = valid_rule();
        missing_method.allowed_methods.clear();
        assert!(validate_stored_cors_rules(&[missing_method]).is_err());

        let mut overlong_id = valid_rule();
        overlong_id.id = Some("x".repeat(MAX_CORS_RULE_ID_LENGTH + 1));
        assert!(validate_stored_cors_rules(&[overlong_id]).is_err());
    }

    #[test]
    fn rejects_invalid_methods_and_header_names() {
        let mut invalid_method = valid_rule();
        invalid_method.allowed_methods = vec!["PATCH".to_string()];
        assert!(validate_stored_cors_rules(&[invalid_method]).is_err());

        let mut empty_origin = valid_rule();
        empty_origin.allowed_origins = vec![String::new()];
        assert!(validate_stored_cors_rules(&[empty_origin]).is_err());
        assert!(validate_header_pattern("*").is_ok());

        let mut invalid_allowed_header = valid_rule();
        invalid_allowed_header.allowed_headers = vec!["bad header".to_string()];
        assert!(validate_stored_cors_rules(&[invalid_allowed_header]).is_err());

        let mut invalid_expose_header = valid_rule();
        invalid_expose_header.expose_headers = vec!["x bad".to_string()];
        assert!(validate_stored_cors_rules(&[invalid_expose_header]).is_err());
    }

    #[test]
    fn converts_cors_dto_to_stored_rules_and_back() {
        let configuration = CORSConfiguration {
            cors_rules: vec![CORSRule {
                allowed_headers: Some(vec!["authorization".to_string()]),
                allowed_methods: vec!["GET".to_string()],
                allowed_origins: vec!["https://app.example.test".to_string()],
                expose_headers: Some(vec!["ETag".to_string()]),
                id: Some("rule".to_string()),
                max_age_seconds: Some(600),
            }],
        };

        let stored = stored_cors_rules_from_configuration(configuration)
            .unwrap_or_else(|err| panic!("valid CORS configuration should normalize: {err}"));
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].allowed_methods, ["GET"]);

        let output = cors_rules_from_stored(stored);
        assert_eq!(
            output[0].allowed_headers.as_deref(),
            Some(&["authorization".to_string()][..])
        );
        assert_eq!(
            output[0].expose_headers.as_deref(),
            Some(&["ETag".to_string()][..])
        );
    }

    #[test]
    fn matches_bucket_preflight_rules() {
        let rules = vec![valid_rule()];
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("GET"),
        );
        headers.insert(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static("authorization,x-amz-date"),
        );
        let origin = HeaderValue::from_static("https://app.example.test");

        let response = bucket_preflight_headers(&rules, &origin, &headers)
            .unwrap_or_else(|| panic!("matching CORS preflight should produce headers"));
        assert_eq!(
            response.allow_origin,
            HeaderValue::from_static("https://app.example.test")
        );
        assert_eq!(response.max_age, Some(HeaderValue::from_static("600")));

        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("PUT"),
        );
        assert!(bucket_preflight_headers(&rules, &origin, &headers).is_none());
    }

    #[test]
    fn fallback_and_configured_cors_helpers_cover_response_shapes() {
        let origin = HeaderValue::from_static("https://app.example.test");
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("GET"),
        );
        headers.insert(
            ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static("authorization"),
        );
        headers.insert(
            hyper::header::HeaderName::from_static(ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK),
            HeaderValue::from_static(TRUE),
        );

        let permissive_preflight = permissive_preflight_headers(&origin, &headers);
        assert!(permissive_preflight.allow_private_network);
        let permissive_actual = permissive_actual_headers(&origin);
        assert!(permissive_actual.allow_credentials);
        assert!(!permissive_actual.allow_private_network);

        let custom = ConfiguredCors {
            rule: valid_rule(),
            allow_credentials: true,
            allow_private_network: true,
        };
        let custom_preflight = custom_preflight_headers(&custom, &origin, &headers);
        assert!(custom_preflight.is_some_and(|headers| headers.allow_private_network));
        assert!(
            custom_preflight_headers(
                &custom,
                &HeaderValue::from_static("https://other.example.test"),
                &headers,
            )
            .is_none()
        );
        assert!(custom_actual_headers(&custom, &origin, "PUT").is_none());
        let custom_actual = custom_actual_headers(&custom, &origin, "GET");
        assert!(custom_actual.is_some_and(|headers| headers.expose_headers.is_some()));

        assert!(FallbackCors::Disabled.is_disabled());
        assert!(CorsMatchSource::select(&FallbackCors::Disabled, None).is_none());
        assert!(matches!(
            CorsMatchSource::select(&FallbackCors::Permissive, None),
            Some(CorsMatchSource::Permissive)
        ));
        let bucket_rules = vec![valid_rule()];
        assert!(matches!(
            CorsMatchSource::select(&FallbackCors::Disabled, Some(&bucket_rules)),
            Some(CorsMatchSource::Bucket(_))
        ));
    }

    #[test]
    fn wildcard_matching_supports_s3_cors_patterns() {
        assert!(wildcard_match("*", "https://app.example.test"));
        assert!(wildcard_match(
            "https://*.example.test",
            "https://app.example.test"
        ));
        assert!(wildcard_match("x-amz-*", "x-amz-date"));
        assert!(!wildcard_match(
            "https://*.example.test",
            "https://example.test"
        ));
    }
}
