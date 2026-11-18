//! Structured OTLP audit records for S3 requests and committed bulk delete results.

use hyper::{HeaderMap, Request};
use opentelemetry::{
    logs::{LogRecord, Logger, Severity},
    trace::SpanContext,
};
use opentelemetry_sdk::logs::{SdkLogRecord, SdkLogger};
use std::{
    cell::{Cell, RefCell},
    fmt,
    net::IpAddr,
    sync::OnceLock,
    time::SystemTime,
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id},
};
use tracing_subscriber::{Layer, layer::Context};
use uuid::Uuid;

pub(super) static AUDIT_LOGGER: OnceLock<SdkLogger> = OnceLock::new();
tokio::task_local! {
    pub(super) static REQUEST: RequestContext;
}

/// Verified S3 handler fields captured after authentication.
#[derive(Default)]
struct Target {
    user: Option<String>,
    bucket: Option<String>,
    key: Option<String>,
    version: Option<String>,
}

impl Visit for Target {
    /// Copies only allowlisted fields, excluding request headers and credentials used to sign.
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let slot = match field.name() {
            "user_id" => &mut self.user,
            "bucket" => &mut self.bucket,
            "key" => &mut self.key,
            "version_id" => &mut self.version,
            _ => return,
        };
        let value = format!("{value:?}");
        if !value.is_empty() {
            *slot = Some(value);
        }
    }
}

/// Observes the existing S3 dispatch spans independently of the trace exporter.
pub(super) struct CaptureOperation;
impl<S: Subscriber> Layer<S> for CaptureOperation {
    /// Associates the first handler entered with the enclosing HTTP request.
    fn on_new_span(&self, attrs: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
        let _ = REQUEST.try_with(|state| {
            if state.operation.get() == "unrouted" {
                state.operation.set(attrs.metadata().name());
                if AUDIT_LOGGER.get().is_some() {
                    attrs.record(&mut *state.target.borrow_mut());
                }
            }
        });
    }
}

/// Correlation and routing data isolated to one HTTP request.
pub(super) struct RequestContext {
    pub(super) id: Uuid,
    pub(super) operation: Cell<&'static str>,
    target: RefCell<Target>,
    method: String,
    path: Option<String>,
    ip: IpAddr,
    trace: SpanContext,
    bypass: bool,
}

impl RequestContext {
    /// Captures safe HTTP metadata before routing and retains the propagated trace identity.
    pub(super) fn new<B>(req: &Request<B>, ip: IpAddr, trace: SpanContext) -> Self {
        Self {
            id: Uuid::new_v4(),
            operation: Cell::new("unrouted"),
            target: RefCell::default(),
            method: req.method().to_string(),
            path: AUDIT_LOGGER.get().map(|_| req.uri().path().to_owned()),
            ip,
            trace,
            bypass: req
                .headers()
                .get("x-amz-bypass-governance-retention")
                .is_some_and(|v| v == "true"),
        }
    }

    /// Adds common attribution and correlation fields to a new log record.
    fn record(&self, logger: &SdkLogger, event: &'static str, id: Option<&str>) -> SdkLogRecord {
        let mut log = logger.create_log_record();
        log.set_event_name(event);
        log.set_timestamp(SystemTime::now());
        log.set_severity_number(Severity::Info);
        log.add_attribute(
            "s3.request.id",
            id.map_or_else(|| self.id.simple().to_string(), str::to_owned),
        );
        log.add_attribute("s3.operation", self.operation.get());
        log.add_attribute("client.address", self.ip.to_string());
        log.add_attribute("http.request.method", self.method.clone());
        if self.bypass {
            log.add_attribute("s3.governance_bypass_requested", true);
        }
        if self.trace.is_valid() {
            log.set_trace_context(
                self.trace.trace_id(),
                self.trace.span_id(),
                Some(self.trace.trace_flags()),
            );
        }
        let target = self.target.borrow();
        for (name, value) in [
            ("s3.principal", &target.user),
            ("s3.bucket", &target.bucket),
            ("s3.key", &target.key),
            ("s3.request.version_id", &target.version),
        ] {
            if let Some(value) = value {
                log.add_attribute(name, value.clone());
            }
        }
        log
    }

    /// Exports mutations and failures, with successful read auditing enabled by configuration.
    pub(super) fn completed(&self, status: u16, reads: bool, headers: Option<&HeaderMap>) {
        let Some(logger) = AUDIT_LOGGER.get() else {
            return;
        };
        let op = self.operation.get();
        let read = op.starts_with("get_")
            || op.starts_with("head_")
            || op.starts_with("list_")
            || (op == "unrouted" && matches!(self.method.as_str(), "GET" | "HEAD" | "OPTIONS"));
        if read && status < 400 && !reads {
            return;
        }
        let header = |name: &str| {
            headers
                .and_then(|h| h.get(name))
                .and_then(|v| v.to_str().ok())
        };
        let mut log = self.record(logger, "s3.request", header("x-amz-request-id"));
        log.add_attribute("http.response.status_code", i64::from(status));
        if let Some(version) = header("x-amz-version-id") {
            log.add_attribute("s3.response.version_id", version.to_owned());
        }
        if op == "unrouted"
            && let Some(path) = &self.path
        {
            log.add_attribute("url.path", path.clone());
        }
        logger.emit(log);
    }
}

/// Returns the request ID shared by HTTP responses and audit records.
pub(crate) fn request_id() -> Uuid {
    REQUEST
        .try_with(|state| state.id)
        .unwrap_or_else(|_| Uuid::new_v4())
}

/// Emits one item outcome after the bulk delete transaction commits, including quiet requests.
pub(crate) fn audit_deletion(
    key: &str,
    version: Option<&str>,
    marker: Option<&str>,
    error: Option<&str>,
) {
    let Some(logger) = AUDIT_LOGGER.get() else {
        return;
    };
    let _ = REQUEST.try_with(|state| {
        let mut log = state.record(logger, "s3.delete_object.result", None);
        log.add_attribute("s3.key", key.to_owned());
        log.add_attribute(
            "s3.outcome",
            if error.is_some() {
                "failure"
            } else {
                "success"
            },
        );
        for (name, value) in [
            ("s3.version_id", version),
            ("s3.delete_marker.version_id", marker),
            ("error.type", error),
        ] {
            if let Some(value) = value {
                log.add_attribute(name, value.to_owned());
            }
        }
        logger.emit(log);
    });
}
