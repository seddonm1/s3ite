//! Runtime-configured OpenTelemetry traces, operational metrics and audit logs.

mod audit;
mod metrics;

use std::{env, net::IpAddr, time::Instant};

use hyper::{Request, Response, http::HeaderValue};
use opentelemetry::{
    Context, global,
    logs::LoggerProvider as _,
    propagation::TextMapCompositePropagator,
    trace::{TraceContextExt, TracerProvider as _},
};
use opentelemetry_http::HeaderExtractor;
use opentelemetry_otlp::{LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::SdkMeterProvider,
    propagation::{BaggagePropagator, TraceContextPropagator},
    trace::SdkTracerProvider,
};
use tower::{Service, ServiceExt};
use tracing::{Instrument, Span, field::Empty, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{EnvFilter, filter::filter_fn, fmt, fmt::format::FmtSpan, prelude::*};

use self::audit::{AUDIT_LOGGER, CaptureOperation, REQUEST, RequestContext};
use self::metrics::METRICS;
pub(crate) use self::{
    audit::{audit_deletion, request_id},
    metrics::{kms_timer, writer_wait_timer},
};
use crate::{Config, MainError, MainResult};

/// Keeps the exporters alive until all connections drain.
pub(crate) struct Telemetry {
    traces: Option<SdkTracerProvider>,
    metrics: Option<SdkMeterProvider>,
    logs: Option<SdkLoggerProvider>,
    audit_reads: bool,
}

impl Drop for Telemetry {
    /// Flushes all active signals, including the final metrics collection.
    fn drop(&mut self) {
        for result in [
            self.logs.as_ref().map(SdkLoggerProvider::shutdown),
            self.metrics.as_ref().map(SdkMeterProvider::shutdown),
            self.traces.as_ref().map(SdkTracerProvider::shutdown),
        ]
        .into_iter()
        .flatten()
        {
            if let Err(error) = result {
                warn!(%error, "OTLP shutdown failed");
            }
        }
    }
}

/// Resolves exporter enablement independently for each signal.
fn export_enabled(signal: &str, endpoint: Option<&str>) -> bool {
    if env::var("OTEL_SDK_DISABLED").is_ok_and(|v| v.eq_ignore_ascii_case("true")) {
        return false;
    }
    match env::var(format!("OTEL_{signal}_EXPORTER")) {
        Ok(names) => names.split(',').any(|name| name.trim() == "otlp"),
        Err(_) => {
            endpoint.is_some()
                || env::var_os(format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT")).is_some()
                || env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        }
    }
}

/// Resolves the signal-specific HTTP protocol before the shared fallback.
fn protocol(signal: &str, label: &str) -> MainResult<Protocol> {
    let value = env::var(format!("OTEL_EXPORTER_OTLP_{signal}_PROTOCOL"))
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok());
    match value.as_deref() {
        Some("http/json") => Ok(Protocol::HttpJson),
        None | Some("" | "http/protobuf") => Ok(Protocol::HttpBinary),
        Some(other) => Err(MainError::Configuration(format!(
            "unsupported OTLP {label} protocol {other:?}, expected http/protobuf or http/json"
        ))),
    }
}

/// Builds each typed OTLP exporter using the same configuration precedence.
macro_rules! exporter {
    ($kind:ty,$signal:literal,$label:literal,$endpoint:expr) => {{
        let endpoint = $endpoint.as_deref();
        if export_enabled($signal, endpoint) {
            let mut builder = <$kind>::builder()
                .with_http()
                .with_protocol(protocol($signal, $label)?);
            if let Some(url) = endpoint {
                builder = builder.with_endpoint(url);
            }
            Some(
                builder
                    .build()
                    .map_err(|e| MainError::Configuration(e.to_string()))?,
            )
        } else {
            None
        }
    }};
}

/// Installs the signal providers and local logging before accepting requests.
pub(crate) fn init(config: &Config) -> MainResult<Telemetry> {
    let resource = Resource::builder()
        .with_service_name(env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "s3ite".into()))
        .build();
    let traces =
        exporter!(SpanExporter, "TRACES", "trace", config.otlp_traces_endpoint).map(|exporter| {
            SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(exporter)
                .build()
        });
    let metrics = exporter!(
        MetricExporter,
        "METRICS",
        "metrics",
        config.otlp_metrics_endpoint
    )
    .map(|exporter| {
        SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_periodic_exporter(exporter)
            .build()
    });
    let logs = exporter!(LogExporter, "LOGS", "logs", config.otlp_logs_endpoint).map(|exporter| {
        SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build()
    });
    if let Some(provider) = &metrics {
        metrics::install(provider, &config.root);
    }
    if let Some(provider) = &logs {
        let _ = AUDIT_LOGGER.set(provider.logger("s3ite.audit"));
    }
    if traces.is_some() || logs.is_some() {
        global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));
    }
    let otlp = traces.as_ref().map(|provider| {
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("s3ite"))
            .with_filter(filter_fn(|meta| {
                meta.is_span() && meta.target().starts_with("s3ite::")
            }))
    });
    let operations = (metrics.is_some() || logs.is_some()).then(|| {
        CaptureOperation.with_filter(filter_fn(|m| m.is_span() && m.target() == "s3ite::s3"))
    });
    tracing_subscriber::registry()
        .with(operations)
        .with(otlp)
        .with(
            fmt::layer()
                .with_span_events(FmtSpan::CLOSE)
                .with_filter(EnvFilter::from_default_env()),
        )
        .init();
    Ok(Telemetry {
        traces,
        metrics,
        logs,
        audit_reads: config.audit_reads,
    })
}

/// Copyable settings for the connection's HTTP request observer.
#[derive(Clone, Copy)]
pub(crate) struct Observer {
    trace: bool,
    operations: bool,
    audit_reads: bool,
}

impl Telemetry {
    /// Selects only the work required by the configured signals.
    pub(crate) fn observer(&self) -> Observer {
        Observer {
            trace: self.traces.is_some(),
            operations: self.logs.is_some() || self.metrics.is_some(),
            audit_reads: self.audit_reads,
        }
    }
}

impl Observer {
    /// Observes the complete HTTP dispatch, including authentication and routing errors.
    pub(crate) async fn call<S, B, R>(
        self,
        svc: S,
        req: Request<B>,
        ip: IpAddr,
    ) -> Result<Response<R>, S::Error>
    where
        S: Service<Request<B>, Response = Response<R>>,
    {
        if !self.operations && !self.trace {
            return svc.oneshot(req).await;
        }
        let parent = global::get_text_map_propagator(|p| {
            p.extract_with_context(&Context::new(), &HeaderExtractor(req.headers()))
        });
        let span = if self.trace {
            let method = req.method().as_str();
            let span = info_span!(parent: None, "http.request",
                otel.name = method, otel.kind = "server",
                http.request.method = method, http.response.status_code = Empty,
                otel.status_code = Empty,
            );
            let _ = span.set_parent(parent.clone());
            span
        } else {
            Span::none()
        };
        let state = self.operations.then(|| {
            RequestContext::new(
                &req,
                ip,
                if self.trace {
                    span.context().span().span_context().clone()
                } else {
                    parent.span().span_context().clone()
                },
            )
        });
        let started = Instant::now();
        let dispatch = async {
            let mut result = svc.oneshot(req).await;
            let status = result.as_ref().map_or(500, |r| r.status().as_u16());
            span.record("http.response.status_code", i64::from(status));
            if status >= 500 {
                span.record("otel.status_code", "ERROR");
            }
            let _ = REQUEST.try_with(|state| {
                if let Some(metrics) = METRICS.get() {
                    metrics.request(state.operation.get(), status, started.elapsed());
                }
                if let Ok(response) = &mut result
                    && let Ok(value) = HeaderValue::from_str(&state.id.simple().to_string())
                {
                    response
                        .headers_mut()
                        .entry("x-amz-request-id")
                        .or_insert(value);
                }
                state.completed(
                    status,
                    self.audit_reads,
                    result.as_ref().ok().map(Response::headers),
                );
            });
            result
        }
        .instrument(span.clone());
        if let Some(state) = state {
            REQUEST.scope(state, dispatch).await
        } else {
            dispatch.await
        }
    }
}
