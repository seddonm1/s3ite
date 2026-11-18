//! Low-cardinality operational metrics for the S3 server and its dependencies.
use opentelemetry::{
    KeyValue,
    metrics::{Histogram, MeterProvider},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;
#[cfg(unix)]
use rustix::fs::statvfs;
use std::{
    path::Path,
    sync::OnceLock,
    time::{Duration, Instant},
};

pub(super) static METRICS: OnceLock<Metrics> = OnceLock::new();

pub(super) struct Metrics {
    requests: Histogram<f64>,
    writer_wait: Histogram<f64>,
    kms: Histogram<f64>,
}

/// Registers operational instruments with the periodic reader.
pub(super) fn install(provider: &SdkMeterProvider, root: &Path) {
    let meter = provider.meter("s3ite");
    #[cfg(unix)]
    {
        let path = root.to_owned();
        meter
            .u64_observable_gauge("s3ite.storage.available")
            .with_unit("By")
            .with_description("Space available to the service on the storage filesystem")
            .with_callback(move |observer| {
                if let Ok(fs) = statvfs(&path) {
                    observer.observe(fs.f_bavail.saturating_mul(fs.f_frsize), &[]);
                }
            })
            .build();
    }
    #[cfg(not(unix))]
    let _ = root;
    let seconds = |name| meter.f64_histogram(name).with_unit("s").build();
    let _ = METRICS.set(Metrics {
        requests: seconds("s3ite.request.duration"),
        writer_wait: seconds("s3ite.sqlite.writer.queue.duration"),
        kms: seconds("s3ite.kms.request.duration"),
    });
}

impl Metrics {
    /// Records request count and time to response headers by operation and status.
    pub(super) fn request(&self, operation: &'static str, status: u16, elapsed: Duration) {
        self.requests.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("s3.operation", operation),
                KeyValue::new("http.response.status_code", i64::from(status)),
            ],
        );
    }
}

/// Starts timing only when metrics are enabled and records when the writer dequeues the job.
pub(crate) fn writer_wait_timer() -> impl FnOnce() + Send + 'static {
    let timer = METRICS.get().map(|metrics| (metrics, Instant::now()));
    move || {
        if let Some((metrics, start)) = timer {
            metrics
                .writer_wait
                .record(start.elapsed().as_secs_f64(), &[]);
        }
    }
}

/// Times one SDK call including retries and records its outcome without exposing key IDs.
pub(crate) fn kms_timer(operation: &'static str) -> impl FnOnce(bool) {
    let timer = METRICS.get().map(|metrics| (metrics, Instant::now()));
    move |success| {
        if let Some((metrics, start)) = timer {
            metrics.kms.record(
                start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("kms.operation", operation),
                    KeyValue::new("success", success),
                ],
            );
        }
    }
}
