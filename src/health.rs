//! Lightweight process health probes on a dedicated HTTP listener.

use std::{
    convert::Infallible,
    future::ready,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use http_body_util::Empty;
use hyper::{
    Method, Request, Response, StatusCode,
    header::{ALLOW, CACHE_CONTROL, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;

use crate::accept_connection;

/// Shared readiness flag, initially false while the S3 server initializes.
#[derive(Clone, Default)]
pub(crate) struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// Publishes whether the S3 listener is accepting traffic.
    pub(crate) fn set(&self, accepting: bool) {
        self.0.store(accepting, Ordering::Relaxed);
    }

    /// Answers probe requests without touching SQLite, KMS, or the S3 request queue.
    fn response<B>(&self, request: &Request<B>) -> Response<Empty<Bytes>> {
        let mut response = Response::new(Empty::new());
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        *response.status_mut() = match request.uri().path() {
            "/livez" | "/readyz" if !matches!(*request.method(), Method::GET | Method::HEAD) => {
                response
                    .headers_mut()
                    .insert(ALLOW, HeaderValue::from_static("GET, HEAD"));
                StatusCode::METHOD_NOT_ALLOWED
            }
            "/readyz" if !self.0.load(Ordering::Relaxed) => StatusCode::SERVICE_UNAVAILABLE,
            "/readyz" | "/livez" => StatusCode::OK,
            _ => StatusCode::NOT_FOUND,
        };
        response
    }

    /// Serves independent HTTP/1 probes until the main server finishes draining.
    pub(crate) async fn serve(&self, listener: TcpListener) {
        loop {
            let (stream, _) = accept_connection(&listener).await;
            let state = self.clone();
            tokio::spawn(async move {
                let handler =
                    service_fn(move |req| ready(Ok::<_, Infallible>(state.response(&req))));
                let _ = http1::Builder::new()
                    .timer(TokioTimer::new())
                    .keep_alive(false)
                    .serve_connection(TokioIo::new(stream), handler)
                    .await;
            });
        }
    }
}
