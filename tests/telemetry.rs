//! Checks traces, operational metrics and audit records exported over OTLP/HTTP.
mod support;

use std::{process::Command, sync::OnceLock, time::Duration};

use anyhow::{Context, Result, ensure};
use aws_sdk_s3::types::BucketVersioningStatus;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use reqwest::{Method, StatusCode};
use serde_json::Value;
use support::{
    fixtures::{create_bucket, put_bucket_versioning},
    sdk,
};
use support::{
    raw_http::signed_request_with_headers,
    server::{TestServer, TestServerOptions},
};
use tokio::{net::TcpListener, task::spawn_blocking, time::timeout};

/// Receives the batch flushed on shutdown using the collector's trace endpoint.
async fn receive_batch(listener: TcpListener, path: &'static str) -> Result<Value> {
    let captured = OnceLock::new();
    let handler = service_fn(async |request: Request<Incoming>| -> Result<_> {
        ensure!(request.method() == Method::POST && request.uri().path() == path);
        ensure!(request.headers()["content-type"] == "application/json");
        let bytes = request.into_body().collect().await?.to_bytes();
        captured
            .set(serde_json::from_slice(&bytes)?)
            .expect("one export batch");
        Ok(Response::new(Full::new(Bytes::from_static(b"{}"))))
    });
    let (stream, _) = listener.accept().await?;
    http1::Builder::new()
        .keep_alive(false)
        .serve_connection(TokioIo::new(stream), handler)
        .await?;
    captured.into_inner().context("missing OTLP batch")
}

/// Confirms remote parentage, sampling, and early HTTP errors with console logging disabled.
#[tokio::test]
async fn exports_requests_and_s3_children_in_the_callers_trace() -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let endpoint = format!("http://{}/v1/traces", listener.local_addr()?);
    let collector = tokio::spawn(receive_batch(listener, "/v1/traces"));
    let server = TestServer::spawn_with(TestServerOptions {
        config: Some(format!("otlp_traces_endpoint: {endpoint}")),
        rust_log: Some("off".to_owned()),
        env: vec![
            ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json".to_owned()),
            ("OTEL_BSP_SCHEDULE_DELAY", "600000".to_owned()),
        ],
        ..TestServerOptions::default()
    })
    .await?;
    let trace = "112233445566778899aabbccddeeff00";
    let parent = "1122334455667788";
    for (sampled, bucket) in [("01", "sampled"), ("00", "unsampled")] {
        let traceparent = format!("00-{trace}-{parent}-{sampled}");
        let response = signed_request_with_headers(
            &server,
            Method::PUT,
            &format!("/{bucket}"),
            Vec::new(),
            &[
                ("traceparent", &traceparent),
                ("tracestate", "vendor=abc"),
                ("baggage", "tenant=example"),
            ],
        )
        .await?;
        ensure!(response.status().is_success());
    }
    let unsupported =
        signed_request_with_headers(&server, Method::GET, "/sampled?website", Vec::new(), &[])
            .await?;
    assert_eq!(unsupported.status(), StatusCode::NOT_IMPLEMENTED);
    let rejected = reqwest::Client::new()
        .head(format!("{}/sampled", server.endpoint()))
        .header("traceparent", "invalid")
        .send()
        .await?;
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
    let status = rejected.status().as_u16().to_string();
    spawn_blocking(move || server.shutdown_and_read_log()).await??;
    let batch = timeout(Duration::from_secs(10), collector).await???;
    let resource = &batch["resourceSpans"][0];
    assert!(
        resource["resource"]["attributes"]
            .as_array()
            .context("missing resource")?
            .iter()
            .any(|a| a["key"] == "service.name" && a["value"]["stringValue"] == "s3ite")
    );
    let spans = resource["scopeSpans"][0]["spans"]
        .as_array()
        .context("missing spans")?;
    let find = |name| spans.iter().find(|s| s["name"] == name);
    let put = find("PUT").context("missing server span")?;
    assert_eq!(put["traceId"], trace);
    assert_eq!(put["parentSpanId"], parent);
    assert_eq!(put["traceState"], "vendor=abc");
    assert_eq!(put["kind"], 2);
    let create = find("create_bucket").context("missing S3 child span")?;
    assert_eq!(create["parentSpanId"], put["spanId"]);
    assert_eq!(create["traceId"], trace);
    let get = find("HEAD").context("missing rejected request")?;
    assert_ne!(get["traceId"], trace);
    assert!(
        get["attributes"]
            .as_array()
            .context("missing attributes")?
            .iter()
            .any(|a| a["key"] == "http.response.status_code" && a["value"]["intValue"] == status),
        "{get}"
    );
    let failed = find("GET").context("missing failed request")?;
    assert_eq!(
        failed["status"]["code"], 2,
        "5xx must mark the span as an error"
    );
    assert_eq!(spans.len(), 4, "unsampled requests must not be exported");
    Ok(())
}

/// Both runtime off switches suppress export even when an unusable collector is configured.
#[tokio::test]
async fn disabling_export_skips_collector_setup() -> Result<()> {
    for (name, value) in [
        ("OTEL_TRACES_EXPORTER", "none"),
        ("OTEL_SDK_DISABLED", "true"),
    ] {
        let server = TestServer::spawn_with(TestServerOptions {
            config: Some("otlp_traces_endpoint: 'invalid address'".to_owned()),
            env: vec![(name, value.to_owned())],
            ..TestServerOptions::default()
        })
        .await?;
        let response =
            signed_request_with_headers(&server, Method::GET, "/", Vec::new(), &[]).await?;
        ensure!(response.status().is_success());
    }
    Ok(())
}

/// Checks startup errors for collector URLs supplied through either config or the environment.
#[test]
fn invalid_export_endpoint_fails_startup() -> Result<()> {
    for cli in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_s3ite"));
        command
            .env_remove("OTEL_SDK_DISABLED")
            .env_remove("OTEL_TRACES_EXPORTER");
        if cli {
            command.arg("--otlp-traces-endpoint").arg("invalid address");
        } else {
            command.env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "invalid address");
        }
        let result = command.output()?;
        ensure!(!result.status.success());
        ensure!(String::from_utf8_lossy(&result.stderr).contains("endpoint"));
    }
    Ok(())
}

/// A configured but unsupported collector protocol must produce an actionable startup error.
#[test]
fn unsupported_export_protocol_fails_startup() -> Result<()> {
    let result = Command::new(env!("CARGO_BIN_EXE_s3ite"))
        .env_remove("OTEL_SDK_DISABLED")
        .env("OTEL_TRACES_EXPORTER", "otlp")
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        .output()?;
    ensure!(!result.status.success());
    ensure!(String::from_utf8_lossy(&result.stderr).contains("unsupported OTLP trace protocol"));
    Ok(())
}

/// Returns the JSON oneof value corresponding to an OTLP attribute.
fn attribute<'a>(item: &'a Value, name: &str) -> Option<&'a Value> {
    item["attributes"]
        .as_array()?
        .iter()
        .find(|a| a["key"] == name)
        .map(|a| &a["value"])
}

/// Exercises request and per-item auditing with trace export disabled and an unsampled parent.
#[tokio::test]
async fn exports_audit_and_metrics_independently_of_traces() -> Result<()> {
    let logs = TcpListener::bind(("127.0.0.1", 0)).await?;
    let metrics = TcpListener::bind(("127.0.0.1", 0)).await?;
    let config = format!(
        "otlp_logs_endpoint: http://{}/v1/logs\notlp_metrics_endpoint: http://{}/v1/metrics\nkms_region: us-east-1",
        logs.local_addr()?,
        metrics.local_addr()?
    );
    let logs = tokio::spawn(receive_batch(logs, "/v1/logs"));
    let metrics = tokio::spawn(receive_batch(metrics, "/v1/metrics"));
    let server = TestServer::spawn_with(TestServerOptions {
        config: Some(config),
        rust_log: Some("off".into()),
        env: vec![
            ("OTEL_TRACES_EXPORTER", "none".into()),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json".into()),
            ("OTEL_METRIC_EXPORT_INTERVAL", "600000".into()),
            ("OTEL_BLRP_SCHEDULE_DELAY", "600000".into()),
            ("AWS_ENDPOINT_URL_KMS", "http://127.0.0.1:0".into()),
            ("AWS_ACCESS_KEY_ID", "test".into()),
            ("AWS_SECRET_ACCESS_KEY", "test".into()),
            ("AWS_MAX_ATTEMPTS", "1".into()),
        ],
        ..Default::default()
    })
    .await?;
    let parent = "00-112233445566778899aabbccddeeff00-1122334455667788-00";
    let client = sdk::client(&server);
    create_bucket(&client, "audit-test").await?;
    put_bucket_versioning(&client, "audit-test", BucketVersioningStatus::Enabled).await?;
    let put = |key| client.put_object().bucket("audit-test").key(key);
    let removed = put("remove")
        .send()
        .await?
        .version_id
        .context("missing version")?;
    let kept = put("keep")
        .send()
        .await?
        .version_id
        .context("missing version")?;
    let res =
        signed_request_with_headers(&server, Method::GET, "/audit-test/keep", vec![], &[]).await?;
    ensure!(res.status().is_success());
    let body = format!(
        r#"<Delete><Object><Key>remove</Key><VersionId>{removed}</VersionId></Object><Object><Key>keep</Key><VersionId>{kept}</VersionId><ETag>wrong</ETag></Object><Quiet>true</Quiet></Delete>"#
    );
    let response = signed_request_with_headers(
        &server,
        Method::POST,
        "/audit-test?delete",
        body.into_bytes(),
        &[
            ("traceparent", parent),
            ("x-amz-bypass-governance-retention", "true"),
        ],
    )
    .await?;
    let id = response.headers()["x-amz-request-id"].to_str()?.to_owned();
    ensure!(response.status().is_success());
    ensure!(response.text().await?.contains("PreconditionFailed"));
    let denied = reqwest::Client::new()
        .head(format!(
            "{}/audit-test/private?never-export-this",
            server.endpoint()
        ))
        .send()
        .await?;
    ensure!(denied.status() == StatusCode::FORBIDDEN);
    let unavailable = signed_request_with_headers(
        &server,
        Method::PUT,
        "/audit-test/protected",
        vec![],
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            (
                "x-amz-server-side-encryption-aws-kms-key-id",
                "key-label-must-stay-private",
            ),
        ],
    )
    .await?;
    ensure!(unavailable.status().is_server_error());
    spawn_blocking(move || server.shutdown_and_read_log()).await??;
    let logs = timeout(Duration::from_secs(10), logs).await???;
    let records = logs["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array()
        .context("no audit records")?;
    let items: Vec<_> = records
        .iter()
        .filter(|r| r["eventName"] == "s3.delete_object.result")
        .collect();
    assert_eq!(items.len(), 2);
    for record in items {
        assert_eq!(record["traceId"], &parent[3..35]);
        assert_eq!(
            attribute(record, "s3.request.id").unwrap()["stringValue"],
            id
        );
        assert_eq!(
            attribute(record, "s3.principal").unwrap()["stringValue"],
            "s3ite-access-key"
        );
        assert_eq!(
            attribute(record, "s3.bucket").unwrap()["stringValue"],
            "audit-test"
        );
        assert_eq!(
            attribute(record, "s3.governance_bypass_requested").unwrap()["boolValue"],
            true
        );
        let key = &attribute(record, "s3.key").unwrap()["stringValue"];
        assert_eq!(
            attribute(record, "s3.version_id").unwrap()["stringValue"],
            if key == "keep" {
                kept.as_str()
            } else {
                removed.as_str()
            }
        );
        if key == "keep" {
            assert_eq!(
                attribute(record, "error.type").unwrap()["stringValue"],
                "PreconditionFailed"
            );
        } else {
            assert_eq!(key, "remove");
            assert_eq!(
                attribute(record, "s3.outcome").unwrap()["stringValue"],
                "success"
            );
        }
    }
    let encoded = logs.to_string();
    assert!(
        !encoded.contains("never-export-this")
            && !encoded.contains("s3ite-secret-key")
            && !encoded.contains("get_object")
    );
    assert!(records.iter().any(|r| {
        attribute(r, "http.response.status_code").is_some_and(|v| v["intValue"] == "403")
            && attribute(r, "s3.principal").is_none()
    }));
    let metrics = timeout(Duration::from_secs(10), metrics).await???;
    let names = metrics["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        .as_array()
        .context("missing metrics")?;
    for name in [
        "s3ite.request.duration",
        "s3ite.kms.request.duration",
        "s3ite.sqlite.writer.queue.duration",
    ] {
        assert!(names.iter().any(|m| m["name"] == name), "{name} missing");
    }
    #[cfg(unix)]
    assert!(names.iter().any(|m| m["name"] == "s3ite.storage.available"));
    let encoded = metrics.to_string();
    assert!(
        encoded.contains("delete_objects")
            && encoded.contains("403")
            && encoded.contains("GenerateDataKey")
    );
    assert!(!encoded.contains("audit-test") && !encoded.contains("key-label-must-stay-private"));
    Ok(())
}

/// Checks successful read auditing and request correlation with tracing enabled.
#[tokio::test]
async fn audit_reads_follow_config_and_share_unsampled_trace_identity() -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let base = format!("http://{}", listener.local_addr()?);
    let collector = tokio::spawn(receive_batch(listener, "/v1/logs"));
    let server = TestServer::spawn_with(TestServerOptions {
        config: Some("audit_reads: true".into()),
        env: vec![
            ("OTEL_EXPORTER_OTLP_ENDPOINT", base),
            ("OTEL_METRICS_EXPORTER", "none".into()),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json".into()),
            ("OTEL_TRACES_SAMPLER", "always_off".into()),
        ],
        ..Default::default()
    })
    .await?;
    let parent = "00-112233445566778899aabbccddeeff00-1122334455667788-00";
    let response = signed_request_with_headers(
        &server,
        Method::GET,
        "/",
        vec![],
        &[("traceparent", parent)],
    )
    .await?;
    assert!(response.status().is_success());
    let id = response.headers()["x-amz-request-id"].to_str()?.to_owned();
    spawn_blocking(move || server.shutdown_and_read_log()).await??;
    let batch = timeout(Duration::from_secs(10), collector).await???;
    let record = &batch["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
    assert_eq!(record["traceId"], &parent[3..35]);
    assert_eq!(
        attribute(record, "s3.request.id").unwrap()["stringValue"],
        id
    );
    assert_eq!(
        attribute(record, "s3.operation").unwrap()["stringValue"],
        "list_buckets"
    );
    Ok(())
}
