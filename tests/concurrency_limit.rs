//! Verifies `concurrency_limit` caps in-flight S3 requests across connections.

mod support;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use reqwest::{Method, StatusCode};
use support::{
    fixtures::{bucket_name, create_bucket},
    raw_http, sdk,
    server::{TestServer, TestServerOptions},
};
use tokio::time::timeout;

/// A request on a second connection waits while the only permit is held by an upload.
///
/// The upload pauses after its first chunk, so its request stays in flight. With
/// a per-connection limiter the second connection would be served immediately.
/// The same second request must then complete once the upload releases the
/// permit, which shows it was queued rather than refused.
#[tokio::test]
async fn concurrency_limit_is_shared_across_connections() -> Result<()> {
    let server = TestServer::spawn_with(TestServerOptions {
        config: Some("concurrency_limit: 1\n".to_string()),
        ..TestServerOptions::default()
    })
    .await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-limit");
    create_bucket(&client, &bucket).await?;

    let waiting = Arc::new(Mutex::new(None));
    let hook_waiting = waiting.clone();
    let blocked_client = client.clone();
    let response = raw_http::signed_streaming_request_after_first_chunk(
        &server,
        Method::PUT,
        &format!("/{bucket}/slow"),
        vec![b'a'; 64 * 1024],
        1024,
        &[],
        move || async move {
            // Give the server time to accept the upload and take the permit.
            // If it has not, the second request runs first and the check fails.
            tokio::time::sleep(Duration::from_millis(250)).await;
            let second = tokio::spawn(async move { blocked_client.list_buckets().send().await });
            tokio::time::sleep(Duration::from_millis(750)).await;
            ensure!(
                !second.is_finished(),
                "second connection was served while the only request permit was held"
            );
            *hook_waiting.lock().map_err(|_| anyhow!("poisoned"))? = Some(second);
            Ok(())
        },
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let second = waiting
        .lock()
        .map_err(|_| anyhow!("poisoned"))?
        .take()
        .ok_or_else(|| anyhow!("the second request was not started"))?;
    timeout(Duration::from_secs(5), second).await???;
    Ok(())
}
