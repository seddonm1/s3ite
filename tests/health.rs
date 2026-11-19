//! Exercises startup and graceful drain via the independent process health listener.
#![cfg(unix)]

mod support;

use std::{
    net::TcpListener,
    process::{Child, Command},
    time::Duration,
};

use anyhow::{Result, ensure};
use reqwest::{Client, Method, StatusCode};
use rusqlite::Connection;
use tempfile::tempdir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::{sleep, timeout},
};

use support::{raw_http::presigned_url_with_credentials, server::s3ite_binary};

/// Credentials the test server is started with and its S3 requests are signed with.
const CREDENTIALS: (&str, &str) = ("test-access", "test-secret");

/// Reaps the spawned process even if a probe assertion fails.
struct Server(Child);
impl Drop for Server {
    /// Stops the isolated test server and releases its temporary database files.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Polls for a lifecycle transition with a bounded deadline.
async fn await_status(client: &Client, url: &str, expected: StatusCode) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(url)
                .send()
                .await
                .is_ok_and(|response| response.status() == expected)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

/// Keeps initialization and a final PUT open to observe both unavailable readiness states.
#[tokio::test]
async fn probes_follow_storage_startup_and_graceful_drain() -> Result<()> {
    let directory = tempdir()?;
    let lock = Connection::open(directory.path().join("readyz.sqlite3"))?;
    lock.execute_batch("PRAGMA journal_mode=WAL; BEGIN IMMEDIATE;")?;
    let reservations = [
        TcpListener::bind("127.0.0.1:0")?,
        TcpListener::bind("127.0.0.1:0")?,
    ];
    let s3 = reservations[0].local_addr()?;
    let health = reservations[1].local_addr()?;
    drop(reservations);
    let mut server = Server(
        Command::new(s3ite_binary()?)
            .arg("--root")
            .arg(directory.path())
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &s3.port().to_string(),
                "--health-bind",
                &health.to_string(),
                "--maintenance-interval-seconds",
                "0",
                "--busy-timeout-milliseconds",
                "10000",
                "--access-key",
                CREDENTIALS.0,
                "--secret-key",
                CREDENTIALS.1,
            ])
            .env("OTEL_SDK_DISABLED", "true")
            .spawn()?,
    );
    let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
    let ready = format!("http://{health}/readyz");
    let live = format!("http://{health}/livez");
    await_status(&client, &ready, StatusCode::SERVICE_UNAVAILABLE).await?;
    assert_eq!(client.head(&live).send().await?.status(), StatusCode::OK);
    lock.execute_batch("COMMIT;")?;
    await_status(&client, &ready, StatusCode::OK).await?;
    let status = client.get(&live).send().await?;
    assert_eq!(status.headers()["cache-control"], "no-store");
    assert!(status.bytes().await?.is_empty());
    let wrong = client.post(&ready).send().await?;
    assert_eq!(wrong.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(wrong.headers()["allow"], "GET, HEAD");
    assert_eq!(
        client
            .get(format!("http://{health}/objects"))
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let endpoint = format!("http://{s3}");
    let listing_url =
        presigned_url_with_credentials(&endpoint, CREDENTIALS, Method::GET, "/readyz")?;
    let listing = client.get(listing_url).send().await?;
    assert!(listing.text().await?.contains("ListBucketResult"));

    let upload_url =
        presigned_url_with_credentials(&endpoint, CREDENTIALS, Method::PUT, "/readyz/draining")?;
    let upload_path = upload_url
        .strip_prefix(&endpoint)
        .ok_or_else(|| anyhow::anyhow!("presigned URL should start with the endpoint"))?;
    let mut upload = BufReader::new(TcpStream::connect(s3).await?);
    let headers = format!(
        "PUT {upload_path} HTTP/1.1\r\nHost: {s3}\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"
    );
    upload.get_mut().write_all(headers.as_bytes()).await?;
    let mut interim = String::new();
    timeout(Duration::from_secs(3), upload.read_line(&mut interim)).await??;
    assert!(interim.starts_with("HTTP/1.1 100"), "{interim}");
    ensure!(
        Command::new("kill")
            .args(["-TERM", &server.0.id().to_string()])
            .status()?
            .success()
    );
    await_status(&client, &ready, StatusCode::SERVICE_UNAVAILABLE).await?;
    assert_eq!(client.get(&live).send().await?.status(), StatusCode::OK);
    // During the SIGTERM grace window the listener still accepts new requests,
    // so traffic routed before Kubernetes sees the 503 is served.
    let listing_url =
        presigned_url_with_credentials(&endpoint, CREDENTIALS, Method::GET, "/readyz")?;
    let during_grace = client.get(listing_url).send().await?;
    assert_eq!(during_grace.status(), StatusCode::OK);
    upload.get_mut().write_all(b"x").await?;
    let mut result = String::new();
    timeout(Duration::from_secs(5), upload.read_to_string(&mut result)).await??;
    assert!(result.contains("HTTP/1.1 200"), "{result}");
    let status = server.0.wait()?;
    ensure!(status.success(), "server exited with {status}");
    Ok(())
}
