//! Real-binary logging side-effect tests.

mod support;

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use serial_test::serial;
use support::{
    fixtures::{bucket_name, create_bucket},
    sdk,
    server::{TestServer, TestServerOptions},
};

const LOG_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[tokio::test]
#[serial(conformance)]
async fn server_startup_is_logged() -> Result<()> {
    let server = logged_server().await?;
    let log = wait_for_log_contains(&server, "server is running at http://").await?;

    assert!(log.contains(server.endpoint()));
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn graceful_shutdown_is_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log");

    create_bucket(&client, &bucket).await?;
    let log = wait_for_log_contains(&server, "spawn writer").await?;
    assert!(log.contains("spawn reader"));
    assert!(log.contains("spawn blob reader"));

    let log = server.shutdown_and_read_log()?;
    assert!(log.contains("Gracefully shutdown!"));
    assert!(log.contains("server is stopped"));
    assert!(log.contains("close writer"));
    assert!(log.contains("close reader"));
    assert!(log.contains("close blob reader"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn bucket_creation_and_put_object_are_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log");

    create_bucket(&client, &bucket).await?;
    let log = wait_for_log_contains_all(
        &server,
        &[
            "created bucket",
            &format!("create_bucket{{bucket={bucket}}}: s3ite::s3: close"),
            "time.busy=",
            "time.idle=",
        ],
    )
    .await?;
    assert!(log.contains(&bucket));

    client
        .put_object()
        .bucket(&bucket)
        .key("logged-object")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    let log = wait_for_log_contains_all(
        &server,
        &[
            "write file",
            &format!(
                "put_object{{bucket={bucket} key=logged-object content_length=Some(4)}}: s3ite::s3: close"
            ),
        ],
    )
    .await?;
    assert!(log.contains("logged-object"));
    assert!(log.contains("size=4"));
    assert!(log.contains("841a2d689ad86bd1611447453c22c6fc"));

    client
        .get_object()
        .bucket(&bucket)
        .key("logged-object")
        .send()
        .await?;
    let log = wait_for_log_contains_all(
        &server,
        &[
            &format!("get_object{{bucket={bucket} key=logged-object"),
            "version_id= part_number=None range=None}: s3ite::s3: close",
        ],
    )
    .await?;
    assert!(!log.contains("secret-key-for-tests"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn multipart_completion_is_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log");
    let key = "logged-multipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let part = upload_part(&client, &bucket, key, &upload_id, 1, b"part-body").await?;
    complete_upload(&client, &bucket, key, &upload_id, vec![part]).await?;

    let log = wait_for_log_contains_all(
        &server,
        &[
            "completed multipart upload",
            &format!(
                "complete_multipart_upload{{bucket={bucket} key={key} upload_id={upload_id}}}: s3ite::s3: close"
            ),
        ],
    )
    .await?;
    assert!(log.contains("version_id=null"));

    Ok(())
}

async fn logged_server() -> Result<TestServer> {
    TestServer::spawn_with(TestServerOptions {
        rust_log: Some("s3ite=debug,s3s=warn".to_string()),
        ..Default::default()
    })
    .await
}

async fn wait_for_log_contains(server: &TestServer, needle: &str) -> Result<String> {
    wait_for_log_contains_all(server, &[needle]).await
}

async fn wait_for_log_contains_all(server: &TestServer, needles: &[&str]) -> Result<String> {
    let deadline = Instant::now() + LOG_TIMEOUT;
    loop {
        let log = tokio::fs::read_to_string(server.log_path())
            .await
            .with_context(|| format!("read {}", server.log_path().display()))?;
        if needles.iter().all(|needle| log.contains(needle)) {
            return Ok(log);
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for log entries {needles:?}; current log:\n{log}");
        }
        tokio::time::sleep(LOG_POLL_INTERVAL).await;
    }
}

async fn create_upload(client: &Client, bucket: &str, key: &str) -> Result<String> {
    Ok(client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .context("create multipart response missing upload ID")?
        .to_string())
}

async fn upload_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: &'static [u8],
) -> Result<CompletedPart> {
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(
            response
                .e_tag()
                .context("upload part response missing ETag")?,
        )
        .build())
}

async fn complete_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> Result<()> {
    let multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(multipart_upload)
        .send()
        .await?;
    Ok(())
}
