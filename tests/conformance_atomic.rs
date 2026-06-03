//! Real-binary HTTP atomic read/write conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_atomic`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::Result;
use aws_sdk_s3::{Client, primitives::ByteStream};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::assert_status,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    raw_http, sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;
const STREAM_CHUNK: usize = 128 * 1024;
const KEY: &str = "testobj";

/// Verifies a 1 MiB GET sees one object snapshot while a later PUT wins.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_read_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_read_1mb
    test_atomic_read(MIB).await
}

/// Verifies a 4 MiB GET sees one object snapshot while a later PUT wins.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_read_4mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_read_4mb
    test_atomic_read(4 * MIB).await
}

/// Verifies an 8 MiB GET sees one object snapshot while a later PUT wins.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_read_8mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_read_8mb
    test_atomic_read(8 * MIB).await
}

/// Verifies a 1 MiB streaming PUT is not visible until it completes.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_write_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_write_1mb
    test_atomic_write(MIB).await
}

/// Verifies a 4 MiB streaming PUT is not visible until it completes.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_write_4mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_write_4mb
    test_atomic_write(4 * MIB).await
}

/// Verifies an 8 MiB streaming PUT is not visible until it completes.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_write_8mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_write_8mb
    test_atomic_write(8 * MIB).await
}

/// Verifies a 1 MiB interleaved streaming PUT finishes as one complete object.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_dual_write_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_dual_write_1mb
    test_atomic_dual_write(MIB).await
}

/// Verifies a 4 MiB interleaved streaming PUT finishes as one complete object.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_dual_write_4mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_dual_write_4mb
    test_atomic_dual_write(4 * MIB).await
}

/// Verifies an 8 MiB interleaved streaming PUT finishes as one complete object.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_dual_write_8mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_dual_write_8mb
    test_atomic_dual_write(8 * MIB).await
}

/// Verifies a conditional streaming PUT is not visible until it completes.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_conditional_write_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_conditional_write_1mb
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    put_repeated(&client, &bucket, KEY, MIB, b'A').await?;

    let hook_client = client.clone();
    let hook_bucket = bucket.clone();
    let response =
        streaming_put(
            &server,
            &bucket,
            KEY,
            repeated(MIB, b'B'),
            &[("If-Match", "*")],
            move || async move {
                assert_repeated_object(&hook_client, &hook_bucket, KEY, MIB, b'A').await
            },
        )
        .await?;
    assert_status(&response, StatusCode::OK);
    assert_repeated_object(&client, &bucket, KEY, MIB, b'B').await?;

    Ok(())
}

/// Verifies stale conditional streaming PUT loses to an interleaved writer.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_dual_conditional_write_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_dual_conditional_write_1mb
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    let etag = put_repeated(&client, &bucket, KEY, MIB, b'A').await?;
    assert_repeated_object(&client, &bucket, KEY, MIB, b'A').await?;

    let hook_client = client.clone();
    let hook_bucket = bucket.clone();
    let hook_etag = etag.clone();
    let response = streaming_put(
        &server,
        &bucket,
        KEY,
        repeated(MIB, b'C'),
        &[("If-Match", etag.as_str())],
        move || async move {
            hook_client
                .put_object()
                .bucket(&hook_bucket)
                .key(KEY)
                .if_match(hook_etag)
                .body(ByteStream::from(repeated(MIB, b'B')))
                .send()
                .await?;
            Ok(())
        },
    )
    .await?;
    assert_status(&response, StatusCode::PRECONDITION_FAILED);
    let body = response.text().await?;
    assert!(
        body.contains("PreconditionFailed"),
        "expected PreconditionFailed, got {body}"
    );
    assert_repeated_object(&client, &bucket, KEY, MIB, b'B').await?;

    Ok(())
}

/// Verifies deleting the bucket during a streaming PUT makes the PUT fail.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_write_bucket_gone() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_write_bucket_gone
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    let hook_client = client.clone();
    let hook_bucket = bucket.clone();
    let response = streaming_put(
        &server,
        &bucket,
        "foo",
        repeated(MIB, b'A'),
        &[],
        move || async move {
            hook_client
                .delete_bucket()
                .bucket(&hook_bucket)
                .send()
                .await?;
            Ok(())
        },
    )
    .await?;
    assert_status(&response, StatusCode::NOT_FOUND);
    let body = response.text().await?;
    assert!(
        body.contains("NoSuchBucket"),
        "expected NoSuchBucket, got {body}"
    );

    Ok(())
}

/// Verifies an initiated multipart upload does not overwrite an object.
#[tokio::test]
#[serial(conformance)]
async fn test_atomic_multipart_upload_write() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_atomic_multipart_upload_write
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;

    let upload_id = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string();
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("foo")
        .upload_id(upload_id)
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    Ok(())
}

async fn test_atomic_read(size: usize) -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    put_repeated(&client, &bucket, KEY, size, b'A').await?;

    let mut response =
        raw_http::signed_request(&server, Method::GET, &object_path(&bucket, KEY), Vec::new())
            .await?;
    assert_status(&response, StatusCode::OK);

    let mut body = Vec::with_capacity(size);
    let first = response
        .chunk()
        .await?
        .ok_or_else(|| anyhow::anyhow!("streaming GET returned no first chunk"))?;
    body.extend_from_slice(&first);

    put_repeated(&client, &bucket, KEY, size, b'B').await?;
    while let Some(chunk) = response.chunk().await? {
        body.extend_from_slice(&chunk);
    }
    assert_repeated_bytes(&body, size, b'A');
    assert_repeated_object(&client, &bucket, KEY, size, b'B').await?;

    Ok(())
}

async fn test_atomic_write(size: usize) -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    put_repeated(&client, &bucket, KEY, size, b'A').await?;

    let hook_client = client.clone();
    let hook_bucket = bucket.clone();
    let response =
        streaming_put(
            &server,
            &bucket,
            KEY,
            repeated(size, b'B'),
            &[],
            move || async move {
                assert_repeated_object(&hook_client, &hook_bucket, KEY, size, b'A').await
            },
        )
        .await?;
    assert_status(&response, StatusCode::OK);
    assert_repeated_object(&client, &bucket, KEY, size, b'B').await?;

    Ok(())
}

async fn test_atomic_dual_write(size: usize) -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-atomic");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, KEY, b"").await?;

    let hook_client = client.clone();
    let hook_bucket = bucket.clone();
    let response = streaming_put(
        &server,
        &bucket,
        KEY,
        repeated(size, b'B'),
        &[],
        move || async move {
            hook_client
                .put_object()
                .bucket(&hook_bucket)
                .key(KEY)
                .body(ByteStream::from(repeated(size, b'A')))
                .send()
                .await?;
            Ok(())
        },
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    assert_repeated_object(&client, &bucket, KEY, size, b'B').await?;

    Ok(())
}

async fn streaming_put<F, Fut>(
    server: &TestServer,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
    after_first_chunk: F,
) -> Result<reqwest::Response>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    raw_http::signed_streaming_request_after_first_chunk(
        server,
        Method::PUT,
        &object_path(bucket, key),
        body,
        STREAM_CHUNK,
        headers,
        after_first_chunk,
    )
    .await
}

async fn put_repeated(
    client: &Client,
    bucket: &str,
    key: &str,
    size: usize,
    byte: u8,
) -> Result<String> {
    Ok(client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(repeated(size, byte)))
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string())
}

async fn assert_repeated_object(
    client: &Client,
    bucket: &str,
    key: &str,
    size: usize,
    byte: u8,
) -> Result<()> {
    let body = get_object_bytes(client, bucket, key).await?;
    assert_repeated_bytes(&body, size, byte);
    Ok(())
}

fn assert_repeated_bytes(body: &[u8], size: usize, byte: u8) {
    assert_eq!(body.len(), size);
    assert!(
        body.iter().all(|value| *value == byte),
        "body was not all {} bytes",
        char::from(byte)
    );
}

fn repeated(size: usize, byte: u8) -> Vec<u8> {
    vec![byte; size]
}

fn object_path(bucket: &str, key: &str) -> String {
    format!(
        "/{}/{}",
        raw_http::path_segment(bucket),
        raw_http::path_segment(key)
    )
}
