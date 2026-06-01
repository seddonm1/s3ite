//! Real-binary HTTP object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_object`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    sdk,
    server::TestServer,
};

/// Verifies HeadObject reports zero content length for an empty object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_head_zero_bytes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_head_zero_bytes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"").await?;

    let response = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    assert_eq!(response.content_length(), Some(0));

    Ok(())
}

/// Verifies PutObject returns the MD5-based ETag for a small object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_check_etag() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_check_etag
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    let response = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    assert_eq!(
        response.e_tag().map(|etag| etag.trim_matches('"')),
        Some("37b51d194a7513e45b56f6524f2d51f2")
    );

    Ok(())
}

/// Verifies object writes, overwrites, reads, and deletes through HTTP.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_read_update_read_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_read_update_read_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    put_object(&client, &bucket, "foo", b"soup").await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"soup");

    client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;

    let error = match client.get_object().bucket(&bucket).key("foo").send().await {
        Ok(_) => bail!("deleted object should not be readable"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

/// Verifies a PUT without metadata replaces previous user metadata.
#[tokio::test]
#[serial(conformance)]
async fn test_object_metadata_replaced_on_put() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_metadata_replaced_on_put
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .metadata("meta1", "bar")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;
    put_object(&client, &bucket, "foo", b"bar").await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    assert!(
        response
            .metadata()
            .is_none_or(|metadata| metadata.is_empty())
    );

    Ok(())
}

/// Verifies copying a zero-byte object preserves an empty body.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_zero_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_zero_size
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo123bar", b"").await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!("{bucket}/foo123bar"))
        .send()
        .await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("bar321foo")
        .send()
        .await?;
    assert_eq!(response.content_length(), Some(0));
    assert!(response.body.collect().await?.into_bytes().is_empty());

    Ok(())
}

/// Verifies ranged GET returns partial content and Content-Range.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_response_code() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_response_code
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let content = b"testcontent";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", content).await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=4-7")
        .send()
        .await?;

    assert_eq!(response.content_range(), Some("bytes 4-7/11"));
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        &content[4..8]
    );

    Ok(())
}

/// Verifies unsatisfiable ranged GET fails with InvalidRange.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_invalid_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_invalid_range
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", b"testcontent").await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=40-50")
        .send()
        .await
    {
        Ok(_) => bail!("unsatisfiable range should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidRange");

    Ok(())
}
