//! Real-binary HTTP copy-object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_copy`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{primitives::ByteStream, types::MetadataDirective};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;

/// Verifies copying within the same bucket preserves object bytes.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_same_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_same_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo123bar", b"foo").await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!("{bucket}/foo123bar"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &bucket, "bar321foo").await?,
        b"foo"
    );

    Ok(())
}

/// Verifies CopyObject preserves the source content type by default.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_verify_contenttype() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_verify_contenttype
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");
    let content_type = "text/bla";

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo123bar")
        .content_type(content_type)
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await?;

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
    assert_eq!(response.content_type(), Some(content_type));
    assert_eq!(response.body.collect().await?.into_bytes().as_ref(), b"foo");

    Ok(())
}

/// Verifies self-copy without metadata changes is rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_to_itself() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_to_itself
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo123bar", b"foo").await?;

    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("foo123bar")
        .copy_source(format!("{bucket}/foo123bar"))
        .send()
        .await
    {
        Ok(_) => bail!("copying an object to itself without changes should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidRequest");

    Ok(())
}

/// Verifies self-copy with replacement metadata updates object metadata.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_to_itself_with_metadata() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_to_itself_with_metadata
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo123bar", b"foo").await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("foo123bar")
        .copy_source(format!("{bucket}/foo123bar"))
        .metadata("foo", "bar")
        .metadata_directive(MetadataDirective::Replace)
        .send()
        .await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("foo123bar")
        .send()
        .await?;
    let metadata = response
        .metadata()
        .ok_or_else(|| anyhow::anyhow!("copied object missing metadata"))?;
    assert_eq!(metadata.get("foo").map(String::as_str), Some("bar"));
    assert_eq!(response.body.collect().await?.into_bytes().as_ref(), b"foo");

    Ok(())
}

/// Verifies copying across buckets preserves object bytes.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_diff_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_diff_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-copy-src");
    let target_bucket = bucket_name("s3ite-copy-dst");

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_object(&client, &source_bucket, "foo123bar", b"foo").await?;

    client
        .copy_object()
        .bucket(&target_bucket)
        .key("bar321foo")
        .copy_source(format!("{source_bucket}/foo123bar"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &target_bucket, "bar321foo").await?,
        b"foo"
    );

    Ok(())
}

/// Verifies CopyObject retains user metadata and content type by default.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_retaining_metadata() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_retaining_metadata
    for size in [3, MIB] {
        let server = TestServer::spawn().await?;
        let client = sdk::client(&server);
        let bucket = bucket_name("s3ite-copy");
        let content_type = "audio/ogg";

        create_bucket(&client, &bucket).await?;
        client
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .content_type(content_type)
            .body(ByteStream::from(vec![0; size]))
            .send()
            .await?;

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
        assert_eq!(response.content_type(), Some(content_type));
        let metadata = response
            .metadata()
            .ok_or_else(|| anyhow::anyhow!("copied object missing metadata"))?;
        assert_eq!(metadata.get("key1").map(String::as_str), Some("value1"));
        assert_eq!(metadata.get("key2").map(String::as_str), Some("value2"));
        assert_eq!(response.content_length(), Some(size as i64));
    }

    Ok(())
}

/// Verifies replacement metadata and content type are applied during copy.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_replacing_metadata() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_replacing_metadata
    for size in [3, MIB] {
        let server = TestServer::spawn().await?;
        let client = sdk::client(&server);
        let bucket = bucket_name("s3ite-copy");

        create_bucket(&client, &bucket).await?;
        client
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .content_type("audio/ogg")
            .body(ByteStream::from(vec![0; size]))
            .send()
            .await?;

        client
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{bucket}/foo123bar"))
            .metadata("key3", "value3")
            .metadata("key2", "value2")
            .metadata_directive(MetadataDirective::Replace)
            .content_type("audio/mpeg")
            .send()
            .await?;

        let response = client
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await?;
        assert_eq!(response.content_type(), Some("audio/mpeg"));
        let metadata = response
            .metadata()
            .ok_or_else(|| anyhow::anyhow!("copied object missing metadata"))?;
        assert_eq!(metadata.get("key1"), None);
        assert_eq!(metadata.get("key2").map(String::as_str), Some("value2"));
        assert_eq!(metadata.get("key3").map(String::as_str), Some("value3"));
        assert_eq!(response.content_length(), Some(size as i64));
    }

    Ok(())
}

/// Verifies copying from a missing source bucket fails.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_bucket_not_found() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_bucket_not_found
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");

    create_bucket(&client, &bucket).await?;
    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!("{bucket}-fake/foo123bar"))
        .send()
        .await
    {
        Ok(_) => bail!("copying from a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

/// Verifies copying from a missing source key fails.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_key_not_found() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_key_not_found
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");

    create_bucket(&client, &bucket).await?;
    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!("{bucket}/foo123bar"))
        .send()
        .await
    {
        Ok(_) => bail!("copying from a missing key should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}
