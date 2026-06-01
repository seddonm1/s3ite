//! Real-binary HTTP conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_bucket`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use reqwest::StatusCode;
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::bucket_name,
    raw_http, sdk,
    server::TestServer,
};

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_empty
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);

    let output = client.list_buckets().send().await?;
    assert!(output.buckets().is_empty());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;

    let output = client.list_buckets().send().await?;
    assert!(
        output
            .buckets()
            .iter()
            .any(|listed| listed.name() == Some(bucket.as_str()))
    );

    client.delete_bucket().bucket(&bucket).send().await?;

    let output = client.list_buckets().send().await?;
    assert!(
        output
            .buckets()
            .iter()
            .all(|listed| listed.name() != Some(bucket.as_str()))
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_head
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;
    client.head_bucket().bucket(&bucket).send().await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_head_notexist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_head_notexist
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-missing");

    let error = match client.head_bucket().bucket(&bucket).send().await {
        Ok(_) => bail!("HeadBucket for a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(404)");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_list_buckets_bad_auth() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_list_buckets_bad_auth
    let server = TestServer::spawn().await?;
    let unsigned = raw_http::unsigned_get(&server, "/").await?;
    assert_status(&unsigned, StatusCode::FORBIDDEN);

    let client = sdk::bad_secret_client(&server);

    let error = match client.list_buckets().send().await {
        Ok(_) => bail!("list buckets with bad credentials should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "403");

    Ok(())
}
