//! Real-binary HTTP conformance tests for expected bucket owner headers.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_expected_owner`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::assert_status,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    raw_http, sdk,
    server::TestServer,
};

const OWNER_ID: &str = "s3ite";
const WRONG_OWNER: &str = "not-s3ite";
const EXPECTED_OWNER_HEADER: &str = "x-amz-expected-bucket-owner";
const SOURCE_EXPECTED_OWNER_HEADER: &str = "x-amz-source-expected-bucket-owner";

/// Verifies HeadBucket applies expected-owner matching before bucket existence.
#[tokio::test]
#[serial(conformance)]
async fn test_expected_owner_head_bucket() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-owner");
    let missing_bucket = bucket_name("s3ite-owner-missing");

    create_bucket(&client, &bucket).await?;

    let bucket_path = format!("/{}", raw_http::path_segment(&bucket));
    let ok = raw_http::signed_request_with_headers(
        &server,
        Method::HEAD,
        &bucket_path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, OWNER_ID)],
    )
    .await?;
    assert_status(&ok, StatusCode::OK);

    let denied = raw_http::signed_request_with_headers(
        &server,
        Method::HEAD,
        &bucket_path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&denied, StatusCode::FORBIDDEN);

    let missing_path = format!("/{}", raw_http::path_segment(&missing_bucket));
    let missing = raw_http::signed_request_with_headers(
        &server,
        Method::HEAD,
        &missing_path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&missing, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies expected-owner checks guard object reads and writes without mutation.
#[tokio::test]
#[serial(conformance)]
async fn test_expected_owner_object_io() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-owner");
    let key = "object.txt";
    let path = object_path(&bucket, key);

    create_bucket(&client, &bucket).await?;

    let blocked_put = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &path,
        b"blocked".to_vec(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&blocked_put, StatusCode::FORBIDDEN);
    assert_missing_raw(&server, &bucket, key).await?;

    let put = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &path,
        b"allowed".to_vec(),
        &[(EXPECTED_OWNER_HEADER, OWNER_ID)],
    )
    .await?;
    assert_status(&put, StatusCode::OK);

    let get = raw_http::signed_request_with_headers(
        &server,
        Method::GET,
        &path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, OWNER_ID)],
    )
    .await?;
    assert_status(&get, StatusCode::OK);
    assert_eq!(get.bytes().await?.as_ref(), b"allowed");

    let denied_get = raw_http::signed_request_with_headers(
        &server,
        Method::GET,
        &path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&denied_get, StatusCode::FORBIDDEN);

    let denied_head = raw_http::signed_request_with_headers(
        &server,
        Method::HEAD,
        &path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&denied_head, StatusCode::FORBIDDEN);

    let denied_delete = raw_http::signed_request_with_headers(
        &server,
        Method::DELETE,
        &path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&denied_delete, StatusCode::FORBIDDEN);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"allowed");

    Ok(())
}

/// Verifies expected-owner checks guard bucket object listings.
#[tokio::test]
#[serial(conformance)]
async fn test_expected_owner_list_objects() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-owner");
    let list_path = format!("/{}?list-type=2", raw_http::path_segment(&bucket));

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "listed.txt", b"listed").await?;

    let ok = raw_http::signed_request_with_headers(
        &server,
        Method::GET,
        &list_path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, OWNER_ID)],
    )
    .await?;
    assert_status(&ok, StatusCode::OK);

    let denied = raw_http::signed_request_with_headers(
        &server,
        Method::GET,
        &list_path,
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&denied, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies CopyObject validates both destination and source expected owners.
#[tokio::test]
#[serial(conformance)]
async fn test_expected_owner_copy_object() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-owner");
    let copy_source = format!("{}/{}", bucket, raw_http::path_segment("source.txt"));

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("source.txt")
        .body(ByteStream::from_static(b"source-body"))
        .send()
        .await?;

    let denied_target = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &object_path(&bucket, "target-owner.txt"),
        Vec::new(),
        &[
            ("x-amz-copy-source", copy_source.as_str()),
            (EXPECTED_OWNER_HEADER, WRONG_OWNER),
        ],
    )
    .await?;
    assert_status(&denied_target, StatusCode::FORBIDDEN);
    assert_missing_raw(&server, &bucket, "target-owner.txt").await?;

    let denied_source = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &object_path(&bucket, "source-owner.txt"),
        Vec::new(),
        &[
            ("x-amz-copy-source", copy_source.as_str()),
            (SOURCE_EXPECTED_OWNER_HEADER, WRONG_OWNER),
        ],
    )
    .await?;
    assert_status(&denied_source, StatusCode::FORBIDDEN);
    assert_missing_raw(&server, &bucket, "source-owner.txt").await?;

    let ok = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &object_path(&bucket, "copied.txt"),
        Vec::new(),
        &[
            ("x-amz-copy-source", copy_source.as_str()),
            (EXPECTED_OWNER_HEADER, OWNER_ID),
            (SOURCE_EXPECTED_OWNER_HEADER, OWNER_ID),
        ],
    )
    .await?;
    assert_status(&ok, StatusCode::OK);
    assert_eq!(
        get_object_bytes(&client, &bucket, "copied.txt").await?,
        b"source-body"
    );

    Ok(())
}

/// Verifies expected-owner checks guard multipart upload creation.
#[tokio::test]
#[serial(conformance)]
async fn test_expected_owner_create_multipart_upload() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-owner");
    let key = "multipart.txt";

    create_bucket(&client, &bucket).await?;

    let denied = raw_http::signed_request_with_headers(
        &server,
        Method::POST,
        &format!("{}?uploads", object_path(&bucket, key)),
        Vec::new(),
        &[(EXPECTED_OWNER_HEADER, WRONG_OWNER)],
    )
    .await?;
    assert_status(&denied, StatusCode::FORBIDDEN);
    assert!(
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await?
            .uploads()
            .is_empty()
    );

    Ok(())
}

async fn assert_missing_raw(server: &TestServer, bucket: &str, key: &str) -> Result<()> {
    let response =
        raw_http::signed_request(server, Method::GET, &object_path(bucket, key), Vec::new())
            .await?;
    assert_status(&response, StatusCode::NOT_FOUND);
    Ok(())
}

fn object_path(bucket: &str, key: &str) -> String {
    format!(
        "/{}/{}",
        raw_http::path_segment(bucket),
        raw_http::path_segment(key)
    )
}
