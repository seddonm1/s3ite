//! Real-binary unsupported server-side-encryption conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_sse`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Context, Result, bail};
use aws_sdk_s3::{Client, primitives::ByteStream};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, put_object},
    raw_http, sdk,
    server::TestServer,
};

/// Verifies unsupported SSE request headers fail before plaintext is stored.
#[tokio::test]
#[serial(conformance)]
async fn test_unsupported_sse_headers_are_rejected_without_side_effects() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse");

    create_bucket(&client, &bucket).await?;

    assert_put_sse_rejected(&server, &client, &bucket).await?;
    assert_copy_sse_rejected(&server, &client, &bucket).await?;
    assert_create_multipart_sse_rejected(&server, &client, &bucket).await?;
    assert_upload_part_sse_rejected(&server, &client, &bucket).await?;
    assert_upload_part_copy_sse_rejected(&server, &client, &bucket).await?;
    assert_complete_multipart_sse_rejected(&server, &client, &bucket).await?;

    Ok(())
}

async fn assert_put_sse_rejected(server: &TestServer, client: &Client, bucket: &str) -> Result<()> {
    let key = "put-sse";
    let response = signed(
        server,
        Method::PUT,
        &object_path(bucket, key),
        b"plaintext".to_vec(),
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await?;
    assert_sse_rejected(response).await?;
    assert_missing_object(client, bucket, key).await
}

async fn assert_copy_sse_rejected(
    server: &TestServer,
    client: &Client,
    bucket: &str,
) -> Result<()> {
    put_object(client, bucket, "copy-source", b"source").await?;
    let key = "copy-sse";
    let response = signed(
        server,
        Method::PUT,
        &object_path(bucket, key),
        Vec::new(),
        &[
            ("x-amz-copy-source", &format!("/{bucket}/copy-source")),
            ("x-amz-server-side-encryption", "AES256"),
        ],
    )
    .await?;
    assert_sse_rejected(response).await?;
    assert_missing_object(client, bucket, key).await
}

async fn assert_create_multipart_sse_rejected(
    server: &TestServer,
    client: &Client,
    bucket: &str,
) -> Result<()> {
    let key = "create-mpu-sse";
    let response = signed(
        server,
        Method::POST,
        &format!("{}?uploads", object_path(bucket, key)),
        Vec::new(),
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await?;
    assert_sse_rejected(response).await?;

    let uploads = client
        .list_multipart_uploads()
        .bucket(bucket)
        .prefix(key)
        .send()
        .await?;
    assert!(uploads.uploads().is_empty());
    Ok(())
}

async fn assert_upload_part_sse_rejected(
    server: &TestServer,
    client: &Client,
    bucket: &str,
) -> Result<()> {
    let key = "upload-part-sse";
    let upload_id = create_upload(client, bucket, key).await?;
    let response = signed(
        server,
        Method::PUT,
        &format!(
            "{}?partNumber=1&uploadId={upload_id}",
            object_path(bucket, key)
        ),
        b"part".to_vec(),
        &[("x-amz-server-side-encryption-customer-algorithm", "AES256")],
    )
    .await?;
    assert_sse_rejected(response).await?;

    let parts = client
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;
    assert!(parts.parts().is_empty());
    Ok(())
}

async fn assert_upload_part_copy_sse_rejected(
    server: &TestServer,
    client: &Client,
    bucket: &str,
) -> Result<()> {
    put_object(client, bucket, "copy-part-source", b"source").await?;
    let key = "upload-part-copy-sse";
    let upload_id = create_upload(client, bucket, key).await?;
    let response = signed(
        server,
        Method::PUT,
        &format!(
            "{}?partNumber=1&uploadId={upload_id}",
            object_path(bucket, key)
        ),
        Vec::new(),
        &[
            ("x-amz-copy-source", &format!("/{bucket}/copy-part-source")),
            (
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                "AES256",
            ),
        ],
    )
    .await?;
    assert_sse_rejected(response).await?;

    let parts = client
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;
    assert!(parts.parts().is_empty());
    Ok(())
}

async fn assert_complete_multipart_sse_rejected(
    server: &TestServer,
    client: &Client,
    bucket: &str,
) -> Result<()> {
    let key = "complete-mpu-sse";
    let upload_id = create_upload(client, bucket, key).await?;
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await?;
    let etag = part
        .e_tag()
        .context("UploadPart response missing ETag")?
        .to_string();
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let response = signed(
        server,
        Method::POST,
        &format!("{}?uploadId={upload_id}", object_path(bucket, key)),
        body.into_bytes(),
        &[("x-amz-server-side-encryption-bucket-key-enabled", "true")],
    )
    .await?;
    assert_sse_rejected(response).await?;
    assert_missing_object(client, bucket, key).await
}

async fn create_upload(client: &Client, bucket: &str, key: &str) -> Result<String> {
    client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .map(str::to_string)
        .context("CreateMultipartUpload response missing upload ID")
}

async fn signed(
    server: &TestServer,
    method: Method,
    path: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Result<reqwest::Response> {
    raw_http::signed_request_with_headers(server, method, path, body, headers).await
}

async fn assert_sse_rejected(response: reqwest::Response) -> Result<()> {
    assert_status(&response, StatusCode::NOT_IMPLEMENTED);
    let body = response.text().await?;
    assert!(
        body.contains("NotImplemented"),
        "expected NotImplemented error, got {body}"
    );
    assert!(
        body.contains("server-side encryption is not supported"),
        "expected unsupported SSE message, got {body}"
    );
    Ok(())
}

async fn assert_missing_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    let error = match client.get_object().bucket(bucket).key(key).send().await {
        Ok(_) => bail!("rejected SSE request should not create object {key}"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    Ok(())
}

fn object_path(bucket: &str, key: &str) -> String {
    format!(
        "/{}/{}",
        raw_http::path_segment(bucket),
        raw_http::path_segment(key)
    )
}
