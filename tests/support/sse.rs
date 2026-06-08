use anyhow::{Context, Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{
        CompletedMultipartUpload, CompletedPart, ServerSideEncryption,
        ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    },
};
use reqwest::{Method, StatusCode};
use rusqlite::Connection;

use super::{
    assertions::{assert_error_contains, assert_status},
    raw_http,
    server::{TestServer, TestServerOptions},
};

pub const SSE_S3_KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
pub const MIB: usize = 1024 * 1024;

pub async fn put_sse_s3_object(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<()> {
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(body))
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    Ok(())
}

pub async fn put_plain_object(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

pub async fn upload_part_copy_object(
    client: &Client,
    source_bucket: &str,
    source_key: &str,
    destination_bucket: &str,
    destination_key: &str,
    destination_encrypted: bool,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let mut create_request = client
        .create_multipart_upload()
        .bucket(destination_bucket)
        .key(destination_key);
    if destination_encrypted {
        create_request = create_request.server_side_encryption(ServerSideEncryption::Aes256);
    }

    let create = create_request.send().await?;
    assert_sse_header(create.server_side_encryption(), destination_encrypted);
    let upload_id = create
        .upload_id()
        .context("CreateMultipartUpload response missing upload ID")?;

    let copy = client
        .upload_part_copy()
        .bucket(destination_bucket)
        .key(destination_key)
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(copy_source(source_bucket, source_key))
        .send()
        .await?;
    let etag = copy
        .copy_part_result()
        .and_then(|result| result.e_tag())
        .context("UploadPartCopy response missing ETag")?;
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
        .build();

    Ok(client
        .complete_multipart_upload()
        .bucket(destination_bucket)
        .key(destination_key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await?)
}

pub async fn assert_object_sse(
    client: &Client,
    bucket: &str,
    key: &str,
    encrypted: bool,
) -> Result<()> {
    let head = client.head_object().bucket(bucket).key(key).send().await?;
    assert_sse_header(head.server_side_encryption(), encrypted);
    Ok(())
}

pub fn assert_sse_header(value: Option<&ServerSideEncryption>, encrypted: bool) {
    if encrypted {
        assert_eq!(value, Some(&ServerSideEncryption::Aes256));
    } else {
        assert_eq!(value, None);
    }
}

pub async fn spawn_sse_server() -> Result<TestServer> {
    TestServer::spawn_with(TestServerOptions {
        config: Some(format!("sse_s3_key: \"{SSE_S3_KEY}\"\n")),
        ..TestServerOptions::default()
    })
    .await
}

pub fn sse_s3_bucket_configuration() -> Result<ServerSideEncryptionConfiguration> {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()?;
    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(default)
        .build();
    Ok(ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()?)
}

#[allow(clippy::too_many_arguments)]
pub async fn assert_rejected_without_object(
    server: &TestServer,
    client: &Client,
    bucket: &str,
    key: &str,
    method: Method,
    path: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Result<()> {
    let response = signed(server, method, path, body, headers).await?;
    assert_status(&response, StatusCode::BAD_REQUEST);
    assert_missing_object(client, bucket, key).await
}

pub async fn signed(
    server: &TestServer,
    method: Method,
    path: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Result<reqwest::Response> {
    raw_http::signed_request_with_headers(server, method, path, body, headers).await
}

async fn assert_missing_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    let error = match client.get_object().bucket(bucket).key(key).send().await {
        Ok(_) => bail!("rejected SSE request should not create object {key}"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    Ok(())
}

pub fn assert_storage_does_not_contain(
    server: &TestServer,
    bucket: &str,
    plaintext: &[u8],
) -> Result<()> {
    let path = server.data_root().join(format!("{bucket}.sqlite3"));
    let connection = Connection::open(path)?;
    let mut statement = connection.prepare("SELECT body FROM object_blob_chunk;")?;
    let bodies = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    for body in bodies {
        let body = body?;
        assert!(
            body.windows(plaintext.len())
                .all(|window| window != plaintext),
            "stored object chunk unexpectedly contains plaintext"
        );
    }
    Ok(())
}

pub fn copy_source(bucket: &str, key: &str) -> String {
    format!("{}/{}", bucket, raw_http::path_segment(key))
}

pub fn patterned_body(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

pub fn object_path(bucket: &str, key: &str) -> String {
    format!(
        "/{}/{}",
        raw_http::path_segment(bucket),
        raw_http::path_segment(key)
    )
}
