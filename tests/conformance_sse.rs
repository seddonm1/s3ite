//! Real-binary SSE-S3 conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_sse`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

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
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    raw_http, sdk,
    server::{TestServer, TestServerOptions},
};

const SSE_S3_KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
const MIB: usize = 1024 * 1024;

/// Verifies explicit SSE-S3 writes round-trip while stored chunks are encrypted.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_explicit_put_round_trips_and_encrypts_storage() -> Result<()> {
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-explicit");
    let key = "put-sse";
    let body = b"plaintext payload that must not be stored as plaintext";

    create_bucket(&client, &bucket).await?;
    let output = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);
    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    assert_storage_does_not_contain(&server, &bucket, body)?;

    Ok(())
}

/// Verifies explicit SSE-S3 writes an 8 MiB object as encrypted storage.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_explicit_put_8mb_round_trips_and_encrypts_storage() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_encrypted_upload_8mb
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-explicit-8mb");
    let key = "put-sse-8mb";
    let body = patterned_body(8 * MIB, b'A');

    create_bucket(&client, &bucket).await?;
    let output = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);
    assert_object_sse(&client, &bucket, key, true).await?;
    assert_storage_does_not_contain(&server, &bucket, &body)?;

    Ok(())
}

/// Verifies bucket-default SSE-S3 applies to PutObject and multipart uploads.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_bucket_default_put_and_multipart() -> Result<()> {
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-default");

    create_bucket(&client, &bucket).await?;
    client
        .put_bucket_encryption()
        .bucket(&bucket)
        .server_side_encryption_configuration(sse_s3_bucket_configuration()?)
        .send()
        .await?;

    let encryption = client
        .get_bucket_encryption()
        .bucket(&bucket)
        .send()
        .await?;
    let rule = encryption
        .server_side_encryption_configuration()
        .and_then(|configuration| configuration.rules().first())
        .and_then(ServerSideEncryptionRule::apply_server_side_encryption_by_default)
        .context("bucket encryption response missing SSE default")?;
    assert_eq!(rule.sse_algorithm(), &ServerSideEncryption::Aes256);

    let key = "default-put";
    let body = b"default encrypted object";
    let output = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);

    let multipart_key = "default-multipart";
    let multipart_body = b"default encrypted multipart body";
    let create = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(multipart_key)
        .send()
        .await?;
    assert_eq!(
        create.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    let upload_id = create
        .upload_id()
        .context("CreateMultipartUpload response missing upload ID")?;
    let part = client
        .upload_part()
        .bucket(&bucket)
        .key(multipart_key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(multipart_body))
        .send()
        .await?;
    assert_eq!(
        part.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    let etag = part
        .e_tag()
        .context("UploadPart response missing ETag")?
        .to_string();
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
        .build();
    let complete = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(multipart_key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await?;
    assert_eq!(
        complete.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    assert_eq!(
        get_object_bytes(&client, &bucket, multipart_key).await?,
        multipart_body
    );
    assert_storage_does_not_contain(&server, &bucket, multipart_body)?;

    client
        .delete_bucket_encryption()
        .bucket(&bucket)
        .send()
        .await?;
    let error = match client.get_bucket_encryption().bucket(&bucket).send().await {
        Ok(_) => bail!("deleted bucket encryption should not be returned"),
        Err(error) => error,
    };
    assert_error_contains(&error, "ServerSideEncryptionConfigurationNotFoundError");

    Ok(())
}

/// Verifies bucket-default SSE-S3 writes an 8 MiB object as encrypted storage.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_bucket_default_put_8mb_round_trips_and_encrypts_storage() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_upload_8mb
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-default-8mb");
    let key = "default-put-8mb";
    let body = patterned_body(8 * MIB, b'B');

    create_bucket(&client, &bucket).await?;
    client
        .put_bucket_encryption()
        .bucket(&bucket)
        .server_side_encryption_configuration(sse_s3_bucket_configuration()?)
        .send()
        .await?;

    let output = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);
    assert_object_sse(&client, &bucket, key, true).await?;
    assert_storage_does_not_contain(&server, &bucket, &body)?;

    Ok(())
}

/// Verifies CopyObject can copy plaintext into an SSE-S3 destination.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_copy_plaintext_to_encrypted_destination() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[unencrypted-sse-s3-STANDARD-STANDARD-8388608]
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-sse-copy-src-plain");
    let destination_bucket = bucket_name("s3ite-sse-copy-dst-enc");
    let source_key = "source";
    let destination_key = "destination";
    let body = patterned_body(8 * MIB, b'C');

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &destination_bucket).await?;
    put_plain_object(&client, &source_bucket, source_key, body.clone()).await?;

    let output = client
        .copy_object()
        .bucket(&destination_bucket)
        .key(destination_key)
        .copy_source(copy_source(&source_bucket, source_key))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(
        get_object_bytes(&client, &destination_bucket, destination_key).await?,
        body
    );
    assert_object_sse(&client, &destination_bucket, destination_key, true).await?;
    assert_storage_does_not_contain(&server, &destination_bucket, &body)?;

    Ok(())
}

/// Verifies CopyObject can copy an SSE-S3 source into a plaintext destination.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_copy_encrypted_source_to_plaintext_destination() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-s3-unencrypted-STANDARD-STANDARD-8388608]
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-sse-copy-src-enc");
    let destination_bucket = bucket_name("s3ite-sse-copy-dst-plain");
    let source_key = "source";
    let destination_key = "destination";
    let body = patterned_body(8 * MIB, b'D');

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &destination_bucket).await?;
    put_sse_s3_object(&client, &source_bucket, source_key, body.clone()).await?;

    let output = client
        .copy_object()
        .bucket(&destination_bucket)
        .key(destination_key)
        .copy_source(copy_source(&source_bucket, source_key))
        .send()
        .await?;
    assert_eq!(output.server_side_encryption(), None);

    assert_eq!(
        get_object_bytes(&client, &destination_bucket, destination_key).await?,
        body
    );
    assert_object_sse(&client, &destination_bucket, destination_key, false).await?;
    assert_storage_does_not_contain(&server, &source_bucket, &body)?;

    Ok(())
}

/// Verifies CopyObject can copy an SSE-S3 source into an SSE-S3 destination.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_copy_encrypted_source_to_encrypted_destination() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-s3-sse-s3-STANDARD-STANDARD-8388608]
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-sse-copy-src-enc2");
    let destination_bucket = bucket_name("s3ite-sse-copy-dst-enc2");
    let source_key = "source";
    let destination_key = "destination";
    let body = patterned_body(8 * MIB, b'E');

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &destination_bucket).await?;
    put_sse_s3_object(&client, &source_bucket, source_key, body.clone()).await?;

    let output = client
        .copy_object()
        .bucket(&destination_bucket)
        .key(destination_key)
        .copy_source(copy_source(&source_bucket, source_key))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(
        get_object_bytes(&client, &destination_bucket, destination_key).await?,
        body
    );
    assert_object_sse(&client, &destination_bucket, destination_key, true).await?;
    assert_storage_does_not_contain(&server, &source_bucket, &body)?;
    assert_storage_does_not_contain(&server, &destination_bucket, &body)?;

    Ok(())
}

/// Verifies UploadPartCopy can copy plaintext into an SSE-S3 destination.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_upload_part_copy_plaintext_to_encrypted_destination() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[unencrypted-sse-s3-STANDARD-STANDARD-8388608]
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-sse-upc-src-plain");
    let destination_bucket = bucket_name("s3ite-sse-upc-dst-enc");
    let source_key = "source";
    let destination_key = "destination";
    let body = patterned_body(8 * MIB, b'F');

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &destination_bucket).await?;
    put_plain_object(&client, &source_bucket, source_key, body.clone()).await?;

    let output = upload_part_copy_object(
        &client,
        &source_bucket,
        source_key,
        &destination_bucket,
        destination_key,
        true,
    )
    .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(
        get_object_bytes(&client, &destination_bucket, destination_key).await?,
        body
    );
    assert_object_sse(&client, &destination_bucket, destination_key, true).await?;
    assert_storage_does_not_contain(&server, &destination_bucket, &body)?;

    Ok(())
}

/// Verifies UploadPartCopy can copy an SSE-S3 source into a plaintext destination.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_upload_part_copy_encrypted_source_to_plaintext_destination() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[sse-s3-unencrypted-STANDARD-STANDARD-8388608]
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-sse-upc-src-enc");
    let destination_bucket = bucket_name("s3ite-sse-upc-dst-plain");
    let source_key = "source";
    let destination_key = "destination";
    let body = patterned_body(8 * MIB, b'G');

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &destination_bucket).await?;
    put_sse_s3_object(&client, &source_bucket, source_key, body.clone()).await?;

    let output = upload_part_copy_object(
        &client,
        &source_bucket,
        source_key,
        &destination_bucket,
        destination_key,
        false,
    )
    .await?;
    assert_eq!(output.server_side_encryption(), None);

    assert_eq!(
        get_object_bytes(&client, &destination_bucket, destination_key).await?,
        body
    );
    assert_object_sse(&client, &destination_bucket, destination_key, false).await?;
    assert_storage_does_not_contain(&server, &source_bucket, &body)?;

    Ok(())
}

/// Verifies UploadPartCopy can copy an SSE-S3 source into an SSE-S3 destination.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_upload_part_copy_encrypted_source_to_encrypted_destination() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[sse-s3-sse-s3-STANDARD-STANDARD-8388608]
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-sse-upc-src-enc2");
    let destination_bucket = bucket_name("s3ite-sse-upc-dst-enc2");
    let source_key = "source";
    let destination_key = "destination";
    let body = patterned_body(8 * MIB, b'H');

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &destination_bucket).await?;
    put_sse_s3_object(&client, &source_bucket, source_key, body.clone()).await?;

    let output = upload_part_copy_object(
        &client,
        &source_bucket,
        source_key,
        &destination_bucket,
        destination_key,
        true,
    )
    .await?;
    assert_eq!(
        output.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(
        get_object_bytes(&client, &destination_bucket, destination_key).await?,
        body
    );
    assert_object_sse(&client, &destination_bucket, destination_key, true).await?;
    assert_storage_does_not_contain(&server, &source_bucket, &body)?;
    assert_storage_does_not_contain(&server, &destination_bucket, &body)?;

    Ok(())
}

/// Verifies unsupported SSE combinations are rejected without creating objects.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_conflicts_are_rejected_without_side_effects() -> Result<()> {
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-conflict");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "copy-source", b"source").await?;

    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "put-ssec",
        Method::PUT,
        &object_path(&bucket, "put-ssec"),
        b"plaintext".to_vec(),
        &[("x-amz-server-side-encryption-customer-algorithm", "AES256")],
    )
    .await?;
    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "put-ssec-sse-s3",
        Method::PUT,
        &object_path(&bucket, "put-ssec-sse-s3"),
        b"plaintext".to_vec(),
        &[
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ],
    )
    .await?;
    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "put-ssec-kms",
        Method::PUT,
        &object_path(&bucket, "put-ssec-kms"),
        b"plaintext".to_vec(),
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ],
    )
    .await?;
    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "put-kms-conflict",
        Method::PUT,
        &object_path(&bucket, "put-kms-conflict"),
        b"plaintext".to_vec(),
        &[
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "kms-key"),
        ],
    )
    .await?;
    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "put-kms",
        Method::PUT,
        &object_path(&bucket, "put-kms"),
        b"plaintext".to_vec(),
        &[("x-amz-server-side-encryption", "aws:kms")],
    )
    .await?;
    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "put-bad-sse",
        Method::PUT,
        &object_path(&bucket, "put-bad-sse"),
        b"plaintext".to_vec(),
        &[("x-amz-server-side-encryption", "invalid-sse")],
    )
    .await?;
    assert_rejected_without_object(
        &server,
        &client,
        &bucket,
        "copy-ssec",
        Method::PUT,
        &object_path(&bucket, "copy-ssec"),
        Vec::new(),
        &[
            ("x-amz-copy-source", &format!("/{bucket}/copy-source")),
            (
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                "AES256",
            ),
        ],
    )
    .await?;

    let key = "head-sse-header";
    put_object(&client, &bucket, key, b"body").await?;
    let response = signed(
        &server,
        Method::HEAD,
        &object_path(&bucket, key),
        Vec::new(),
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await?;
    assert_status(&response, StatusCode::BAD_REQUEST);

    Ok(())
}

async fn put_sse_s3_object(client: &Client, bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
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

async fn put_plain_object(client: &Client, bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

async fn upload_part_copy_object(
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

async fn assert_object_sse(
    client: &Client,
    bucket: &str,
    key: &str,
    encrypted: bool,
) -> Result<()> {
    let head = client.head_object().bucket(bucket).key(key).send().await?;
    assert_sse_header(head.server_side_encryption(), encrypted);
    Ok(())
}

fn assert_sse_header(value: Option<&ServerSideEncryption>, encrypted: bool) {
    if encrypted {
        assert_eq!(value, Some(&ServerSideEncryption::Aes256));
    } else {
        assert_eq!(value, None);
    }
}

async fn spawn_sse_server() -> Result<TestServer> {
    TestServer::spawn_with(TestServerOptions {
        config: Some(format!("sse_s3_key: \"{SSE_S3_KEY}\"\n")),
        ..TestServerOptions::default()
    })
    .await
}

fn sse_s3_bucket_configuration() -> Result<ServerSideEncryptionConfiguration> {
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

async fn assert_rejected_without_object(
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

async fn signed(
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

fn assert_storage_does_not_contain(
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

fn copy_source(bucket: &str, key: &str) -> String {
    format!("{}/{}", bucket, raw_http::path_segment(key))
}

fn patterned_body(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

fn object_path(bucket: &str, key: &str) -> String {
    format!(
        "/{}/{}",
        raw_http::path_segment(bucket),
        raw_http::path_segment(key)
    )
}
