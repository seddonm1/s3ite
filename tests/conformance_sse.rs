//! Real-binary SSE-S3 conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_sse`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Context, Result};
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{
        CompletedMultipartUpload, CompletedPart, ServerSideEncryption, ServerSideEncryptionRule,
    },
};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    sdk,
    server::TestServer,
    sse::{
        MIB, assert_object_sse, assert_rejected_without_object, assert_storage_does_not_contain,
        copy_source, object_path, patterned_body, put_plain_object, put_sse_s3_object, signed,
        spawn_sse_server, sse_s3_bucket_configuration, upload_part_copy_object,
    },
};

async fn put_bucket_encryption_s3(client: &aws_sdk_s3::Client, bucket: &str) -> Result<()> {
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(sse_s3_bucket_configuration()?)
        .send()
        .await?;
    Ok(())
}

async fn assert_bucket_encryption_missing(client: &aws_sdk_s3::Client, bucket: &str) -> Result<()> {
    let error = client
        .get_bucket_encryption()
        .bucket(bucket)
        .send()
        .await
        .expect_err("bucket encryption should be absent");
    assert_error_contains(&error, "ServerSideEncryptionConfigurationNotFoundError");
    Ok(())
}

fn upload_body(size: usize, seed: u8) -> Vec<u8> {
    if size == 1 {
        vec![seed]
    } else {
        patterned_body(size, seed)
    }
}

fn assert_storage_does_not_contain_large_body(
    server: &TestServer,
    bucket: &str,
    body: &[u8],
) -> Result<()> {
    if body.len() > 1 {
        assert_storage_does_not_contain(server, bucket, body)?;
    }
    Ok(())
}

async fn assert_default_sse_s3_upload(size: usize, seed: u8) -> Result<()> {
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-default");
    let key = "testobj";
    let body = upload_body(size, seed);

    create_bucket(&client, &bucket).await?;
    put_bucket_encryption_s3(&client, &bucket).await?;

    let put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    let get = client.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        get.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    assert_eq!(
        get.body.collect().await?.into_bytes().as_ref(),
        body.as_slice()
    );
    assert_storage_does_not_contain_large_body(&server, &bucket, &body)?;

    Ok(())
}

async fn assert_explicit_sse_s3_upload(size: usize, seed: u8) -> Result<()> {
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-explicit");
    let key = "testobj";
    let body = upload_body(size, seed);

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);
    assert_object_sse(&client, &bucket, key, true).await?;
    assert_storage_does_not_contain_large_body(&server, &bucket, &body)?;

    Ok(())
}

/// Verifies PutBucketEncryption accepts an SSE-S3 default configuration.
#[tokio::test]
#[serial(conformance)]
async fn test_put_bucket_encryption_s3() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_bucket_encryption_s3
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-config");

    create_bucket(&client, &bucket).await?;
    put_bucket_encryption_s3(&client, &bucket).await
}

/// Verifies GetBucketEncryption returns an SSE-S3 default configuration.
#[tokio::test]
#[serial(conformance)]
async fn test_get_bucket_encryption_s3() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_bucket_encryption_s3
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-config");

    create_bucket(&client, &bucket).await?;
    assert_bucket_encryption_missing(&client, &bucket).await?;
    put_bucket_encryption_s3(&client, &bucket).await?;

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

    Ok(())
}

/// Verifies DeleteBucketEncryption removes an SSE-S3 default configuration.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_bucket_encryption_s3() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_bucket_encryption_s3
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-config");

    create_bucket(&client, &bucket).await?;
    client
        .delete_bucket_encryption()
        .bucket(&bucket)
        .send()
        .await?;
    put_bucket_encryption_s3(&client, &bucket).await?;
    client
        .delete_bucket_encryption()
        .bucket(&bucket)
        .send()
        .await?;
    assert_bucket_encryption_missing(&client, &bucket).await
}

/// Verifies bucket-default SSE-S3 writes a 1-byte object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_upload_1b() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_upload_1b
    assert_default_sse_s3_upload(1, b'A').await
}

/// Verifies bucket-default SSE-S3 writes a 1 KiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_upload_1kb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_upload_1kb
    assert_default_sse_s3_upload(1024, b'B').await
}

/// Verifies bucket-default SSE-S3 writes a 1 MiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_upload_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_upload_1mb
    assert_default_sse_s3_upload(MIB, b'C').await
}

/// Verifies bucket-default SSE-S3 writes an 8 MiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_upload_8mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_upload_8mb
    assert_default_sse_s3_upload(8 * MIB, b'D').await
}

/// Verifies default SSE-S3 is visible on HeadObject and rejects write SSE headers on HEAD.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_method_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_method_head
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-head");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_encryption_s3(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(vec![b'A'; 1000]))
        .send()
        .await?;

    assert_object_sse(&client, &bucket, key, true).await?;
    let rejected = signed(
        &server,
        Method::HEAD,
        &object_path(&bucket, key),
        Vec::new(),
        &[("x-amz-server-side-encryption", "AES256")],
    )
    .await?;
    assert_status(&rejected, StatusCode::BAD_REQUEST);

    Ok(())
}

/// Verifies bucket-default SSE-S3 applies to multipart uploads.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_multipart_upload() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_default_multipart_upload
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-sse-mpu");
    let key = "multipart_enc";
    let body = patterned_body(30 * MIB, b'E');

    create_bucket(&client, &bucket).await?;
    put_bucket_encryption_s3(&client, &bucket).await?;
    let create = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .content_type("text/plain")
        .metadata("foo", "bar")
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
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .content_length(body.len().try_into()?)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part.e_tag().context("UploadPart response missing ETag")?)
                .build(),
        )
        .build();
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await?;

    let listed = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(key)
        .send()
        .await?;
    assert_eq!(listed.contents().len(), 1);
    assert_eq!(listed.contents()[0].size(), Some(body.len().try_into()?));

    let object = client.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        object
            .metadata()
            .and_then(|metadata| metadata.get("foo"))
            .map(String::as_str),
        Some("bar")
    );
    assert_eq!(object.content_type(), Some("text/plain"));
    assert_eq!(
        object.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    assert_eq!(
        object.body.collect().await?.into_bytes().as_ref(),
        body.as_slice()
    );

    for start in [1_000_000_usize, 10_000_000] {
        let end = start + 999;
        let ranged = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .range(format!("bytes={start}-{end}"))
            .send()
            .await?;
        assert_eq!(
            ranged.body.collect().await?.into_bytes().as_ref(),
            &body[start..=end]
        );
    }
    assert_storage_does_not_contain(&server, &bucket, &body)?;

    Ok(())
}

/// Verifies explicit SSE-S3 writes a 1-byte object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_encrypted_upload_1b() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_encrypted_upload_1b
    assert_explicit_sse_s3_upload(1, b'F').await
}

/// Verifies explicit SSE-S3 writes a 1 KiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_encrypted_upload_1kb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_encrypted_upload_1kb
    assert_explicit_sse_s3_upload(1024, b'G').await
}

/// Verifies explicit SSE-S3 writes a 1 MiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_encrypted_upload_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_encrypted_upload_1mb
    assert_explicit_sse_s3_upload(MIB, b'H').await
}

/// Verifies explicit SSE-S3 writes an 8 MiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_encrypted_upload_8mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_s3_encrypted_upload_8mb
    assert_explicit_sse_s3_upload(8 * MIB, b'I').await
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
async fn s3ite_sse_s3_conflicts_are_rejected_without_side_effects() -> Result<()> {
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
