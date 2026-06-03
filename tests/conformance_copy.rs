//! Real-binary HTTP copy-object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_copy`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, MetadataDirective},
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_bucket_versioning, put_object},
    raw_http, sdk,
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

/// Verifies CopyObject can copy a 16 MiB object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_16m() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_16m
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("obj1")
        .body(ByteStream::from(vec![0; 16 * MIB]))
        .send()
        .await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("obj2")
        .copy_source(format!("{bucket}/obj1"))
        .send()
        .await?;
    let response = client
        .get_object()
        .bucket(&bucket)
        .key("obj2")
        .send()
        .await?;
    assert_eq!(response.content_length(), Some((16 * MIB) as i64));

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

/// Verifies CopyObject can copy exact versions in and across versioned buckets.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_versioned_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_versioned_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");
    let versioned_target_bucket = bucket_name("s3ite-copy-versioned");
    let plain_target_bucket = bucket_name("s3ite-copy-plain");
    let body = vec![0; 5];

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let source = client
        .put_object()
        .bucket(&bucket)
        .key("foo123bar")
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let source_version_id = source
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("versioned PutObject missing version ID"))?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!("{bucket}/foo123bar?versionId={source_version_id}"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "bar321foo").await?, body);

    let copied = client
        .get_object()
        .bucket(&bucket)
        .key("bar321foo")
        .send()
        .await?;
    let copied_version_id = copied
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("copied version missing version ID"))?
        .to_string();
    assert_eq!(copied.content_length(), Some(5));

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo2")
        .copy_source(format!("{bucket}/bar321foo?versionId={copied_version_id}"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &bucket, "bar321foo2").await?,
        body
    );

    create_bucket(&client, &versioned_target_bucket).await?;
    put_bucket_versioning(
        &client,
        &versioned_target_bucket,
        BucketVersioningStatus::Enabled,
    )
    .await?;
    client
        .copy_object()
        .bucket(&versioned_target_bucket)
        .key("bar321foo3")
        .copy_source(format!("{bucket}/foo123bar?versionId={source_version_id}"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &versioned_target_bucket, "bar321foo3").await?,
        body
    );

    create_bucket(&client, &plain_target_bucket).await?;
    client
        .copy_object()
        .bucket(&plain_target_bucket)
        .key("bar321foo4")
        .copy_source(format!("{bucket}/foo123bar?versionId={source_version_id}"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &plain_target_bucket, "bar321foo4").await?,
        body
    );

    client
        .copy_object()
        .bucket(&bucket)
        .key("foo123bar2")
        .copy_source(format!("{plain_target_bucket}/bar321foo4"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &bucket, "foo123bar2").await?,
        body
    );

    Ok(())
}

/// Verifies CopyObject can copy exact versions created by multipart upload.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_versioning_multipart_upload() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_versioning_multipart_upload
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");
    let versioned_target_bucket = bucket_name("s3ite-copy-versioned");
    let plain_target_bucket = bucket_name("s3ite-copy-plain");
    let source_key = "foo123bar";
    let source_body = vec![b'a'; 6 * MIB];
    let content_type = "text/bla";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let completed =
        multipart_upload_with_metadata(&client, &bucket, source_key, source_body.clone()).await?;
    let source_version_id = completed
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing version ID"))?
        .to_string();

    let copied = client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!(
            "{bucket}/{source_key}?versionId={source_version_id}"
        ))
        .send()
        .await?;
    let copied_version_id = copied
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("copy in versioned bucket missing version ID"))?
        .to_string();
    assert_copied_object(&client, &bucket, "bar321foo", &source_body, content_type).await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo2")
        .copy_source(format!("{bucket}/bar321foo?versionId={copied_version_id}"))
        .send()
        .await?;
    assert_copied_object(&client, &bucket, "bar321foo2", &source_body, content_type).await?;

    create_bucket(&client, &versioned_target_bucket).await?;
    put_bucket_versioning(
        &client,
        &versioned_target_bucket,
        BucketVersioningStatus::Enabled,
    )
    .await?;
    client
        .copy_object()
        .bucket(&versioned_target_bucket)
        .key("bar321foo3")
        .copy_source(format!(
            "{bucket}/{source_key}?versionId={source_version_id}"
        ))
        .send()
        .await?;
    assert_copied_object(
        &client,
        &versioned_target_bucket,
        "bar321foo3",
        &source_body,
        content_type,
    )
    .await?;

    create_bucket(&client, &plain_target_bucket).await?;
    client
        .copy_object()
        .bucket(&plain_target_bucket)
        .key("bar321foo4")
        .copy_source(format!(
            "{bucket}/{source_key}?versionId={source_version_id}"
        ))
        .send()
        .await?;
    assert_copied_object(
        &client,
        &plain_target_bucket,
        "bar321foo4",
        &source_body,
        content_type,
    )
    .await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("foo123bar2")
        .copy_source(format!("{plain_target_bucket}/bar321foo4"))
        .send()
        .await?;
    assert_copied_object(&client, &bucket, "foo123bar2", &source_body, content_type).await?;

    Ok(())
}

/// Verifies CopyObject source version parsing handles URL-encoded keys.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_versioned_url_encoding() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_versioned_url_encoding
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-copy");
    let source_key = "foo?bar";
    let target_key = "bar&foo";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let source = client
        .put_object()
        .bucket(&bucket)
        .key(source_key)
        .body(ByteStream::from_static(b""))
        .send()
        .await?;
    let source_version_id = source
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("versioned PutObject missing version ID"))?;

    client
        .head_object()
        .bucket(&bucket)
        .key(source_key)
        .version_id(source_version_id)
        .send()
        .await?;
    client
        .copy_object()
        .bucket(&bucket)
        .key(target_key)
        .copy_source(format!(
            "{bucket}/{}?versionId={source_version_id}",
            raw_http::path_segment(source_key)
        ))
        .send()
        .await?;
    client
        .head_object()
        .bucket(&bucket)
        .key(target_key)
        .send()
        .await?;

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

async fn multipart_upload_with_metadata(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .metadata("foo", "bar")
        .content_type("text/bla")
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string();
    let mut completed_parts = Vec::new();

    for (index, chunk) in body.chunks(5 * MIB).enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let response = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(chunk.to_vec()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .e_tag(
                    response
                        .e_tag()
                        .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
                )
                .part_number(part_number)
                .build(),
        );
    }

    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();
    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await?)
}

async fn assert_copied_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_body: &[u8],
    expected_content_type: &str,
) -> Result<()> {
    let response = client.get_object().bucket(bucket).key(key).send().await?;
    assert_eq!(response.content_type(), Some(expected_content_type));
    let metadata = response
        .metadata()
        .ok_or_else(|| anyhow::anyhow!("copied object missing metadata"))?;
    assert_eq!(metadata.get("foo").map(String::as_str), Some("bar"));
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        expected_body
    );
    Ok(())
}
