//! Real-binary HTTP object tagging conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_tagging`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{
        BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Tag, Tagging,
        TaggingDirective,
    },
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, put_bucket_versioning},
    sdk,
    server::{TestServer, TestServerOptions},
};

fn tag(key: &str, value: &str) -> Result<Tag> {
    Ok(Tag::builder().key(key).value(value).build()?)
}

fn tagging(tags: &[(&str, &str)]) -> Result<Tagging> {
    let mut builder = Tagging::builder();
    for (key, value) in tags {
        builder = builder.tag_set(tag(key, value)?);
    }
    Ok(builder.build()?)
}

fn tag_map(tags: &[Tag]) -> BTreeMap<String, String> {
    tags.iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect()
}

/// Verifies bucket tags can be replaced, read back, and deleted.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_tagging_put_get_delete() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-bucket-tagging");

    create_bucket(&client, &bucket).await?;
    let missing = client
        .get_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("empty bucket tag set should fail");
    assert_error_contains(&missing, "NoSuchTagSet");

    client
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(tagging(&[("team", "red"), ("space", "a b")])?)
        .send()
        .await?;
    let response = client.get_bucket_tagging().bucket(&bucket).send().await?;
    assert_eq!(
        tag_map(response.tag_set()),
        BTreeMap::from([
            ("space".to_string(), "a b".to_string()),
            ("team".to_string(), "red".to_string()),
        ])
    );

    client
        .delete_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await?;
    let deleted = client
        .get_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("deleted bucket tag set should fail");
    assert_error_contains(&deleted, "NoSuchTagSet");

    Ok(())
}

/// Verifies bucket tagging validation rejects duplicate keys and too many tags.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_tagging_rejects_invalid_tags() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-bkt-tag-invalid");

    create_bucket(&client, &bucket).await?;
    let duplicate = client
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(tagging(&[("a", "one"), ("a", "two")])?)
        .send()
        .await
        .expect_err("duplicate bucket tag keys should fail");
    assert_error_contains(&duplicate, "InvalidTag");

    let too_many_tags = (0..11)
        .map(|index| tag(&index.to_string(), "value"))
        .collect::<Result<Vec<_>>>()?;
    let too_many = client
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(
            Tagging::builder()
                .set_tag_set(Some(too_many_tags))
                .build()?,
        )
        .send()
        .await
        .expect_err("too many bucket tags should fail");
    assert_error_contains(&too_many, "InvalidTag");

    Ok(())
}

/// Verifies bucket tagging reports normal missing-bucket and read-only errors.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_tagging_missing_and_read_only_errors() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-bkt-tag-miss");

    let missing = client
        .get_bucket_tagging()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("missing bucket should fail");
    assert_error_contains(&missing, "NoSuchBucket");

    let read_only_server = TestServer::spawn_with(TestServerOptions {
        config: Some("read_only: true\n".to_string()),
        ..Default::default()
    })
    .await?;
    let read_only_client = sdk::client(&read_only_server);
    let read_only = read_only_client
        .put_bucket_tagging()
        .bucket(bucket_name("s3ite-bkt-tag-ro"))
        .tagging(tagging(&[("team", "red")])?)
        .send()
        .await
        .expect_err("read-only bucket tag mutation should fail");
    assert_error_contains(&read_only, "MethodNotAllowed");

    Ok(())
}

/// Verifies PutObject tags, object tagging replacement, and tag deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_object_tagging_put_get_replace_delete() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .tagging("team=red&space=a+b")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(
        tag_map(response.tag_set()),
        BTreeMap::from([
            ("space".to_string(), "a b".to_string()),
            ("team".to_string(), "red".to_string()),
        ])
    );

    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("object")
        .tagging(tagging(&[("team", "blue")])?)
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(
        tag_map(response.tag_set()),
        BTreeMap::from([("team".to_string(), "blue".to_string())])
    );

    client
        .delete_object_tagging()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert!(response.tag_set().is_empty());

    Ok(())
}

/// Verifies tags are attached to object versions, not just keys.
#[tokio::test]
#[serial(conformance)]
async fn test_object_tagging_version_isolation() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging-ver");

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;

    let v1 = client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .tagging("stage=one")
        .body(ByteStream::from_static(b"one"))
        .send()
        .await?
        .version_id()
        .ok_or_else(|| anyhow!("missing first version ID"))?
        .to_string();
    let v2 = client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .tagging("stage=two")
        .body(ByteStream::from_static(b"two"))
        .send()
        .await?
        .version_id()
        .ok_or_else(|| anyhow!("missing second version ID"))?
        .to_string();

    let current = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(
        tag_map(current.tag_set()),
        BTreeMap::from([("stage".to_string(), "two".to_string())])
    );

    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("object")
        .version_id(&v1)
        .tagging(tagging(&[("stage", "old")])?)
        .send()
        .await?;

    let old = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .version_id(&v1)
        .send()
        .await?;
    let latest = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .version_id(&v2)
        .send()
        .await?;
    assert_eq!(
        tag_map(old.tag_set()),
        BTreeMap::from([("stage".to_string(), "old".to_string())])
    );
    assert_eq!(
        tag_map(latest.tag_set()),
        BTreeMap::from([("stage".to_string(), "two".to_string())])
    );

    Ok(())
}

/// Verifies CopyObject copies tags by default and can replace them.
#[tokio::test]
#[serial(conformance)]
async fn test_object_tagging_copy_directives() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging-copy");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("source")
        .tagging("a=one&b=two")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("copy")
        .copy_source(format!("{bucket}/source"))
        .send()
        .await?;
    let copied = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("copy")
        .send()
        .await?;
    assert_eq!(
        tag_map(copied.tag_set()),
        BTreeMap::from([
            ("a".to_string(), "one".to_string()),
            ("b".to_string(), "two".to_string()),
        ])
    );

    client
        .copy_object()
        .bucket(&bucket)
        .key("replaced")
        .copy_source(format!("{bucket}/source"))
        .tagging_directive(TaggingDirective::Replace)
        .tagging("c=three")
        .send()
        .await?;
    let replaced = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("replaced")
        .send()
        .await?;
    assert_eq!(
        tag_map(replaced.tag_set()),
        BTreeMap::from([("c".to_string(), "three".to_string())])
    );

    client
        .copy_object()
        .bucket(&bucket)
        .key("empty")
        .copy_source(format!("{bucket}/source"))
        .tagging_directive(TaggingDirective::Replace)
        .send()
        .await?;
    let empty = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("empty")
        .send()
        .await?;
    assert!(empty.tag_set().is_empty());

    Ok(())
}

/// Verifies CreateMultipartUpload tags become completed object tags.
#[tokio::test]
#[serial(conformance)]
async fn test_object_tagging_multipart_completion() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging-mpu");

    create_bucket(&client, &bucket).await?;
    let upload_id = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("object")
        .tagging("mp=yes")
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow!("create multipart response missing upload ID"))?
        .to_string();
    let part = client
        .upload_part()
        .bucket(&bucket)
        .key("object")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    let completed = CompletedPart::builder()
        .part_number(1)
        .e_tag(
            part.e_tag()
                .ok_or_else(|| anyhow!("upload part response missing ETag"))?,
        )
        .build();
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("object")
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(vec![completed]))
                .build(),
        )
        .send()
        .await?;

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(
        tag_map(response.tag_set()),
        BTreeMap::from([("mp".to_string(), "yes".to_string())])
    );

    Ok(())
}

/// Verifies invalid tag requests fail before storing object tag state.
#[tokio::test]
#[serial(conformance)]
async fn test_object_tagging_rejects_invalid_tags() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging-invalid");

    create_bucket(&client, &bucket).await?;
    let duplicate = client
        .put_object()
        .bucket(&bucket)
        .key("duplicate")
        .tagging("a=one&a=two")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await
        .expect_err("duplicate tag keys should fail");
    assert_error_contains(&duplicate, "InvalidTag");

    client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    let too_many_tags = (0..11)
        .map(|index| tag(&index.to_string(), "value"))
        .collect::<Result<Vec<_>>>()?;
    let too_many = client
        .put_object_tagging()
        .bucket(&bucket)
        .key("object")
        .tagging(
            Tagging::builder()
                .set_tag_set(Some(too_many_tags))
                .build()?,
        )
        .send()
        .await
        .expect_err("too many tags should fail");
    assert_error_contains(&too_many, "InvalidTag");

    Ok(())
}
