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

fn simple_tagging(count: usize) -> Result<Tagging> {
    let tags = (0..count)
        .map(|index| tag(&index.to_string(), &index.to_string()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Tagging::builder().set_tag_set(Some(tags)).build()?)
}

fn exact_len_key(index: usize, len: usize) -> String {
    let prefix = index.to_string();
    format!("{prefix}{}", "k".repeat(len - prefix.len()))
}

fn tag_map(tags: &[Tag]) -> BTreeMap<String, String> {
    tags.iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect()
}

fn assert_tag_set(tags: &[Tag], expected: &[(&str, &str)]) {
    assert_eq!(
        tag_map(tags),
        expected
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    );
}

async fn create_object_with_body(key: &str) -> Result<(TestServer, aws_sdk_s3::Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging");
    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await?;
    Ok((server, client, bucket))
}

/// Verifies bucket tagging can be set, read, and deleted.
#[tokio::test]
#[serial(conformance)]
async fn test_set_bucket_tagging() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_set_bucket_tagging
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
        .tagging(tagging(&[("Hello", "World")])?)
        .send()
        .await?;
    let response = client.get_bucket_tagging().bucket(&bucket).send().await?;
    assert_tag_set(response.tag_set(), &[("Hello", "World")]);

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

/// Verifies CreateMultipartUpload tags are moved onto the completed object.
#[tokio::test]
#[serial(conformance)]
async fn test_set_multipart_tagging() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_set_multipart_tagging
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .tagging("Hello=World&foo=bar")
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow!("create multipart response missing upload ID"))?
        .to_string();
    let part = client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"A"))
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
        .key(key)
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
        .key(key)
        .send()
        .await?;
    assert_tag_set(response.tag_set(), &[("Hello", "World"), ("foo", "bar")]);

    client
        .delete_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert!(response.tag_set().is_empty());

    Ok(())
}

/// Verifies object tags can be read back after PutObjectTagging.
#[tokio::test]
#[serial(conformance)]
async fn test_get_obj_tagging() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_obj_tagging
    let (_server, client, bucket) = create_object_with_body("testputtags").await?;

    let input = simple_tagging(2)?;
    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputtags")
        .tagging(input.clone())
        .send()
        .await?;

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputtags")
        .send()
        .await?;
    assert_eq!(response.tag_set(), input.tag_set());

    Ok(())
}

/// Verifies HeadObject returns the x-amz-tagging-count value.
#[tokio::test]
#[serial(conformance)]
async fn test_get_obj_head_tagging() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_obj_head_tagging
    let (_server, client, bucket) = create_object_with_body("testputtags").await?;

    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputtags")
        .tagging(simple_tagging(2)?)
        .send()
        .await?;

    let response = client
        .head_object()
        .bucket(&bucket)
        .key("testputtags")
        .send()
        .await?;
    assert_eq!(response.tag_count(), Some(2));

    Ok(())
}

/// Verifies the maximum of 10 object tags is accepted.
#[tokio::test]
#[serial(conformance)]
async fn test_put_max_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_max_tags
    let (_server, client, bucket) = create_object_with_body("testputmaxtags").await?;
    let input = simple_tagging(10)?;

    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputmaxtags")
        .tagging(input.clone())
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmaxtags")
        .send()
        .await?;
    assert_eq!(response.tag_set(), input.tag_set());

    Ok(())
}

/// Verifies more than 10 object tags are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_put_excess_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_excess_tags
    let (_server, client, bucket) = create_object_with_body("testputmaxtags").await?;

    let error = client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputmaxtags")
        .tagging(simple_tagging(11)?)
        .send()
        .await
        .expect_err("more than 10 object tags should fail");
    assert_error_contains(&error, "InvalidTag");

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmaxtags")
        .send()
        .await?;
    assert!(response.tag_set().is_empty());

    Ok(())
}

/// Verifies maximum key/value tag lengths are accepted.
#[tokio::test]
#[serial(conformance)]
async fn test_put_max_kvsize_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_max_kvsize_tags
    let (_server, client, bucket) = create_object_with_body("testputmaxkeysize").await?;
    let tags = (0..10)
        .map(|index| tag(&exact_len_key(index, 128), &"v".repeat(256)))
        .collect::<Result<Vec<_>>>()?;
    let input = Tagging::builder().set_tag_set(Some(tags.clone())).build()?;

    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputmaxkeysize")
        .tagging(input)
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmaxkeysize")
        .send()
        .await?;
    assert_eq!(tag_map(response.tag_set()), tag_map(&tags));

    Ok(())
}

/// Verifies overlong object tag keys are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_put_excess_key_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_excess_key_tags
    let (_server, client, bucket) = create_object_with_body("testputexcesskeytags").await?;
    let tags = (0..10)
        .map(|index| tag(&exact_len_key(index, 129), &"v".repeat(256)))
        .collect::<Result<Vec<_>>>()?;

    let error = client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputexcesskeytags")
        .tagging(Tagging::builder().set_tag_set(Some(tags)).build()?)
        .send()
        .await
        .expect_err("overlong tag keys should fail");
    assert_error_contains(&error, "InvalidTag");

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputexcesskeytags")
        .send()
        .await?;
    assert!(response.tag_set().is_empty());

    Ok(())
}

/// Verifies overlong object tag values are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_put_excess_val_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_excess_val_tags
    let (_server, client, bucket) = create_object_with_body("testputexcesskeytags").await?;
    let tags = (0..10)
        .map(|index| tag(&exact_len_key(index, 128), &"v".repeat(257)))
        .collect::<Result<Vec<_>>>()?;

    let error = client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputexcesskeytags")
        .tagging(Tagging::builder().set_tag_set(Some(tags)).build()?)
        .send()
        .await
        .expect_err("overlong tag values should fail");
    assert_error_contains(&error, "InvalidTag");

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputexcesskeytags")
        .send()
        .await?;
    assert!(response.tag_set().is_empty());

    Ok(())
}

/// Verifies PutObjectTagging replaces the full tag set.
#[tokio::test]
#[serial(conformance)]
async fn test_put_modify_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_modify_tags
    let (_server, client, bucket) = create_object_with_body("testputmodifytags").await?;

    let first = tagging(&[("key", "val"), ("key2", "val2")])?;
    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .tagging(first.clone())
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .send()
        .await?;
    assert_eq!(response.tag_set(), first.tag_set());

    let second = tagging(&[("key3", "val3")])?;
    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .tagging(second.clone())
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .send()
        .await?;
    assert_eq!(response.tag_set(), second.tag_set());

    Ok(())
}

/// Verifies DeleteObjectTagging removes all tags.
#[tokio::test]
#[serial(conformance)]
async fn test_put_delete_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_delete_tags
    let (_server, client, bucket) = create_object_with_body("testputmodifytags").await?;
    let input = simple_tagging(2)?;

    client
        .put_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .tagging(input.clone())
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .send()
        .await?;
    assert_eq!(response.tag_set(), input.tag_set());

    client
        .delete_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .send()
        .await?;
    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key("testputmodifytags")
        .send()
        .await?;
    assert!(response.tag_set().is_empty());

    Ok(())
}

/// Verifies PutObject accepts x-amz-tagging header syntax.
#[tokio::test]
#[serial(conformance)]
async fn test_put_obj_with_tags() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_obj_with_tags
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-tagging");
    let key = "testtagobj1";
    let data = "A".repeat(100);

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .tagging("foo=bar&bar")
        .body(ByteStream::from(data.clone().into_bytes()))
        .send()
        .await?;
    let object = client.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        object.body.collect().await?.into_bytes().as_ref(),
        data.as_bytes()
    );

    let response = client
        .get_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_tag_set(response.tag_set(), &[("bar", ""), ("foo", "bar")]);

    Ok(())
}

/// Verifies bucket tagging validation rejects duplicate keys and too many tags.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_bucket_tagging_rejects_invalid_tags() -> Result<()> {
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

    let too_many = client
        .put_bucket_tagging()
        .bucket(&bucket)
        .tagging(simple_tagging(11)?)
        .send()
        .await
        .expect_err("too many bucket tags should fail");
    assert_error_contains(&too_many, "InvalidTag");

    Ok(())
}

/// Verifies bucket tagging reports normal missing-bucket and read-only errors.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_bucket_tagging_missing_and_read_only_errors() -> Result<()> {
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

/// Verifies tags are attached to object versions, not just keys.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_object_tagging_version_isolation() -> Result<()> {
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
    assert_tag_set(old.tag_set(), &[("stage", "old")]);
    assert_tag_set(latest.tag_set(), &[("stage", "two")]);

    Ok(())
}

/// Verifies CopyObject copies tags by default and can replace them.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_object_tagging_copy_directives() -> Result<()> {
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
    assert_tag_set(copied.tag_set(), &[("a", "one"), ("b", "two")]);

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
    assert_tag_set(replaced.tag_set(), &[("c", "three")]);

    Ok(())
}

/// Verifies duplicate x-amz-tagging keys are rejected without object creation.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_object_tagging_rejects_duplicate_header_keys() -> Result<()> {
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

    Ok(())
}
