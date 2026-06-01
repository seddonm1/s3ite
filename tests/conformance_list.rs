//! Real-binary HTTP list-object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_list`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::Result;
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{CommonPrefix, EncodingType, Object},
};
use serial_test::serial;
use support::{
    fixtures::{bucket_name, create_bucket},
    sdk,
    server::TestServer,
};

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_many() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_many
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, &["foo", "bar", "baz"]).await?;

    let first = client
        .list_objects()
        .bucket(&bucket)
        .max_keys(2)
        .send()
        .await?;
    assert_eq!(object_keys(first.contents()), vec!["bar", "baz"]);
    assert_eq!(first.is_truncated(), Some(true));

    let second = client
        .list_objects()
        .bucket(&bucket)
        .marker("baz")
        .max_keys(2)
        .send()
        .await?;
    assert_eq!(object_keys(second.contents()), vec!["foo"]);
    assert_eq!(second.is_truncated(), Some(false));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_many() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_many
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, &["foo", "bar", "baz"]).await?;

    let first = client
        .list_objects_v2()
        .bucket(&bucket)
        .max_keys(2)
        .send()
        .await?;
    assert_eq!(object_keys(first.contents()), vec!["bar", "baz"]);
    assert_eq!(first.is_truncated(), Some(true));
    assert_eq!(first.key_count(), Some(2));

    let second = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("baz")
        .max_keys(2)
        .send()
        .await?;
    assert_eq!(object_keys(second.contents()), vec!["foo"]);
    assert_eq!(second.is_truncated(), Some(false));
    assert_eq!(second.key_count(), Some(1));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_basic
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(
        &client,
        &bucket,
        &["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"],
    )
    .await?;

    let response = client
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await?;
    assert_eq!(response.delimiter(), Some("/"));
    assert_eq!(object_keys(response.contents()), vec!["asdf"]);
    assert_eq!(prefixes(response.common_prefixes()), vec!["foo/", "quux/"]);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_basic
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(
        &client,
        &bucket,
        &["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"],
    )
    .await?;

    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await?;
    assert_eq!(response.delimiter(), Some("/"));
    assert_eq!(object_keys(response.contents()), vec!["asdf"]);
    assert_eq!(prefixes(response.common_prefixes()), vec!["foo/", "quux/"]);
    assert_eq!(response.key_count(), Some(3));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_encoding_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_encoding_basic
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(
        &client,
        &bucket,
        &["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"],
    )
    .await?;

    let response = client
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .encoding_type(EncodingType::Url)
        .send()
        .await?;
    assert_eq!(response.delimiter(), Some("/"));
    assert_eq!(object_keys(response.contents()), vec!["asdf%2Bb"]);
    assert_eq!(
        prefixes(response.common_prefixes()),
        vec!["foo%2B1/", "foo/", "quux%20ab/"]
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_unreadable
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, &["foo/bar", "foo/baz", "quux"]).await?;

    let response = client
        .list_objects()
        .bucket(&bucket)
        .prefix("\n")
        .send()
        .await?;
    assert_eq!(response.prefix(), Some("\n"));
    assert!(response.contents().is_empty());
    assert!(response.common_prefixes().is_empty());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_unreadable
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, &["foo/bar", "foo/baz", "quux"]).await?;

    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("\n")
        .send()
        .await?;
    assert_eq!(response.prefix(), Some("\n"));
    assert!(response.contents().is_empty());
    assert!(response.common_prefixes().is_empty());

    Ok(())
}

async fn put_keys(client: &Client, bucket: &str, keys: &[&'static str]) -> Result<()> {
    for key in keys {
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(ByteStream::from_static(key.as_bytes()))
            .send()
            .await?;
    }
    Ok(())
}

fn object_keys(objects: &[Object]) -> Vec<&str> {
    objects.iter().filter_map(Object::key).collect()
}

fn prefixes(common_prefixes: &[CommonPrefix]) -> Vec<&str> {
    common_prefixes
        .iter()
        .filter_map(CommonPrefix::prefix)
        .collect()
}
