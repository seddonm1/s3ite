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
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_encoding_basic
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

    let response = client
        .list_objects_v2()
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
    assert_eq!(response.key_count(), Some(4));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_special_characters() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_alt
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_percentage
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_whitespace
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_dot
    // Mirrors the corresponding list_objects_v2 variants.
    for (delimiter, keys, expected_prefixes) in [
        ("a", ["bar", "baz", "cab", "foo"], ["ba", "ca"]),
        ("%", ["b%ar", "b%az", "c%ab", "foo"], ["b%", "c%"]),
        (" ", ["b ar", "b az", "c ab", "foo"], ["b ", "c "]),
        (".", ["b.ar", "b.az", "c.ab", "foo"], ["b.", "c."]),
    ] {
        let server = TestServer::spawn().await?;
        let client = sdk::client(&server);
        let bucket = bucket_name("s3ite-list");

        create_bucket(&client, &bucket).await?;
        put_keys(&client, &bucket, &keys).await?;

        let response = client
            .list_objects()
            .bucket(&bucket)
            .delimiter(delimiter)
            .send()
            .await?;
        assert_eq!(response.delimiter(), Some(delimiter));
        assert_eq!(object_keys(response.contents()), vec!["foo"]);
        assert_eq!(
            prefixes(response.common_prefixes()),
            expected_prefixes.to_vec()
        );

        let response = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter(delimiter)
            .send()
            .await?;
        assert_eq!(response.delimiter(), Some(delimiter));
        assert_eq!(object_keys(response.contents()), vec!["foo"]);
        assert_eq!(
            prefixes(response.common_prefixes()),
            expected_prefixes.to_vec()
        );
    }

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_empty_none_and_missing() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_empty
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_none
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_not_exist
    // Mirrors the corresponding list_objects_v2 variants.
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");
    let keys = ["bar", "baz", "cab", "foo"];

    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, &keys).await?;

    let empty = client
        .list_objects()
        .bucket(&bucket)
        .delimiter("")
        .send()
        .await?;
    assert_eq!(empty.delimiter(), None);
    assert_eq!(object_keys(empty.contents()), keys.to_vec());
    assert!(empty.common_prefixes().is_empty());

    let no_delimiter = client.list_objects().bucket(&bucket).send().await?;
    assert_eq!(no_delimiter.delimiter(), None);
    assert_eq!(object_keys(no_delimiter.contents()), keys.to_vec());
    assert!(no_delimiter.common_prefixes().is_empty());

    let missing = client
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await?;
    assert_eq!(missing.delimiter(), Some("/"));
    assert_eq!(object_keys(missing.contents()), keys.to_vec());
    assert!(missing.common_prefixes().is_empty());

    let empty_v2 = client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("")
        .send()
        .await?;
    assert_eq!(empty_v2.delimiter(), None);
    assert_eq!(object_keys(empty_v2.contents()), keys.to_vec());
    assert!(empty_v2.common_prefixes().is_empty());

    let missing_v2 = client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await?;
    assert_eq!(missing_v2.delimiter(), Some("/"));
    assert_eq!(object_keys(missing_v2.contents()), keys.to_vec());
    assert!(missing_v2.common_prefixes().is_empty());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_basic_alt_empty_missing() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_basic
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_alt
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_empty
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_not_exist
    // Mirrors the corresponding list_objects_v2 variants.
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(
        &client,
        &bucket,
        &["bar", "baz", "foo/bar", "foo/baz", "quux"],
    )
    .await?;

    let prefixed = client
        .list_objects()
        .bucket(&bucket)
        .prefix("foo/")
        .send()
        .await?;
    assert_eq!(prefixed.prefix(), Some("foo/"));
    assert_eq!(object_keys(prefixed.contents()), vec!["foo/bar", "foo/baz"]);
    assert!(prefixed.common_prefixes().is_empty());

    let prefixed_v2 = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("foo/")
        .send()
        .await?;
    assert_eq!(prefixed_v2.prefix(), Some("foo/"));
    assert_eq!(
        object_keys(prefixed_v2.contents()),
        vec!["foo/bar", "foo/baz"]
    );
    assert!(prefixed_v2.common_prefixes().is_empty());

    let alt = client
        .list_objects()
        .bucket(&bucket)
        .prefix("ba")
        .send()
        .await?;
    assert_eq!(alt.prefix(), Some("ba"));
    assert_eq!(object_keys(alt.contents()), vec!["bar", "baz"]);

    let empty = client
        .list_objects()
        .bucket(&bucket)
        .prefix("")
        .send()
        .await?;
    assert_eq!(empty.prefix(), Some(""));
    assert_eq!(
        object_keys(empty.contents()),
        vec!["bar", "baz", "foo/bar", "foo/baz", "quux"]
    );

    let missing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("missing")
        .send()
        .await?;
    assert_eq!(missing.prefix(), Some("missing"));
    assert!(missing.contents().is_empty());
    assert!(missing.common_prefixes().is_empty());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_delimiter_basic
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_delimiter_basic
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");

    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, &["foo/bar", "foo/baz/xyzzy", "quux/thud"]).await?;

    let response = client
        .list_objects()
        .bucket(&bucket)
        .prefix("foo/")
        .delimiter("/")
        .send()
        .await?;
    assert_eq!(response.prefix(), Some("foo/"));
    assert_eq!(response.delimiter(), Some("/"));
    assert_eq!(object_keys(response.contents()), vec!["foo/bar"]);
    assert_eq!(prefixes(response.common_prefixes()), vec!["foo/baz/"]);

    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("foo/")
        .delimiter("/")
        .send()
        .await?;
    assert_eq!(response.prefix(), Some("foo/"));
    assert_eq!(response.delimiter(), Some("/"));
    assert_eq!(object_keys(response.contents()), vec!["foo/bar"]);
    assert_eq!(prefixes(response.common_prefixes()), vec!["foo/baz/"]);

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
