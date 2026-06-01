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

/// Verifies ListObjects v1 ordering, max-keys truncation, and markers.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_many() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_many
    let (_server, client, bucket) = seeded_bucket(&["foo", "bar", "baz"]).await?;

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

/// Verifies ListObjectsV2 ordering, max-keys truncation, and start-after.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_many() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_many
    let (_server, client, bucket) = seeded_bucket(&["foo", "bar", "baz"]).await?;

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

/// Verifies ListObjects v1 slash delimiters emit contents and common prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_basic
    delimiter_case_v1(
        "/",
        Some("/"),
        &["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"],
        &["asdf"],
        &["foo/", "quux/"],
    )
    .await
}

/// Verifies ListObjectsV2 slash delimiters emit contents, prefixes, and key count.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_basic
    delimiter_case_v2(
        "/",
        Some("/"),
        &["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"],
        &["asdf"],
        &["foo/", "quux/"],
    )
    .await
}

/// Verifies URL encoding applies to returned ListObjects v1 keys and prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_encoding_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_encoding_basic
    encoding_case_v1().await
}

/// Verifies URL encoding applies to returned ListObjectsV2 keys and prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_encoding_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_encoding_basic
    encoding_case_v2().await
}

/// Verifies ListObjects v1 alternate delimiter characters group prefixes literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_alt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_alt
    delimiter_case_v1(
        "a",
        Some("a"),
        &["bar", "baz", "cab", "foo"],
        &["foo"],
        &["ba", "ca"],
    )
    .await
}

/// Verifies ListObjectsV2 alternate delimiter characters group prefixes literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_alt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_alt
    delimiter_case_v2(
        "a",
        Some("a"),
        &["bar", "baz", "cab", "foo"],
        &["foo"],
        &["ba", "ca"],
    )
    .await
}

/// Verifies ListObjects v1 percent delimiters are treated literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_percentage() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_percentage
    delimiter_case_v1(
        "%",
        Some("%"),
        &["b%ar", "b%az", "c%ab", "foo"],
        &["foo"],
        &["b%", "c%"],
    )
    .await
}

/// Verifies ListObjectsV2 percent delimiters are treated literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_percentage() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_percentage
    delimiter_case_v2(
        "%",
        Some("%"),
        &["b%ar", "b%az", "c%ab", "foo"],
        &["foo"],
        &["b%", "c%"],
    )
    .await
}

/// Verifies ListObjects v1 whitespace delimiters are treated literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_whitespace() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_whitespace
    delimiter_case_v1(
        " ",
        Some(" "),
        &["b ar", "b az", "c ab", "foo"],
        &["foo"],
        &["b ", "c "],
    )
    .await
}

/// Verifies ListObjectsV2 whitespace delimiters are treated literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_whitespace() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_whitespace
    delimiter_case_v2(
        " ",
        Some(" "),
        &["b ar", "b az", "c ab", "foo"],
        &["foo"],
        &["b ", "c "],
    )
    .await
}

/// Verifies ListObjects v1 dot delimiters are treated literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_dot() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_dot
    delimiter_case_v1(
        ".",
        Some("."),
        &["b.ar", "b.az", "c.ab", "foo"],
        &["foo"],
        &["b.", "c."],
    )
    .await
}

/// Verifies ListObjectsV2 dot delimiters are treated literally.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_dot() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_dot
    delimiter_case_v2(
        ".",
        Some("."),
        &["b.ar", "b.az", "c.ab", "foo"],
        &["foo"],
        &["b.", "c."],
    )
    .await
}

/// Verifies ListObjects v1 empty delimiters do not group keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_empty
    delimiter_case_v1(
        "",
        None,
        &["bar", "baz", "cab", "foo"],
        &["bar", "baz", "cab", "foo"],
        &[],
    )
    .await
}

/// Verifies ListObjectsV2 empty delimiters do not group keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_empty
    delimiter_case_v2(
        "",
        None,
        &["bar", "baz", "cab", "foo"],
        &["bar", "baz", "cab", "foo"],
        &[],
    )
    .await
}

/// Verifies ListObjects v1 without a delimiter returns all keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_none
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "cab", "foo"]).await?;
    let response = client.list_objects().bucket(&bucket).send().await?;
    assert_eq!(response.delimiter(), None);
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "cab", "foo"]
    );
    assert!(response.common_prefixes().is_empty());
    Ok(())
}

/// Verifies ListObjectsV2 without a delimiter returns all keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_none
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "cab", "foo"]).await?;
    let response = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(response.delimiter(), None);
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "cab", "foo"]
    );
    assert!(response.common_prefixes().is_empty());
    Ok(())
}

/// Verifies ListObjects v1 with an unmatched delimiter returns all keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_not_exist
    delimiter_case_v1(
        "/",
        Some("/"),
        &["bar", "baz", "cab", "foo"],
        &["bar", "baz", "cab", "foo"],
        &[],
    )
    .await
}

/// Verifies ListObjectsV2 with an unmatched delimiter returns all keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_not_exist
    delimiter_case_v2(
        "/",
        Some("/"),
        &["bar", "baz", "cab", "foo"],
        &["bar", "baz", "cab", "foo"],
        &[],
    )
    .await
}

/// Verifies ListObjects v1 prefix filtering with slash-style keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_basic
    prefix_case_v1(
        "foo/",
        &["foo/bar", "foo/baz", "quux"],
        &["foo/bar", "foo/baz"],
    )
    .await
}

/// Verifies ListObjectsV2 prefix filtering with slash-style keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_basic
    prefix_case_v2(
        "foo/",
        &["foo/bar", "foo/baz", "quux"],
        &["foo/bar", "foo/baz"],
    )
    .await
}

/// Verifies ListObjects v1 prefix filtering with non-slash prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_alt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_alt
    prefix_case_v1("ba", &["bar", "baz", "foo"], &["bar", "baz"]).await
}

/// Verifies ListObjectsV2 prefix filtering with non-slash prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_alt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_alt
    prefix_case_v2("ba", &["bar", "baz", "foo"], &["bar", "baz"]).await
}

/// Verifies ListObjects v1 empty prefixes return all keys and echo empty prefix.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_empty
    prefix_case_v1(
        "",
        &["foo/bar", "foo/baz", "quux"],
        &["foo/bar", "foo/baz", "quux"],
    )
    .await
}

/// Verifies ListObjectsV2 empty prefixes return all keys and echo empty prefix.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_empty
    prefix_case_v2(
        "",
        &["foo/bar", "foo/baz", "quux"],
        &["foo/bar", "foo/baz", "quux"],
    )
    .await
}

/// Verifies ListObjects v1 missing prefixes return no keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_not_exist
    prefix_case_v1("d", &["foo/bar", "foo/baz", "quux"], &[]).await
}

/// Verifies ListObjectsV2 missing prefixes return no keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_not_exist
    prefix_case_v2("d", &["foo/bar", "foo/baz", "quux"], &[]).await
}

/// Verifies ListObjects v1 prefix and delimiter together split matching keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_delimiter_basic
    prefix_delimiter_case_v1().await
}

/// Verifies ListObjectsV2 prefix and delimiter together split matching keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_delimiter_basic() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_delimiter_basic
    prefix_delimiter_case_v2().await
}

/// Verifies ListObjects v1 echoes unreadable prefixes without URL encoding.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_unreadable
    let (_server, client, bucket) = seeded_bucket(&["foo/bar", "foo/baz", "quux"]).await?;
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

/// Verifies ListObjectsV2 echoes unreadable prefixes without URL encoding.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_unreadable
    let (_server, client, bucket) = seeded_bucket(&["foo/bar", "foo/baz", "quux"]).await?;
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

async fn seeded_bucket(keys: &[&'static str]) -> Result<(TestServer, Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");
    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, keys).await?;
    Ok((server, client, bucket))
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

async fn delimiter_case_v1(
    delimiter: &str,
    expected_delimiter: Option<&str>,
    keys: &[&'static str],
    expected_keys: &[&str],
    expected_prefixes: &[&str],
) -> Result<()> {
    let (_server, client, bucket) = seeded_bucket(keys).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .delimiter(delimiter)
        .send()
        .await?;
    assert_eq!(response.delimiter(), expected_delimiter);
    assert_eq!(object_keys(response.contents()), expected_keys.to_vec());
    assert_eq!(
        prefixes(response.common_prefixes()),
        expected_prefixes.to_vec()
    );
    Ok(())
}

async fn delimiter_case_v2(
    delimiter: &str,
    expected_delimiter: Option<&str>,
    keys: &[&'static str],
    expected_keys: &[&str],
    expected_prefixes: &[&str],
) -> Result<()> {
    let (_server, client, bucket) = seeded_bucket(keys).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter(delimiter)
        .send()
        .await?;
    assert_eq!(response.delimiter(), expected_delimiter);
    assert_eq!(object_keys(response.contents()), expected_keys.to_vec());
    assert_eq!(
        prefixes(response.common_prefixes()),
        expected_prefixes.to_vec()
    );
    assert_eq!(
        response.key_count(),
        Some((expected_keys.len() + expected_prefixes.len()) as i32)
    );
    Ok(())
}

async fn encoding_case_v1() -> Result<()> {
    let (_server, client, bucket) =
        seeded_bucket(&["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]).await?;
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

async fn encoding_case_v2() -> Result<()> {
    let (_server, client, bucket) =
        seeded_bucket(&["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]).await?;
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

async fn prefix_case_v1(prefix: &str, keys: &[&'static str], expected_keys: &[&str]) -> Result<()> {
    let (_server, client, bucket) = seeded_bucket(keys).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .prefix(prefix)
        .send()
        .await?;
    assert_eq!(response.prefix(), Some(prefix));
    assert_eq!(object_keys(response.contents()), expected_keys.to_vec());
    assert!(response.common_prefixes().is_empty());
    Ok(())
}

async fn prefix_case_v2(prefix: &str, keys: &[&'static str], expected_keys: &[&str]) -> Result<()> {
    let (_server, client, bucket) = seeded_bucket(keys).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(prefix)
        .send()
        .await?;
    assert_eq!(response.prefix(), Some(prefix));
    assert_eq!(object_keys(response.contents()), expected_keys.to_vec());
    assert!(response.common_prefixes().is_empty());
    Ok(())
}

async fn prefix_delimiter_case_v1() -> Result<()> {
    let (_server, client, bucket) =
        seeded_bucket(&["foo/bar", "foo/baz/xyzzy", "quux/thud"]).await?;
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
    Ok(())
}

async fn prefix_delimiter_case_v2() -> Result<()> {
    let (_server, client, bucket) =
        seeded_bucket(&["foo/bar", "foo/baz/xyzzy", "quux/thud"]).await?;
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
    assert_eq!(response.key_count(), Some(2));
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
