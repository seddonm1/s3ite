//! Real-binary HTTP list-object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_list`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::Result;
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{BucketVersioningStatus, CommonPrefix, EncodingType, Object},
};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::assert_status,
    fixtures::{bucket_name, create_bucket, put_bucket_versioning},
    raw_http, sdk,
    server::TestServer,
};

struct ListPageV1<'a> {
    prefix: &'a str,
    delimiter: &'a str,
    marker: &'a str,
    max_keys: i32,
    is_truncated: bool,
    expected_keys: &'a [&'a str],
    expected_prefixes: &'a [&'a str],
    next_marker: Option<&'a str>,
}

struct ListPageV2<'a> {
    prefix: &'a str,
    delimiter: &'a str,
    continuation_token: Option<&'a str>,
    max_keys: i32,
    is_truncated: bool,
    expected_keys: &'a [&'a str],
    expected_prefixes: &'a [&'a str],
}

macro_rules! validate_bucket_list_v1 {
    (
        $client:expr,
        $bucket:expr,
        $prefix:expr,
        $delimiter:expr,
        $marker:expr,
        $max_keys:expr,
        $is_truncated:expr,
        $expected_keys:expr,
        $expected_prefixes:expr,
        $next_marker:expr $(,)?
    ) => {
        validate_bucket_list_v1_page(
            $client,
            $bucket,
            ListPageV1 {
                prefix: $prefix,
                delimiter: $delimiter,
                marker: $marker,
                max_keys: $max_keys,
                is_truncated: $is_truncated,
                expected_keys: $expected_keys,
                expected_prefixes: $expected_prefixes,
                next_marker: $next_marker,
            },
        )
    };
}

macro_rules! validate_bucket_list_v2 {
    (
        $client:expr,
        $bucket:expr,
        $prefix:expr,
        $delimiter:expr,
        $continuation_token:expr,
        $max_keys:expr,
        $is_truncated:expr,
        $expected_keys:expr,
        $expected_prefixes:expr $(,)?
    ) => {
        validate_bucket_list_v2_page(
            $client,
            $bucket,
            ListPageV2 {
                prefix: $prefix,
                delimiter: $delimiter,
                continuation_token: $continuation_token,
                max_keys: $max_keys,
                is_truncated: $is_truncated,
                expected_keys: $expected_keys,
                expected_prefixes: $expected_prefixes,
            },
        )
    };
}

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

/// Verifies ListObjectsV2 only lists objects from the requested bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_distinct() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_distinct
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket1 = bucket_name("s3ite-list");
    let bucket2 = bucket_name("s3ite-list");
    create_bucket(&client, &bucket1).await?;
    create_bucket(&client, &bucket2).await?;
    put_keys(&client, &bucket1, &["asdf"]).await?;

    let response = client.list_objects_v2().bucket(&bucket2).send().await?;
    assert!(response.contents().is_empty());

    Ok(())
}

/// Verifies ListObjectsV2 reports KeyCount for listed objects.
#[tokio::test]
#[serial(conformance)]
async fn test_basic_key_count() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_basic_key_count
    let (_server, client, bucket) = seeded_bucket(&["0", "1", "2", "3", "4"]).await?;
    let response = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(response.key_count(), Some(5));
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

/// Verifies ListObjects v1 paginates mixed contents and common prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_prefix() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_prefix
    let (_server, client, bucket) = seeded_bucket(&[
        "asdf",
        "boo/bar",
        "boo/baz/xyzzy",
        "cquux/thud",
        "cquux/bla",
    ])
    .await?;

    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        "",
        1,
        true,
        &["asdf"],
        &[],
        Some("asdf"),
    )
    .await?;
    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        &marker,
        1,
        true,
        &[],
        &["boo/"],
        Some("boo/"),
    )
    .await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        &marker,
        1,
        false,
        &[],
        &["cquux/"],
        None,
    )
    .await?;

    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        "",
        2,
        true,
        &["asdf"],
        &["boo/"],
        Some("boo/"),
    )
    .await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        &marker,
        2,
        false,
        &[],
        &["cquux/"],
        None,
    )
    .await?;

    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "boo/",
        "/",
        "",
        1,
        true,
        &["boo/bar"],
        &[],
        Some("boo/bar"),
    )
    .await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "boo/",
        "/",
        &marker,
        1,
        false,
        &[],
        &["boo/baz/"],
        None,
    )
    .await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "boo/",
        "/",
        "",
        2,
        false,
        &["boo/bar"],
        &["boo/baz/"],
        None,
    )
    .await?;

    Ok(())
}

/// Verifies ListObjectsV2 paginates mixed contents and common prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_prefix() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_prefix
    let (_server, client, bucket) = seeded_bucket(&[
        "asdf",
        "boo/bar",
        "boo/baz/xyzzy",
        "cquux/thud",
        "cquux/bla",
    ])
    .await?;

    let token =
        validate_bucket_list_v2!(&client, &bucket, "", "/", None, 1, true, &["asdf"], &[]).await?;
    let token = validate_bucket_list_v2!(
        &client,
        &bucket,
        "",
        "/",
        Some(&token),
        1,
        true,
        &[],
        &["boo/"],
    )
    .await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "",
        "/",
        Some(&token),
        1,
        false,
        &[],
        &["cquux/"],
    )
    .await?;

    let token = validate_bucket_list_v2!(
        &client,
        &bucket,
        "",
        "/",
        None,
        2,
        true,
        &["asdf"],
        &["boo/"],
    )
    .await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "",
        "/",
        Some(&token),
        2,
        false,
        &[],
        &["cquux/"],
    )
    .await?;

    let token = validate_bucket_list_v2!(
        &client,
        &bucket,
        "boo/",
        "/",
        None,
        1,
        true,
        &["boo/bar"],
        &[],
    )
    .await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "boo/",
        "/",
        Some(&token),
        1,
        false,
        &[],
        &["boo/baz/"],
    )
    .await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "boo/",
        "/",
        None,
        2,
        false,
        &["boo/bar"],
        &["boo/baz/"],
    )
    .await?;

    Ok(())
}

/// Verifies ListObjects v1 includes a key that exactly equals prefix plus delimiter.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_prefix_ends_with_delimiter() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_prefix_ends_with_delimiter
    let (_server, client, bucket) = seeded_bucket(&["asdf/"]).await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "asdf/",
        "/",
        "",
        1000,
        false,
        &["asdf/"],
        &[],
        None,
    )
    .await?;
    Ok(())
}

/// Verifies ListObjectsV2 includes a key that exactly equals prefix plus delimiter.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_prefix_ends_with_delimiter() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_prefix_ends_with_delimiter
    let (_server, client, bucket) = seeded_bucket(&["asdf/"]).await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "asdf/",
        "/",
        None,
        1000,
        false,
        &["asdf/"],
        &[],
    )
    .await?;
    Ok(())
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

/// Verifies ListObjects v1 treats underscores literally in prefix pagination.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_prefix_underscore() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_prefix_underscore
    let (_server, client, bucket) = seeded_bucket(&[
        "_obj1_",
        "_under1/bar",
        "_under1/baz/xyzzy",
        "_under2/thud",
        "_under2/bla",
    ])
    .await?;

    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        "",
        1,
        true,
        &["_obj1_"],
        &[],
        Some("_obj1_"),
    )
    .await?;
    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        &marker,
        1,
        true,
        &[],
        &["_under1/"],
        Some("_under1/"),
    )
    .await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "",
        "/",
        &marker,
        1,
        false,
        &[],
        &["_under2/"],
        None,
    )
    .await?;

    let marker = validate_bucket_list_v1!(
        &client,
        &bucket,
        "_under1/",
        "/",
        "",
        1,
        true,
        &["_under1/bar"],
        &[],
        Some("_under1/bar"),
    )
    .await?;
    validate_bucket_list_v1!(
        &client,
        &bucket,
        "_under1/",
        "/",
        &marker,
        1,
        false,
        &[],
        &["_under1/baz/"],
        None,
    )
    .await?;

    Ok(())
}

/// Verifies ListObjectsV2 treats underscores literally in prefix pagination.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_prefix_underscore() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_prefix_underscore
    let (_server, client, bucket) = seeded_bucket(&[
        "_obj1_",
        "_under1/bar",
        "_under1/baz/xyzzy",
        "_under2/thud",
        "_under2/bla",
    ])
    .await?;

    let token =
        validate_bucket_list_v2!(&client, &bucket, "", "/", None, 1, true, &["_obj1_"], &[])
            .await?;
    let token = validate_bucket_list_v2!(
        &client,
        &bucket,
        "",
        "/",
        Some(&token),
        1,
        true,
        &[],
        &["_under1/"],
    )
    .await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "",
        "/",
        Some(&token),
        1,
        false,
        &[],
        &["_under2/"],
    )
    .await?;

    let token = validate_bucket_list_v2!(
        &client,
        &bucket,
        "_under1/",
        "/",
        None,
        1,
        true,
        &["_under1/bar"],
        &[],
    )
    .await?;
    validate_bucket_list_v2!(
        &client,
        &bucket,
        "_under1/",
        "/",
        Some(&token),
        1,
        false,
        &[],
        &["_under1/baz/"],
    )
    .await?;

    Ok(())
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

/// Verifies ListObjects v1 unreadable delimiters are echoed and do not group keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_unreadable
    delimiter_case_v1(
        "\n",
        Some("\n"),
        &["bar", "baz", "cab", "foo"],
        &["bar", "baz", "cab", "foo"],
        &[],
    )
    .await
}

/// Verifies ListObjectsV2 unreadable delimiters are echoed and do not group keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_delimiter_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_unreadable
    delimiter_case_v2(
        "\n",
        Some("\n"),
        &["bar", "baz", "cab", "foo"],
        &["bar", "baz", "cab", "foo"],
        &[],
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

/// Verifies ListObjectsV2 FetchOwner=true includes owner data.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_fetchowner_notempty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_fetchowner_notempty
    let (_server, client, bucket) = seeded_bucket(&["foo/bar", "foo/baz", "quux"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .fetch_owner(true)
        .send()
        .await?;
    assert!(response.contents()[0].owner().is_some());
    Ok(())
}

/// Verifies ListObjectsV2 omits owner data by default.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_fetchowner_defaultempty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_fetchowner_defaultempty
    let (_server, client, bucket) = seeded_bucket(&["foo/bar", "foo/baz", "quux"]).await?;
    let response = client.list_objects_v2().bucket(&bucket).send().await?;
    assert!(response.contents()[0].owner().is_none());
    Ok(())
}

/// Verifies ListObjectsV2 FetchOwner=false omits owner data.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_fetchowner_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_fetchowner_empty
    let (_server, client, bucket) = seeded_bucket(&["foo/bar", "foo/baz", "quux"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .fetch_owner(false)
        .send()
        .await?;
    assert!(response.contents()[0].owner().is_none());
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

/// Verifies delimiter listing does not skip the key immediately after a prefix group.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_delimiter_not_skip_special() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_delimiter_not_skip_special
    let generated = (1000..1999)
        .map(|index| format!("0/{index}"))
        .collect::<Vec<_>>();
    let mut keys = vec!["0/"];
    keys.extend(generated.iter().map(String::as_str));
    keys.extend(["1999", "1999#", "1999+", "2000"]);

    delimiter_case_v1(
        "/",
        Some("/"),
        &keys,
        &["1999", "1999#", "1999+", "2000"],
        &["0/"],
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

/// Verifies ListObjects v1 explicit empty prefix behaves like no prefix.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_none
    prefix_case_v1(
        "",
        &["foo/bar", "foo/baz", "quux"],
        &["foo/bar", "foo/baz", "quux"],
    )
    .await
}

/// Verifies ListObjectsV2 explicit empty prefix behaves like no prefix.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_none
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

/// Verifies ListObjects v1 prefix and alternate delimiter split matching keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_delimiter_alt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_delimiter_alt
    prefix_delimiter_case_v1_with(
        "ba",
        "a",
        &["bar", "bazar", "cab", "foo"],
        &["bar"],
        &["baza"],
    )
    .await
}

/// Verifies ListObjectsV2 prefix and alternate delimiter split matching keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_delimiter_alt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_delimiter_alt
    prefix_delimiter_case_v2_with(
        "ba",
        "a",
        &["bar", "bazar", "cab", "foo"],
        &["bar"],
        &["baza"],
    )
    .await
}

/// Verifies ListObjects v1 returns empty output when prefix misses.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_delimiter_prefix_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_delimiter_prefix_not_exist
    prefix_delimiter_case_v1_with("/", "d", &["b/a/r", "b/a/c", "b/a/g", "g"], &[], &[]).await
}

/// Verifies ListObjectsV2 returns empty output when prefix misses.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_delimiter_prefix_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_delimiter_prefix_not_exist
    prefix_delimiter_case_v2_with("/", "d", &["b/a/r", "b/a/c", "b/a/g", "g"], &[], &[]).await
}

/// Verifies ListObjects v1 returns prefixed keys when delimiter misses.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_delimiter_delimiter_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_delimiter_delimiter_not_exist
    prefix_delimiter_case_v1_with(
        "b",
        "z",
        &["b/a/c", "b/a/g", "b/a/r", "g"],
        &["b/a/c", "b/a/g", "b/a/r"],
        &[],
    )
    .await
}

/// Verifies ListObjectsV2 returns prefixed keys when delimiter misses.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_delimiter_delimiter_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_delimiter_delimiter_not_exist
    prefix_delimiter_case_v2_with(
        "b",
        "z",
        &["b/a/c", "b/a/g", "b/a/r", "g"],
        &["b/a/c", "b/a/g", "b/a/r"],
        &[],
    )
    .await
}

/// Verifies ListObjects v1 returns empty output when both prefix and delimiter miss.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist
    prefix_delimiter_case_v1_with("y", "z", &["b/a/c", "b/a/g", "b/a/r", "g"], &[], &[]).await
}

/// Verifies ListObjectsV2 returns empty output when both prefix and delimiter miss.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist
    prefix_delimiter_case_v2_with("y", "z", &["b/a/c", "b/a/g", "b/a/r", "g"], &[], &[]).await
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

/// Verifies ListObjects v1 MaxKeys=1 truncates to one key.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_maxkeys_one() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_maxkeys_one
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .max_keys(1)
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(true));
    assert_eq!(object_keys(response.contents()), vec!["bar"]);

    let response = client
        .list_objects()
        .bucket(&bucket)
        .marker("bar")
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(object_keys(response.contents()), vec!["baz", "foo", "quxx"]);
    Ok(())
}

/// Verifies ListObjectsV2 MaxKeys=1 truncates to one key.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_maxkeys_one() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_maxkeys_one
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .max_keys(1)
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(true));
    assert_eq!(object_keys(response.contents()), vec!["bar"]);

    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("bar")
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(object_keys(response.contents()), vec!["baz", "foo", "quxx"]);
    Ok(())
}

/// Verifies ListObjects v1 MaxKeys=0 returns no contents.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_maxkeys_zero() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_maxkeys_zero
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .max_keys(0)
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(false));
    assert!(response.contents().is_empty());
    Ok(())
}

/// Verifies ListObjectsV2 MaxKeys=0 returns no contents.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_maxkeys_zero() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_maxkeys_zero
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .max_keys(0)
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(false));
    assert!(response.contents().is_empty());
    Ok(())
}

/// Verifies ListObjects v1 default MaxKeys is 1000.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_maxkeys_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_maxkeys_none
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client.list_objects().bucket(&bucket).send().await?;
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "foo", "quxx"]
    );
    assert_eq!(response.max_keys(), Some(1000));
    Ok(())
}

/// Verifies ListObjects v1 rejects a non-numeric max-keys query value.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_maxkeys_invalid() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_maxkeys_invalid
    let (server, _client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;

    let response = raw_http::signed_request(
        &server,
        Method::GET,
        &format!("/{bucket}?max-keys=blah"),
        Vec::new(),
    )
    .await?;
    assert_status(&response, StatusCode::BAD_REQUEST);
    let body = response.text().await?;
    assert!(
        body.contains("InvalidArgument"),
        "expected InvalidArgument error, got {body}"
    );

    Ok(())
}

/// Verifies ListObjectsV2 default MaxKeys is 1000.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_maxkeys_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_maxkeys_none
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "foo", "quxx"]
    );
    assert_eq!(response.max_keys(), Some(1000));
    Ok(())
}

/// Verifies ListObjects v1 without marker echoes the empty marker.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_marker_none() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_marker_none
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client.list_objects().bucket(&bucket).send().await?;
    assert_eq!(response.marker(), Some(""));
    Ok(())
}

/// Verifies ListObjects v1 with empty marker returns all keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_marker_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_marker_empty
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .marker("")
        .send()
        .await?;
    assert_eq!(response.marker(), Some(""));
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "foo", "quxx"]
    );
    Ok(())
}

/// Verifies ListObjectsV2 with empty continuation token returns all keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_continuationtoken_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_continuationtoken_empty
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .continuation_token("")
        .send()
        .await?;
    assert_eq!(response.continuation_token(), Some(""));
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "foo", "quxx"]
    );
    Ok(())
}

/// Verifies ListObjectsV2 continuation tokens resume after the previous page.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_continuationtoken() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_continuationtoken
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let first = client
        .list_objects_v2()
        .bucket(&bucket)
        .max_keys(1)
        .send()
        .await?;
    let token = first
        .next_continuation_token()
        .expect("truncated response should include continuation token");
    let second = client
        .list_objects_v2()
        .bucket(&bucket)
        .continuation_token(token)
        .send()
        .await?;
    assert_eq!(second.continuation_token(), Some(token));
    assert_eq!(second.is_truncated(), Some(false));
    assert_eq!(object_keys(second.contents()), vec!["baz", "foo", "quxx"]);
    Ok(())
}

/// Verifies ListObjectsV2 continuation token wins while preserving StartAfter echo.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_both_continuationtoken_startafter() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_both_continuationtoken_startafter
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let first = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("bar")
        .max_keys(1)
        .send()
        .await?;
    let token = first
        .next_continuation_token()
        .expect("truncated response should include continuation token");
    let second = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("bar")
        .continuation_token(token)
        .send()
        .await?;
    assert_eq!(second.continuation_token(), Some(token));
    assert_eq!(second.start_after(), Some("bar"));
    assert_eq!(second.is_truncated(), Some(false));
    assert_eq!(object_keys(second.contents()), vec!["foo", "quxx"]);
    Ok(())
}

/// Verifies ListObjects v1 unreadable markers are echoed and sort before keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_marker_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_marker_unreadable
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .marker("\n")
        .send()
        .await?;
    assert_eq!(response.marker(), Some("\n"));
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "foo", "quxx"]
    );
    Ok(())
}

/// Verifies ListObjectsV2 unreadable StartAfter values are echoed and sort before keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_startafter_unreadable() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_startafter_unreadable
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("\n")
        .send()
        .await?;
    assert_eq!(response.start_after(), Some("\n"));
    assert_eq!(response.is_truncated(), Some(false));
    assert_eq!(
        object_keys(response.contents()),
        vec!["bar", "baz", "foo", "quxx"]
    );
    Ok(())
}

/// Verifies ListObjects v1 markers not in the bucket still seek lexicographically.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_marker_not_in_list() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_marker_not_in_list
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .marker("blah")
        .send()
        .await?;
    assert_eq!(response.marker(), Some("blah"));
    assert_eq!(object_keys(response.contents()), vec!["foo", "quxx"]);
    Ok(())
}

/// Verifies ListObjectsV2 StartAfter values not in the bucket still seek lexicographically.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_startafter_not_in_list() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_startafter_not_in_list
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("blah")
        .send()
        .await?;
    assert_eq!(response.start_after(), Some("blah"));
    assert_eq!(object_keys(response.contents()), vec!["foo", "quxx"]);
    Ok(())
}

/// Verifies ListObjects v1 markers after all keys return no contents.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_marker_after_list() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_marker_after_list
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .marker("zzz")
        .send()
        .await?;
    assert_eq!(response.marker(), Some("zzz"));
    assert_eq!(response.is_truncated(), Some(false));
    assert!(response.contents().is_empty());
    Ok(())
}

/// Verifies ListObjectsV2 StartAfter values after all keys return no contents.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_listv2_startafter_after_list() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_listv2_startafter_after_list
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo", "quxx"]).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("zzz")
        .send()
        .await?;
    assert_eq!(response.start_after(), Some("zzz"));
    assert_eq!(response.is_truncated(), Some(false));
    assert!(response.contents().is_empty());
    Ok(())
}

/// Verifies ListObjects v1 returns object metadata alongside keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_return_data() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_return_data
    let (_server, client, bucket) = seeded_bucket(&["bar", "baz", "foo"]).await?;
    let response = client.list_objects().bucket(&bucket).send().await?;

    for object in response.contents() {
        let key = object.key().expect("listed object should include key");
        let head = client.head_object().bucket(&bucket).key(key).send().await?;
        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await?;
        assert_eq!(object.e_tag(), head.e_tag());
        assert_eq!(object.size(), head.content_length());
        assert_eq!(
            object.owner().and_then(|owner| owner.display_name()),
            acl.owner().and_then(|owner| owner.display_name())
        );
        assert_eq!(
            object.owner().and_then(|owner| owner.id()),
            acl.owner().and_then(|owner| owner.id())
        );
    }

    Ok(())
}

/// Verifies ListObjectVersions returns version metadata alongside keys.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_return_data_versioning() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_return_data_versioning
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");
    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    put_keys(&client, &bucket, &["bar", "baz", "foo"]).await?;

    let response = client.list_object_versions().bucket(&bucket).send().await?;
    for version in response.versions() {
        let key = version.key().expect("listed version should include key");
        let version_id = version
            .version_id()
            .expect("listed version should include version ID");
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;
        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;
        assert_eq!(version.e_tag(), head.e_tag());
        assert_eq!(version.size(), head.content_length());
        assert_eq!(
            version.owner().and_then(|owner| owner.display_name()),
            acl.owner().and_then(|owner| owner.display_name())
        );
        assert_eq!(
            version.owner().and_then(|owner| owner.id()),
            acl.owner().and_then(|owner| owner.id())
        );
        assert_eq!(Some(version_id), head.version_id());
    }

    Ok(())
}

/// Verifies special S3 object key names can be listed and read.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_special_key_names() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_special_key_names
    let keys = [
        " ", "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__",
    ];
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");
    create_bucket(&client, &bucket).await?;

    for key in keys {
        let path = format!("/{bucket}/{}", raw_http::path_segment(key));
        let response =
            raw_http::signed_request(&server, Method::PUT, &path, key.as_bytes().to_vec()).await?;
        assert_status(&response, StatusCode::OK);
    }

    let response = client.list_objects().bucket(&bucket).send().await?;
    assert_eq!(object_keys(response.contents()), keys.to_vec());

    for key in keys {
        let path = format!("/{bucket}/{}", raw_http::path_segment(key));
        let response = raw_http::signed_request(&server, Method::GET, &path, Vec::new()).await?;
        assert_status(&response, StatusCode::OK);
        assert_eq!(response.bytes().await?.as_ref(), key.as_bytes());
    }

    Ok(())
}

/// Verifies ListObjects prefix filtering works for special prefixes.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_special_prefix() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_special_prefix
    let keys = ["_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd"];
    let (_server, client, bucket) = seeded_bucket(&keys).await?;

    let response = client.list_objects().bucket(&bucket).send().await?;
    assert_eq!(object_keys(response.contents()).len(), 5);

    let response = client
        .list_objects()
        .bucket(&bucket)
        .prefix("_bla/")
        .send()
        .await?;
    assert_eq!(object_keys(response.contents()), keys[..4].to_vec());

    Ok(())
}

async fn seeded_bucket(keys: &[&str]) -> Result<(TestServer, Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-list");
    create_bucket(&client, &bucket).await?;
    put_keys(&client, &bucket, keys).await?;
    Ok((server, client, bucket))
}

async fn put_keys(client: &Client, bucket: &str, keys: &[&str]) -> Result<()> {
    for key in keys {
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(ByteStream::from(key.as_bytes().to_vec()))
            .send()
            .await?;
    }
    Ok(())
}

async fn delimiter_case_v1(
    delimiter: &str,
    expected_delimiter: Option<&str>,
    keys: &[&str],
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
    keys: &[&str],
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

async fn prefix_case_v1(prefix: &str, keys: &[&str], expected_keys: &[&str]) -> Result<()> {
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

async fn prefix_case_v2(prefix: &str, keys: &[&str], expected_keys: &[&str]) -> Result<()> {
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
    prefix_delimiter_case_v1_with(
        "foo/",
        "/",
        &["foo/bar", "foo/baz/xyzzy", "quux/thud"],
        &["foo/bar"],
        &["foo/baz/"],
    )
    .await
}

async fn prefix_delimiter_case_v1_with(
    prefix: &str,
    delimiter: &str,
    keys: &[&str],
    expected_keys: &[&str],
    expected_prefixes: &[&str],
) -> Result<()> {
    let (_server, client, bucket) = seeded_bucket(keys).await?;
    let response = client
        .list_objects()
        .bucket(&bucket)
        .prefix(prefix)
        .delimiter(delimiter)
        .send()
        .await?;
    assert_eq!(response.prefix(), Some(prefix));
    assert_eq!(response.delimiter(), Some(delimiter));
    assert_eq!(object_keys(response.contents()), expected_keys.to_vec());
    assert_eq!(
        prefixes(response.common_prefixes()),
        expected_prefixes.to_vec()
    );
    Ok(())
}

async fn prefix_delimiter_case_v2() -> Result<()> {
    prefix_delimiter_case_v2_with(
        "foo/",
        "/",
        &["foo/bar", "foo/baz/xyzzy", "quux/thud"],
        &["foo/bar"],
        &["foo/baz/"],
    )
    .await
}

async fn prefix_delimiter_case_v2_with(
    prefix: &str,
    delimiter: &str,
    keys: &[&str],
    expected_keys: &[&str],
    expected_prefixes: &[&str],
) -> Result<()> {
    let (_server, client, bucket) = seeded_bucket(keys).await?;
    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(prefix)
        .delimiter(delimiter)
        .send()
        .await?;
    assert_eq!(response.prefix(), Some(prefix));
    assert_eq!(response.delimiter(), Some(delimiter));
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

async fn validate_bucket_list_v1_page(
    client: &Client,
    bucket: &str,
    page: ListPageV1<'_>,
) -> Result<String> {
    let response = client
        .list_objects()
        .bucket(bucket)
        .prefix(page.prefix)
        .delimiter(page.delimiter)
        .marker(page.marker)
        .max_keys(page.max_keys)
        .send()
        .await?;
    assert_eq!(response.is_truncated(), Some(page.is_truncated));
    assert_eq!(response.next_marker(), page.next_marker);
    assert_eq!(
        object_keys(response.contents()),
        page.expected_keys.to_vec()
    );
    assert_eq!(
        prefixes(response.common_prefixes()),
        page.expected_prefixes.to_vec()
    );
    Ok(response.next_marker().unwrap_or_default().to_string())
}

async fn validate_bucket_list_v2_page(
    client: &Client,
    bucket: &str,
    page: ListPageV2<'_>,
) -> Result<String> {
    let mut request = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(page.prefix)
        .delimiter(page.delimiter)
        .max_keys(page.max_keys);
    request = if let Some(token) = page.continuation_token {
        request.continuation_token(token)
    } else {
        request.start_after("")
    };

    let response = request.send().await?;
    assert_eq!(response.is_truncated(), Some(page.is_truncated));
    assert_eq!(
        object_keys(response.contents()),
        page.expected_keys.to_vec()
    );
    assert_eq!(
        prefixes(response.common_prefixes()),
        page.expected_prefixes.to_vec()
    );
    assert_eq!(
        response.key_count(),
        Some((page.expected_keys.len() + page.expected_prefixes.len()) as i32)
    );
    Ok(response
        .next_continuation_token()
        .unwrap_or_default()
        .to_string())
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
