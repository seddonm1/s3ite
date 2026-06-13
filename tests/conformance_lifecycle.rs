//! Real-binary HTTP bucket lifecycle conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_lifecycle`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use std::time::Duration as StdDuration;

use anyhow::{Context, Result, bail};
use aws_sdk_s3::{
    primitives::{ByteStream, DateTime},
    types::{
        AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
        LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
        NoncurrentVersionExpiration, Tag, Tagging,
    },
};
use reqwest::{Method, StatusCode};
use rusqlite::Connection;
use serial_test::serial;
use support::{
    assertions::{assert_error_code, assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, put_bucket_versioning},
    raw_http, sdk,
    server::{TestServer, TestServerOptions},
};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc2822};

fn expiration_rule(id: &str, prefix: &str, days: i32) -> Result<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .expiration(LifecycleExpiration::builder().days(days).build())
        .status(ExpirationStatus::Enabled)
        .build()?)
}

fn delete_marker_rule(id: &str) -> Result<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().build())
        .expiration(
            LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

fn noncurrent_rule(id: &str, noncurrent_days: i32) -> Result<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(noncurrent_days)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

fn multipart_rule(id: &str, days: i32) -> Result<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().build())
        .abort_incomplete_multipart_upload(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(days)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

fn expiration_date_rule(id: &str, prefix: &str, epoch_seconds: i64) -> Result<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .expiration(
            LifecycleExpiration::builder()
                .date(DateTime::from_secs(epoch_seconds))
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

fn lifecycle_config(rules: Vec<LifecycleRule>) -> Result<BucketLifecycleConfiguration> {
    Ok(BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()?)
}

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

async fn put_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    rules: Vec<LifecycleRule>,
) -> Result<()> {
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle_config(rules)?)
        .send()
        .await?;
    Ok(())
}

async fn create_bucket_with_client(
    prefix: &str,
) -> Result<(TestServer, aws_sdk_s3::Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name(prefix);
    create_bucket(&client, &bucket).await?;
    Ok((server, client, bucket))
}

async fn create_bucket_with_maintenance(
    prefix: &str,
) -> Result<(TestServer, aws_sdk_s3::Client, String)> {
    let server = TestServer::spawn_with(TestServerOptions {
        maintenance_interval_seconds: Some(1),
        ..Default::default()
    })
    .await?;
    let client = sdk::client(&server);
    let bucket = bucket_name(prefix);
    create_bucket(&client, &bucket).await?;
    Ok((server, client, bucket))
}

fn bucket_db(server: &TestServer, bucket: &str) -> Result<Connection> {
    Ok(Connection::open(
        server.data_root().join(format!("{bucket}.sqlite3")),
    )?)
}

fn old_timestamp(days_old: i64) -> OffsetDateTime {
    OffsetDateTime::now_utc() - Duration::days(days_old)
}

fn age_keys(server: &TestServer, bucket: &str, keys: &[&str], days_old: i64) -> Result<()> {
    let timestamp = old_timestamp(days_old);
    let mut connection = bucket_db(server, bucket)?;
    let transaction = connection.transaction()?;
    for key in keys {
        transaction.execute(
            "UPDATE object_version SET last_modified = ?1 WHERE key = ?2;",
            (timestamp, key),
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn age_noncurrent_versions(
    server: &TestServer,
    bucket: &str,
    key: &str,
    days_old: i64,
) -> Result<()> {
    let timestamp = old_timestamp(days_old);
    bucket_db(server, bucket)?.execute(
        "
        UPDATE object_version
        SET last_modified = ?1,
            noncurrent_since = ?1
        WHERE key = ?2
          AND is_latest = 0
          AND is_delete_marker = 0;",
        (timestamp, key),
    )?;
    Ok(())
}

fn age_multipart_uploads(server: &TestServer, bucket: &str, days_old: i64) -> Result<()> {
    let timestamp = old_timestamp(days_old);
    bucket_db(server, bucket)?.execute(
        "UPDATE multipart_upload SET initiated = ?1 WHERE bucket = ?2;",
        (timestamp, bucket),
    )?;
    Ok(())
}

async fn wait_for_objects(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    expected: &[&str],
) -> Result<()> {
    let mut expected = expected
        .iter()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    expected.sort();
    for _ in 0..20 {
        let mut actual = list_live_keys(client, bucket).await?;
        actual.sort();
        if actual == expected {
            return Ok(());
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
    bail!("timed out waiting for object set {expected:?}");
}

/// Lists the current live object keys in a bucket.
async fn list_live_keys(client: &aws_sdk_s3::Client, bucket: &str) -> Result<Vec<String>> {
    Ok(client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await?
        .contents()
        .iter()
        .filter_map(|object| object.key().map(str::to_string))
        .collect())
}

/// Waits for exact object-version and delete-marker key sets.
async fn wait_for_version_keys(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    expected_versions: &[&str],
    expected_delete_markers: &[&str],
) -> Result<()> {
    let mut expected_versions = expected_versions
        .iter()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    expected_versions.sort();
    let mut expected_delete_markers = expected_delete_markers
        .iter()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    expected_delete_markers.sort();

    for _ in 0..20 {
        let output = client.list_object_versions().bucket(bucket).send().await?;
        let mut versions = output
            .versions()
            .iter()
            .filter_map(|version| version.key().map(str::to_string))
            .collect::<Vec<_>>();
        versions.sort();
        let mut delete_markers = output
            .delete_markers()
            .iter()
            .filter_map(|marker| marker.key().map(str::to_string))
            .collect::<Vec<_>>();
        delete_markers.sort();
        if versions == expected_versions && delete_markers == expected_delete_markers {
            return Ok(());
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
    bail!(
        "timed out waiting for versions {expected_versions:?} and delete markers {expected_delete_markers:?}"
    );
}

async fn wait_for_multipart_uploads(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    uploads: usize,
) -> Result<()> {
    for _ in 0..20 {
        let output = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await?;
        if output.uploads().len() == uploads {
            return Ok(());
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
    bail!("timed out waiting for {uploads} multipart uploads");
}

/// Asserts an `x-amz-expiration` header has the expected rule and expiry window.
fn assert_expiration_header(
    value: &str,
    rule_id: &str,
    request_started_at: OffsetDateTime,
    days: i64,
) -> Result<()> {
    let (expires_at, actual_rule_id) = parse_expiration_header(value)?;
    assert_eq!(actual_rule_id, rule_id);
    let earliest = request_started_at + Duration::days(days) - Duration::seconds(1);
    let latest = OffsetDateTime::now_utc() + Duration::days(days) + Duration::seconds(2);
    assert!(
        expires_at >= earliest && expires_at <= latest,
        "expiration {expires_at} outside expected window {earliest}..={latest}"
    );
    Ok(())
}

/// Parses the `x-amz-expiration` header shape emitted by S3 object reads.
fn parse_expiration_header(value: &str) -> Result<(OffsetDateTime, String)> {
    let value = value
        .strip_prefix("expiry-date=\"")
        .context("missing expiry-date prefix")?;
    let (expires_at, rule_id) = value
        .split_once("\", rule-id=\"")
        .context("missing rule-id separator")?;
    let rule_id = rule_id
        .strip_suffix('"')
        .context("missing rule-id suffix")?
        .to_string();
    Ok((OffsetDateTime::parse(expires_at, &Rfc2822)?, rule_id))
}

async fn put_text_object(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(format!("{key}-body").into_bytes()))
        .send()
        .await?;
    Ok(())
}

async fn create_versions(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    count: usize,
) -> Result<()> {
    for index in 0..count {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(format!("{key}-{index}").into_bytes()))
            .send()
            .await?;
    }
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-set").await?;

    put_lifecycle(
        &client,
        &bucket,
        vec![expiration_rule("rule-1", "foo/", 1)?],
    )
    .await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;

    let rules = response.rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id(), Some("rule-1"));
    assert_eq!(
        rules[0].filter().and_then(|filter| filter.prefix()),
        Some("foo/")
    );
    assert_eq!(
        rules[0]
            .expiration()
            .and_then(|expiration| expiration.days()),
        Some(1)
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_get() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_get
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-get").await?;

    let missing = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("empty lifecycle config should fail");
    assert_error_contains(&missing, "NoSuchLifecycleConfiguration");

    put_lifecycle(
        &client,
        &bucket,
        vec![
            expiration_rule("rule-a", "a/", 1)?,
            expiration_rule("rule-b", "b/", 2)?,
        ],
    )
    .await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(response.rules().len(), 2);
    assert_eq!(response.rules()[0].id(), Some("rule-a"));
    assert_eq!(response.rules()[1].id(), Some("rule-b"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_delete
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-delete").await?;

    put_lifecycle(&client, &bucket, vec![expiration_rule("delete", "", 1)?]).await?;
    client
        .delete_bucket_lifecycle()
        .bucket(&bucket)
        .send()
        .await?;
    let missing = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("deleted lifecycle config should fail");
    assert_error_contains(&missing, "NoSuchLifecycleConfiguration");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_get_no_id() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_get_no_id
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-no-id").await?;
    let rule = LifecycleRule::builder()
        .filter(LifecycleRuleFilter::builder().prefix("foo/").build())
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;

    put_lifecycle(&client, &bucket, vec![rule]).await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(response.rules().len(), 1);
    assert_eq!(response.rules()[0].id(), None);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-exp").await?;
    let keys = [
        "expire1/foo",
        "expire1/bar",
        "keep2/foo",
        "keep2/bar",
        "expire3/foo",
        "expire3/bar",
    ];
    for key in keys {
        put_text_object(&client, &bucket, key).await?;
    }
    put_lifecycle(
        &client,
        &bucket,
        vec![
            expiration_rule("rule1", "expire1/", 1)?,
            expiration_rule("rule2", "expire3/", 5)?,
        ],
    )
    .await?;

    wait_for_objects(&client, &bucket, &keys).await?;
    age_keys(&server, &bucket, &["expire1/foo", "expire1/bar"], 2)?;
    wait_for_objects(
        &client,
        &bucket,
        &["keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"],
    )
    .await?;
    age_keys(&server, &bucket, &["expire3/foo", "expire3/bar"], 6)?;
    wait_for_objects(&client, &bucket, &["keep2/foo", "keep2/bar"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecyclev2_expiration() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecyclev2_expiration
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-exp-v2").await?;
    let keys = [
        "expire1/foo",
        "expire1/bar",
        "keep2/foo",
        "keep2/bar",
        "expire3/foo",
        "expire3/bar",
    ];
    for key in keys {
        put_text_object(&client, &bucket, key).await?;
    }
    put_lifecycle(
        &client,
        &bucket,
        vec![
            expiration_rule("rule1", "expire1/", 1)?,
            expiration_rule("rule2", "expire3/", 5)?,
        ],
    )
    .await?;

    wait_for_objects(&client, &bucket, &keys).await?;
    age_keys(&server, &bucket, &["expire1/foo", "expire1/bar"], 2)?;
    wait_for_objects(
        &client,
        &bucket,
        &["keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"],
    )
    .await?;
    age_keys(&server, &bucket, &["expire3/foo", "expire3/bar"], 6)?;
    wait_for_objects(&client, &bucket, &["keep2/foo", "keep2/bar"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_versioning_enabled() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_versioning_enabled
    let (_server, client, bucket) = create_bucket_with_maintenance("s3ite-life-exp-ver").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    create_versions(&client, &bucket, "test1/a", 1).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("test1/a")
        .send()
        .await?;
    put_lifecycle(
        &client,
        &bucket,
        vec![expiration_rule("rule1", "test1/", 1)?],
    )
    .await?;

    wait_for_version_keys(&client, &bucket, &["test1/a"], &["test1/a"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_tags1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_tags1
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-tags1").await?;
    let key = "days1/tom";
    put_text_object(&client, &bucket, key).await?;
    client
        .put_object_tagging()
        .bucket(&bucket)
        .key(key)
        .tagging(tagging(&[("tom", "sawyer")])?)
        .send()
        .await?;
    let rule = LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(tag("tom", "sawyer")?)
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_keys(&server, &bucket, &[key], 2)?;
    wait_for_objects(&client, &bucket, &[]).await?;

    Ok(())
}

async fn setup_lifecycle_tags2(client: &aws_sdk_s3::Client, bucket: &str) -> Result<()> {
    put_text_object(client, bucket, "days1/tom").await?;
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("days1/tom")
        .tagging(tagging(&[("tom", "sawyer")])?)
        .send()
        .await?;
    put_text_object(client, bucket, "days1/huck").await?;
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("days1/huck")
        .tagging(tagging(&[("tom", "sawyer"), ("huck", "finn")])?)
        .send()
        .await?;

    let rule = LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    LifecycleRuleAndOperator::builder()
                        .prefix("days1/")
                        .tags(tag("huck", "finn")?)
                        .build(),
                )
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(client, bucket, vec![rule]).await
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_tags2() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_tags2
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-tags2").await?;
    setup_lifecycle_tags2(&client, &bucket).await?;

    age_keys(&server, &bucket, &["days1/tom", "days1/huck"], 2)?;
    wait_for_objects(&client, &bucket, &["days1/tom"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_versioned_tags2() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_versioned_tags2
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-vtags2").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    setup_lifecycle_tags2(&client, &bucket).await?;

    age_keys(&server, &bucket, &["days1/tom", "days1/huck"], 2)?;
    wait_for_objects(&client, &bucket, &["days1/tom"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_noncur_tags1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_noncur_tags1
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-nctag").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    for _ in 0..10 {
        put_text_object(&client, &bucket, "myobject_").await?;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("myobject_")
            .tagging(tagging(&[("vidushi", "mishra")])?)
            .send()
            .await?;
    }
    let rule = LifecycleRule::builder()
        .id("rule_tag1")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(tag("vidushi", "mishra")?)
                .build(),
        )
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(4)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    wait_for_version_keys(&client, &bucket, &["myobject_"; 10], &[]).await?;
    age_noncurrent_versions(&server, &bucket, "myobject_", 5)?;
    wait_for_version_keys(&client, &bucket, &["myobject_"], &[]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_newer_noncurrent() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_newer_noncurrent
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-newer-nc").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    create_versions(&client, &bucket, "myobject_", 10).await?;
    let rule = LifecycleRule::builder()
        .id("newer_noncurrent1")
        .filter(LifecycleRuleFilter::builder().prefix("").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .newer_noncurrent_versions(5)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_noncurrent_versions(&server, &bucket, "myobject_", 2)?;
    wait_for_version_keys(&client, &bucket, &["myobject_"; 6], &[]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_size_gt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_size_gt
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-size-gt").await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("myobject_small")
        .body(ByteStream::from(vec![b'a'; 1000]))
        .send()
        .await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("myobject_big")
        .body(ByteStream::from(vec![b'a'; 3000]))
        .send()
        .await?;
    let rule = LifecycleRule::builder()
        .id("object_gt1")
        .filter(
            LifecycleRuleFilter::builder()
                .object_size_greater_than(2000)
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_keys(&server, &bucket, &["myobject_small", "myobject_big"], 2)?;
    wait_for_objects(&client, &bucket, &["myobject_small"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_size_lt() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_size_lt
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-size-lt").await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("myobject_small")
        .body(ByteStream::from(vec![b'a'; 1000]))
        .send()
        .await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("myobject_big")
        .body(ByteStream::from(vec![b'a'; 3000]))
        .send()
        .await?;
    let rule = LifecycleRule::builder()
        .id("object_lt1")
        .filter(
            LifecycleRuleFilter::builder()
                .object_size_less_than(2000)
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_keys(&server, &bucket, &["myobject_small", "myobject_big"], 2)?;
    wait_for_objects(&client, &bucket, &["myobject_big"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_id_too_long() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_id_too_long
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-long-id").await?;

    let error = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(lifecycle_config(vec![expiration_rule(
            &"x".repeat(256),
            "",
            1,
        )?])?)
        .send()
        .await
        .expect_err("overlong lifecycle rule ID should fail");
    assert_error_code(&error, "InvalidArgument");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_same_id() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_same_id
    let (_server, client, bucket) = create_bucket_with_client("s3ite-life-same-id").await?;

    let error = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(lifecycle_config(vec![
            expiration_rule("rule1", "test1/", 1)?,
            expiration_rule("rule1", "test2/", 2)?,
        ])?)
        .send()
        .await
        .expect_err("duplicate lifecycle rule IDs should fail");
    assert_error_contains(&error, "InvalidArgument");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_invalid_status() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_invalid_status
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-status").await?;
    let rule = LifecycleRule::builder()
        .filter(LifecycleRuleFilter::builder().build())
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::from("Bogus"))
        .build()?;

    let error = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(lifecycle_config(vec![rule])?)
        .send()
        .await
        .expect_err("invalid lifecycle rule status should fail");
    assert_error_contains(&error, "MalformedXML");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_date() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_date
    let (_server, client, bucket) = create_bucket_with_client("s3ite-life-set-date").await?;

    put_lifecycle(
        &client,
        &bucket,
        vec![expiration_date_rule("rule1", "test1/", 1_506_470_400)?],
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_invalid_date() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_invalid_date
    let (server, _client, bucket) = create_bucket_with_client("s3ite-life-bad-date").await?;
    let body = br#"
        <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Rule>
            <ID>rule1</ID>
            <Prefix>test1/</Prefix>
            <Status>Enabled</Status>
            <Expiration><Date>20200101</Date></Expiration>
          </Rule>
        </LifecycleConfiguration>
    "#
    .to_vec();

    let response = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &format!("/{bucket}?lifecycle"),
        body,
        &[("Content-Type", "application/xml")],
    )
    .await?;
    assert_status(&response, StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_date() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_date
    let (_server, client, bucket) = create_bucket_with_maintenance("s3ite-life-exp-date").await?;
    put_text_object(&client, &bucket, "past/foo").await?;
    put_text_object(&client, &bucket, "future/bar").await?;
    put_lifecycle(
        &client,
        &bucket,
        vec![
            expiration_date_rule("rule1", "past/", 1_420_070_400)?,
            expiration_date_rule("rule2", "future/", 1_893_456_000)?,
        ],
    )
    .await?;

    wait_for_objects(&client, &bucket, &["future/bar"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_days0() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_days0
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-days-zero").await?;

    let error = client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(lifecycle_config(vec![expiration_rule(
            "days-zero",
            "",
            0,
        )?])?)
        .send()
        .await
        .expect_err("zero lifecycle expiration days should fail");
    assert_error_code(&error, "InvalidArgument");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_noncurrent() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_noncurrent
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-noncurrent").await?;

    put_lifecycle(&client, &bucket, vec![noncurrent_rule("noncurrent", 1)?]).await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(
        response.rules()[0]
            .noncurrent_version_expiration()
            .and_then(|expiration| expiration.noncurrent_days()),
        Some(1)
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_noncur_expiration() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_noncur_expiration
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-noncur-exp").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    create_versions(&client, &bucket, "test1/a", 3).await?;
    create_versions(&client, &bucket, "test2/abc", 3).await?;
    let rule = LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(2)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_noncurrent_versions(&server, &bucket, "test1/a", 3)?;
    wait_for_version_keys(
        &client,
        &bucket,
        &["test1/a", "test2/abc", "test2/abc", "test2/abc"],
        &[],
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_deletemarker() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_deletemarker
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-marker").await?;

    put_lifecycle(&client, &bucket, vec![delete_marker_rule("marker")?]).await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(
        response.rules()[0]
            .expiration()
            .and_then(|expiration| expiration.expired_object_delete_marker()),
        Some(true)
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_deletemarker_expiration() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_deletemarker_expiration
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-dm-exp").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    create_versions(&client, &bucket, "test1/a", 1).await?;
    create_versions(&client, &bucket, "test2/abc", 1).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("test1/a")
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("test2/abc")
        .send()
        .await?;
    let rule = LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        )
        .expiration(
            LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_noncurrent_versions(&server, &bucket, "test1/a", 2)?;
    wait_for_version_keys(&client, &bucket, &["test2/abc"], &["test2/abc"]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_deletemarker_expiration_with_days_tag() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_deletemarker_expiration_with_days_tag
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-dm-days").await?;
    put_bucket_versioning(
        &client,
        &bucket,
        aws_sdk_s3::types::BucketVersioningStatus::Enabled,
    )
    .await?;
    create_versions(&client, &bucket, "test1/a", 1).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("test1/a")
        .send()
        .await?;
    let rule = LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(5).build())
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_noncurrent_versions(&server, &bucket, "test1/a", 2)?;
    wait_for_version_keys(&client, &bucket, &[], &["test1/a"]).await?;
    age_keys(&server, &bucket, &["test1/a"], 6)?;
    wait_for_version_keys(&client, &bucket, &[], &[]).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_filter() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_filter
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-filter").await?;
    let rule = LifecycleRule::builder()
        .id("and-filter")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    LifecycleRuleAndOperator::builder()
                        .prefix("photos/")
                        .tags(tag("kind", "raw")?)
                        .object_size_greater_than(10)
                        .object_size_less_than(1024)
                        .build(),
                )
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;

    put_lifecycle(&client, &bucket, vec![rule]).await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    let and = response.rules()[0]
        .filter()
        .and_then(|filter| filter.and())
        .context("lifecycle rule missing And filter")?;
    assert_eq!(and.prefix(), Some("photos/"));
    assert_eq!(and.tags().len(), 1);
    assert_eq!(and.object_size_greater_than(), Some(10));
    assert_eq!(and.object_size_less_than(), Some(1024));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_empty_filter() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_empty_filter
    let (_server, client, bucket) = create_bucket_with_client("s3ite-life-empty-filter").await?;
    let rule = LifecycleRule::builder()
        .id("empty-filter")
        .filter(LifecycleRuleFilter::builder().build())
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;

    put_lifecycle(&client, &bucket, vec![rule]).await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert!(response.rules()[0].filter().is_some());
    assert_eq!(
        response.rules()[0]
            .filter()
            .and_then(|filter| filter.prefix()),
        None
    );
    assert!(
        response.rules()[0]
            .filter()
            .and_then(|filter| filter.tag())
            .is_none()
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_set_multipart() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_set_multipart
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-multipart").await?;

    put_lifecycle(&client, &bucket, vec![multipart_rule("multipart", 1)?]).await?;
    let response = client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(
        response.rules()[0]
            .abort_incomplete_multipart_upload()
            .and_then(|abort| abort.days_after_initiation()),
        Some(1)
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_multipart_expiration() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_multipart_expiration
    let (server, client, bucket) = create_bucket_with_maintenance("s3ite-life-mpu-exp").await?;
    for key in ["test1/a", "test2/"] {
        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await?;
    }
    wait_for_multipart_uploads(&client, &bucket, 2).await?;
    let rule = LifecycleRule::builder()
        .id("rule1")
        .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
        .abort_incomplete_multipart_upload(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(2)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?;
    put_lifecycle(&client, &bucket, vec![rule]).await?;

    age_multipart_uploads(&server, &bucket, 3)?;
    wait_for_multipart_uploads(&client, &bucket, 1).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_header_put() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_header_put
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-put-header").await?;
    let key = "foo/object";

    put_lifecycle(
        &client,
        &bucket,
        vec![expiration_rule("expire-put", "foo/", 1)?],
    )
    .await?;
    let request_started_at = OffsetDateTime::now_utc();
    let response = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    let expiration = response
        .expiration()
        .context("missing PutObject expiration header")?;
    assert_expiration_header(expiration, "expire-put", request_started_at, 1)?;

    let response = client.get_object().bucket(&bucket).key(key).send().await?;
    let expiration = response.expiration().context("missing expiration header")?;
    assert_expiration_header(expiration, "expire-put", request_started_at, 1)?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_header_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_header_head
    let (_server, client, bucket) = create_bucket_with_client("s3ite-life-head-header").await?;
    let key = "foo/object";

    put_lifecycle(
        &client,
        &bucket,
        vec![expiration_rule("expire-head", "foo/", 1)?],
    )
    .await?;
    let request_started_at = OffsetDateTime::now_utc();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;

    let response = client.head_object().bucket(&bucket).key(key).send().await?;
    let expiration = response.expiration().context("missing expiration header")?;
    assert_expiration_header(expiration, "expire-head", request_started_at, 1)?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_header_tags_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_header_tags_head
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-tag-head").await?;
    let key = "tagged";
    let rule = LifecycleRule::builder()
        .id("tag-head")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(tag("kind", "tmp")?)
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;

    put_lifecycle(&client, &bucket, vec![rule]).await?;
    let request_started_at = OffsetDateTime::now_utc();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .tagging("kind=tmp")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;

    let response = client.head_object().bucket(&bucket).key(key).send().await?;
    let expiration = response.expiration().context("missing expiration header")?;
    assert_expiration_header(expiration, "tag-head", request_started_at, 1)?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_lifecycle_expiration_header_and_tags_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_lifecycle_expiration_header_and_tags_head
    let (_server, client, bucket) = create_bucket_with_client("s3ite-lifecycle-and-head").await?;
    let key = "photos/raw";
    let rule = LifecycleRule::builder()
        .id("and-head")
        .filter(
            LifecycleRuleFilter::builder()
                .and(
                    LifecycleRuleAndOperator::builder()
                        .prefix("photos/")
                        .tags(tag("kind", "raw")?)
                        .object_size_greater_than(1)
                        .build(),
                )
                .build(),
        )
        .expiration(LifecycleExpiration::builder().days(1).build())
        .status(ExpirationStatus::Enabled)
        .build()?;

    put_lifecycle(&client, &bucket, vec![rule]).await?;
    let request_started_at = OffsetDateTime::now_utc();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .tagging("kind=raw")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;

    let response = client.head_object().bucket(&bucket).key(key).send().await?;
    let expiration = response.expiration().context("missing expiration header")?;
    assert_expiration_header(expiration, "and-head", request_started_at, 1)?;

    Ok(())
}
