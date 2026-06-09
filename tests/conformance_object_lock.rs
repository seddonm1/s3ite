//! Real-binary HTTP Object Lock conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_object_lock`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::{
    Client,
    primitives::{ByteStream, DateTime},
    types::{
        BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, DefaultRetention, Delete,
        ObjectIdentifier, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
        ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode,
        ObjectLockRule,
    },
};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, put_bucket_versioning},
    raw_http, sdk,
    server::TestServer,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const RETAIN_2030: i64 = 1_893_456_000;
const RETAIN_2030_PLUS_TWO_DAYS: i64 = 1_893_628_800;
const RETAIN_2140: i64 = 5_364_662_400;

async fn create_bucket(client: &Client, prefix: &str) -> Result<String> {
    let bucket = object_lock_bucket_name(prefix);
    client.create_bucket().bucket(&bucket).send().await?;
    Ok(bucket)
}

async fn create_object_lock_bucket(client: &Client, prefix: &str) -> Result<String> {
    let bucket = object_lock_bucket_name(prefix);
    client
        .create_bucket()
        .bucket(&bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    Ok(bucket)
}

fn object_lock_bucket_name(prefix: &str) -> String {
    let bounded_prefix: String = prefix.chars().take(20).collect();
    bucket_name(&bounded_prefix)
}

fn date_2030() -> DateTime {
    DateTime::from_secs(RETAIN_2030)
}

fn date_2030_plus_two_days() -> DateTime {
    DateTime::from_secs(RETAIN_2030_PLUS_TWO_DAYS)
}

fn date_2140() -> DateTime {
    DateTime::from_secs(RETAIN_2140)
}

fn default_config(
    mode: ObjectLockRetentionMode,
    days: Option<i32>,
    years: Option<i32>,
) -> ObjectLockConfiguration {
    let mut default = DefaultRetention::builder().mode(mode);
    if let Some(days) = days {
        default = default.days(days);
    }
    if let Some(years) = years {
        default = default.years(years);
    }

    ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(default.build())
                .build(),
        )
        .build()
}

fn governance_days_config(days: i32) -> ObjectLockConfiguration {
    default_config(ObjectLockRetentionMode::Governance, Some(days), None)
}

fn governance_retention(retain_until_date: DateTime) -> ObjectLockRetention {
    retention(ObjectLockRetentionMode::Governance, retain_until_date)
}

fn retention(mode: ObjectLockRetentionMode, retain_until_date: DateTime) -> ObjectLockRetention {
    ObjectLockRetention::builder()
        .mode(mode)
        .retain_until_date(retain_until_date)
        .build()
}

fn legal_hold(status: ObjectLockLegalHoldStatus) -> ObjectLockLegalHold {
    ObjectLockLegalHold::builder().status(status).build()
}

async fn put_object(client: &Client, bucket: &str, key: &str) -> Result<String> {
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await?;
    output
        .version_id()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("PutObject response missing version ID"))
}

async fn put_unversioned_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await?;
    Ok(())
}

async fn put_object_with_governance_retention(
    client: &Client,
    bucket: &str,
    key: &str,
    retain_until_date: DateTime,
) -> Result<String> {
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until_date)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await?;
    output
        .version_id()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("PutObject response missing version ID"))
}

async fn put_object_with_compliance_retention(
    client: &Client,
    bucket: &str,
    key: &str,
    retain_until_date: DateTime,
) -> Result<String> {
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .object_lock_mode(ObjectLockMode::Compliance)
        .object_lock_retain_until_date(retain_until_date)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await?;
    output
        .version_id()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("PutObject response missing version ID"))
}

async fn put_governance_retention(
    client: &Client,
    bucket: &str,
    key: &str,
    retain_until_date: DateTime,
) -> Result<()> {
    client
        .put_object_retention()
        .bucket(bucket)
        .key(key)
        .retention(governance_retention(retain_until_date))
        .send()
        .await?;
    Ok(())
}

async fn get_retention(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<ObjectLockRetention> {
    let mut request = client.get_object_retention().bucket(bucket).key(key);
    if let Some(version_id) = version_id {
        request = request.version_id(version_id);
    }
    request
        .send()
        .await?
        .retention()
        .cloned()
        .ok_or_else(|| anyhow!("GetObjectRetention response missing retention"))
}

fn assert_retention(
    actual: &ObjectLockRetention,
    mode: ObjectLockRetentionMode,
    retain_until_secs: i64,
) {
    assert_eq!(actual.mode(), Some(&mode));
    assert_eq!(
        actual.retain_until_date().map(DateTime::secs),
        Some(retain_until_secs)
    );
}

fn assert_default_retention(
    actual: &ObjectLockConfiguration,
    mode: ObjectLockRetentionMode,
    days: Option<i32>,
    years: Option<i32>,
) -> Result<()> {
    assert_eq!(
        actual.object_lock_enabled(),
        Some(&ObjectLockEnabled::Enabled)
    );
    let default = actual
        .rule()
        .and_then(|rule| rule.default_retention())
        .ok_or_else(|| anyhow!("Object Lock configuration missing default retention"))?;
    assert_eq!(default.mode(), Some(&mode));
    assert_eq!(default.days(), days);
    assert_eq!(default.years(), years);
    Ok(())
}

async fn complete_one_part_upload_with_lock(
    client: &Client,
    bucket: &str,
    key: &str,
    mode: Option<ObjectLockMode>,
    retain_until_date: Option<DateTime>,
    legal_hold_status: Option<ObjectLockLegalHoldStatus>,
) -> Result<String> {
    let mut create = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(mode) = mode {
        create = create.object_lock_mode(mode);
    }
    if let Some(retain_until_date) = retain_until_date {
        create = create.object_lock_retain_until_date(retain_until_date);
    }
    if let Some(legal_hold_status) = legal_hold_status {
        create = create.object_lock_legal_hold_status(legal_hold_status);
    }

    let upload_id = create
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow!("CreateMultipartUpload response missing upload ID"))?
        .to_string();
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await?;
    let completed = CompletedPart::builder()
        .part_number(1)
        .e_tag(
            part.e_tag()
                .ok_or_else(|| anyhow!("UploadPart response missing ETag"))?,
        )
        .build();

    let output = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(vec![completed]))
                .build(),
        )
        .send()
        .await?;
    output
        .version_id()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("CompleteMultipartUpload response missing version ID"))
}

/// Verifies Object Lock bucket configuration can be created and updated.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-put-config").await?;

    client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await?;

    client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(default_config(
            ObjectLockRetentionMode::Compliance,
            None,
            Some(1),
        ))
        .send()
        .await?;

    let versioning = client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));

    Ok(())
}

/// Verifies Object Lock configuration fails on buckets without Object Lock or versioning.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_invalid_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_invalid_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-invalid-bucket").await?;

    let error = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await
        .expect_err("Object Lock configuration should fail before versioning is enabled");
    assert_error_contains(&error, "InvalidBucketState");

    Ok(())
}

/// Verifies Object Lock can be enabled after creation only once versioning is enabled.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_enable_after_create() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_enable_after_create
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-enable-after-create").await?;

    let unversioned = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await
        .expect_err("Object Lock configuration should fail before versioning is configured");
    assert_error_contains(&unversioned, "InvalidBucketState");

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    let suspended = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await
        .expect_err("Object Lock configuration should fail while versioning is suspended");
    assert_error_contains(&suspended, "InvalidBucketState");

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await?;

    let config = client
        .get_object_lock_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_default_retention(
        config
            .object_lock_configuration()
            .ok_or_else(|| anyhow!("GetObjectLockConfiguration missing config"))?,
        ObjectLockRetentionMode::Governance,
        Some(1),
        None,
    )?;

    Ok(())
}

/// Verifies default retention cannot specify both Days and Years.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_with_days_and_years() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_with_days_and_years
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-days-years").await?;

    let error = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(default_config(
            ObjectLockRetentionMode::Governance,
            Some(1),
            Some(1),
        ))
        .send()
        .await
        .expect_err("Days and Years together should be malformed XML");
    assert_error_contains(&error, "MalformedXML");

    Ok(())
}

/// Verifies default retention Days must be positive.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_invalid_days() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_invalid_days
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-invalid-days").await?;

    let error = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(0))
        .send()
        .await
        .expect_err("zero Days should be rejected");
    assert_error_contains(&error, "InvalidRetentionPeriod");

    Ok(())
}

/// Verifies default retention Years must be positive.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_invalid_years() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_invalid_years
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-invalid-years").await?;

    let error = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(default_config(
            ObjectLockRetentionMode::Governance,
            None,
            Some(-1),
        ))
        .send()
        .await
        .expect_err("negative Years should be rejected");
    assert_error_contains(&error, "InvalidRetentionPeriod");

    Ok(())
}

/// Verifies invalid Object Lock default retention modes are malformed XML.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_invalid_mode() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_invalid_mode
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-invalid-mode").await?;

    for mode in [
        ObjectLockRetentionMode::from("abc"),
        ObjectLockRetentionMode::from("governance"),
    ] {
        let error = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(default_config(mode, None, Some(1)))
            .send()
            .await
            .expect_err("invalid default retention mode should be malformed XML");
        assert_error_contains(&error, "MalformedXML");
    }

    Ok(())
}

/// Verifies ObjectLockEnabled must be Enabled.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_lock_invalid_status() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_lock_invalid_status
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-invalid-status").await?;

    let error = client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::from("Disabled"))
                .rule(
                    ObjectLockRule::builder()
                        .default_retention(
                            DefaultRetention::builder()
                                .mode(ObjectLockRetentionMode::Governance)
                                .years(1)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("ObjectLockEnabled=Disabled should be malformed XML");
    assert_error_contains(&error, "MalformedXML");

    Ok(())
}

/// Verifies Object Lock buckets cannot suspend versioning.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_suspend_versioning() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_suspend_versioning
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-suspend-versioning").await?;

    let error = put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended)
        .await
        .expect_err("Object Lock bucket versioning cannot be suspended");
    assert_error_contains(&error, "InvalidBucketState");

    Ok(())
}

/// Verifies Object Lock configuration can be read back.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_obj_lock() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_obj_lock
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-get-config").await?;

    client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await?;
    let output = client
        .get_object_lock_configuration()
        .bucket(&bucket)
        .send()
        .await?;
    assert_default_retention(
        output
            .object_lock_configuration()
            .ok_or_else(|| anyhow!("GetObjectLockConfiguration missing config"))?,
        ObjectLockRetentionMode::Governance,
        Some(1),
        None,
    )?;

    Ok(())
}

/// Verifies buckets without Object Lock have no Object Lock configuration.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_obj_lock_invalid_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_obj_lock_invalid_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-get-invalid-bucket").await?;

    let error = client
        .get_object_lock_configuration()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("bucket without Object Lock should have no config");
    assert_error_contains(&error, "ObjectLockConfigurationNotFoundError");

    Ok(())
}

/// Verifies object retention can be set and read.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-put-retention").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2140()).await?;
    let actual = get_retention(&client, &bucket, key, None).await?;
    assert_retention(&actual, ObjectLockRetentionMode::Governance, RETAIN_2140);

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies object retention APIs reject buckets without Object Lock.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_invalid_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_invalid_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-put-retention-invalid-bucket").await?;
    let key = "file1";
    put_unversioned_object(&client, &bucket, key).await?;

    let error = client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .retention(governance_retention(date_2030()))
        .send()
        .await
        .expect_err("retention on bucket without Object Lock should fail");
    assert_error_contains(&error, "InvalidRequest");

    Ok(())
}

/// Verifies invalid object retention modes are malformed XML.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_invalid_mode() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_invalid_mode
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket =
        create_object_lock_bucket(&client, "object-lock-put-retention-invalid-mode").await?;
    let key = "file1";
    put_object(&client, &bucket, key).await?;

    for mode in [
        ObjectLockRetentionMode::from("governance"),
        ObjectLockRetentionMode::from("abc"),
    ] {
        let error = client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(mode, date_2030()))
            .send()
            .await
            .expect_err("invalid retention mode should be malformed XML");
        assert_error_contains(&error, "MalformedXML");
    }

    Ok(())
}

/// Verifies object retention can be read.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_obj_retention() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_obj_retention
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-get-retention").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030()).await?;
    let actual = get_retention(&client, &bucket, key, None).await?;
    assert_retention(&actual, ObjectLockRetentionMode::Governance, RETAIN_2030);

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies retain-until headers are serialized as ISO-8601 timestamps.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_obj_retention_iso8601() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_obj_retention_iso8601
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-retention-iso8601").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030()).await?;
    let response = raw_http::signed_request(
        &server,
        Method::HEAD,
        &format!("/{bucket}/{key}?versionId={version_id}"),
        Vec::new(),
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    let retain_until = response
        .headers()
        .get("x-amz-object-lock-retain-until-date")
        .context("HeadObject response missing retain-until header")?
        .to_str()?;
    OffsetDateTime::parse(retain_until, &Rfc3339)?;

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies GetObjectRetention rejects buckets without Object Lock.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_obj_retention_invalid_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_obj_retention_invalid_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-get-retention-invalid-bucket").await?;
    let key = "file1";
    put_unversioned_object(&client, &bucket, key).await?;

    let error = client
        .get_object_retention()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect_err("GetObjectRetention on bucket without Object Lock should fail");
    assert_error_contains(&error, "InvalidRequest");

    Ok(())
}

/// Verifies retention can be set and read for a specific version ID.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_versionid() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_versionid
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-retention-versionid").await?;
    let key = "file1";
    put_object(&client, &bucket, key).await?;
    let version_id = put_object(&client, &bucket, key).await?;

    client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .retention(governance_retention(date_2030()))
        .send()
        .await?;
    let actual = get_retention(&client, &bucket, key, Some(&version_id)).await?;
    assert_retention(&actual, ObjectLockRetentionMode::Governance, RETAIN_2030);

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies explicit object retention overrides default retention.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_override_default_retention() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_override_default_retention
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-retention-override").await?;
    let key = "file1";

    client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(governance_days_config(1))
        .send()
        .await?;
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030()).await?;
    let actual = get_retention(&client, &bucket, key, None).await?;
    assert_retention(&actual, ObjectLockRetentionMode::Governance, RETAIN_2030);

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies active governance retention can be increased.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_increase_period() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_increase_period
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-retention-increase").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030()).await?;
    put_governance_retention(&client, &bucket, key, date_2030_plus_two_days()).await?;
    let actual = get_retention(&client, &bucket, key, None).await?;
    assert_retention(
        &actual,
        ObjectLockRetentionMode::Governance,
        RETAIN_2030_PLUS_TWO_DAYS,
    );

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies active governance retention cannot be shortened without bypass.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_shorten_period() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_shorten_period
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-retention-shorten").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030_plus_two_days()).await?;
    let error = client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .retention(governance_retention(date_2030()))
        .send()
        .await
        .expect_err("shortening governance retention without bypass should fail");
    assert_error_contains(&error, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies active governance retention can be shortened with bypass.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_obj_retention_shorten_period_bypass() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_obj_retention_shorten_period_bypass
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-retention-shorten-bypass").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030_plus_two_days()).await?;
    client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .retention(governance_retention(date_2030()))
        .bypass_governance_retention(true)
        .send()
        .await?;
    let actual = get_retention(&client, &bucket, key, None).await?;
    assert_retention(&actual, ObjectLockRetentionMode::Governance, RETAIN_2030);

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies retained object versions cannot be deleted without bypass.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_delete_object_with_retention() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_delete_object_with_retention
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-delete-retention").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030()).await?;
    let error = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("retained object version delete should fail");
    assert_error_contains(&error, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies retained multipart object versions cannot be deleted without bypass.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_delete_multipart_object_with_retention() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_delete_multipart_object_with_retention
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-delete-mpu-retention").await?;
    let key = "file1";

    let version_id = complete_one_part_upload_with_lock(
        &client,
        &bucket,
        key,
        Some(ObjectLockMode::Governance),
        Some(date_2030()),
        None,
    )
    .await?;
    let error = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("retained multipart object version delete should fail");
    assert_error_contains(&error, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies delete markers do not bypass retained prior versions.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_delete_object_with_retention_and_marker() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_delete_object_with_retention_and_marker
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-delete-retention-marker").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    put_governance_retention(&client, &bucket, key, date_2030()).await?;
    let marker = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    let error = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("retained prior version delete should fail after marker creation");
    assert_error_contains(&error, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(
            marker
                .version_id()
                .ok_or_else(|| anyhow!("delete marker response missing version ID"))?,
        )
        .send()
        .await?;
    let error = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("retained prior version delete should still fail after marker removal");
    assert_error_contains(&error, "AccessDenied");

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies multi-delete reports retained objects as per-object errors.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_multi_delete_object_with_retention() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_multi_delete_object_with_retention
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-multi-delete-retention").await?;
    let key1 = "file1";
    let key2 = "file2";
    let version_id1 = put_object(&client, &bucket, key1).await?;
    let version_id2 = put_object(&client, &bucket, key2).await?;

    put_governance_retention(&client, &bucket, key1, date_2030()).await?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(
                    ObjectIdentifier::builder()
                        .key(key1)
                        .version_id(&version_id1)
                        .build()?,
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key(key2)
                        .version_id(&version_id2)
                        .build()?,
                )
                .build()?,
        )
        .send()
        .await?;

    assert_eq!(response.deleted().len(), 1);
    assert_eq!(response.errors().len(), 1);
    let failed = &response.errors()[0];
    assert_eq!(failed.code(), Some("AccessDenied"));
    assert_eq!(failed.key(), Some(key1));
    assert_eq!(failed.version_id(), Some(version_id1.as_str()));
    let deleted = &response.deleted()[0];
    assert_eq!(deleted.key(), Some(key2));
    assert_eq!(deleted.version_id(), Some(version_id2.as_str()));

    let response = client
        .delete_objects()
        .bucket(&bucket)
        .bypass_governance_retention(true)
        .delete(
            Delete::builder()
                .objects(
                    ObjectIdentifier::builder()
                        .key(key1)
                        .version_id(&version_id1)
                        .build()?,
                )
                .build()?,
        )
        .send()
        .await?;
    assert_eq!(response.errors().len(), 0);
    assert_eq!(response.deleted().len(), 1);
    assert_eq!(response.deleted()[0].key(), Some(key1));
    assert_eq!(
        response.deleted()[0].version_id(),
        Some(version_id1.as_str())
    );

    Ok(())
}

/// Verifies legal hold status can be set on and off.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_legal_hold() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_legal_hold
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-put-legal-hold").await?;
    let key = "file1";
    put_object(&client, &bucket, key).await?;

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
        .send()
        .await?;
    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;

    Ok(())
}

/// Verifies legal hold APIs reject buckets without Object Lock.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_legal_hold_invalid_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_legal_hold_invalid_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-put-legal-hold-invalid-bucket").await?;
    let key = "file1";
    put_unversioned_object(&client, &bucket, key).await?;

    let error = client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
        .send()
        .await
        .expect_err("legal hold on bucket without Object Lock should fail");
    assert_error_contains(&error, "InvalidRequest");

    Ok(())
}

/// Verifies invalid legal hold statuses are malformed XML.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_put_legal_hold_invalid_status() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_put_legal_hold_invalid_status
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket =
        create_object_lock_bucket(&client, "object-lock-put-legal-hold-invalid-status").await?;
    let key = "file1";
    put_object(&client, &bucket, key).await?;

    let error = client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::from("abc")))
        .send()
        .await
        .expect_err("invalid legal hold status should be malformed XML");
    assert_error_contains(&error, "MalformedXML");

    Ok(())
}

/// Verifies legal hold status can be read.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_legal_hold() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_legal_hold
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-get-legal-hold").await?;
    let key = "file1";
    put_object(&client, &bucket, key).await?;

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
        .send()
        .await?;
    let output = client
        .get_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(
        output.legal_hold().and_then(|hold| hold.status()),
        Some(&ObjectLockLegalHoldStatus::On)
    );

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;
    let output = client
        .get_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(
        output.legal_hold().and_then(|hold| hold.status()),
        Some(&ObjectLockLegalHoldStatus::Off)
    );

    Ok(())
}

/// Verifies GetObjectLegalHold rejects buckets without Object Lock.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_legal_hold_invalid_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_legal_hold_invalid_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_bucket(&client, "object-lock-get-legal-hold-invalid-bucket").await?;
    let key = "file1";
    put_unversioned_object(&client, &bucket, key).await?;

    let error = client
        .get_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect_err("GetObjectLegalHold on bucket without Object Lock should fail");
    assert_error_contains(&error, "InvalidRequest");

    Ok(())
}

/// Verifies legal hold ON blocks exact version deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_delete_object_with_legal_hold_on() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_delete_object_with_legal_hold_on
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-delete-legal-hold-on").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
        .send()
        .await?;
    let error = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("legal hold ON should block exact version delete");
    assert_error_contains(&error, "AccessDenied");
    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;

    Ok(())
}

/// Verifies legal hold ON blocks multipart object deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_delete_multipart_object_with_legal_hold_on() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_delete_multipart_object_with_legal_hold_on
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-delete-mpu-legal-hold-on").await?;
    let key = "file1";

    let version_id = complete_one_part_upload_with_lock(
        &client,
        &bucket,
        key,
        None,
        None,
        Some(ObjectLockLegalHoldStatus::On),
    )
    .await?;
    let error = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("multipart object legal hold ON should block exact version delete");
    assert_error_contains(&error, "AccessDenied");
    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;

    Ok(())
}

/// Verifies legal hold OFF allows exact version deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_delete_object_with_legal_hold_off() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_delete_object_with_legal_hold_off
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-delete-legal-hold-off").await?;
    let key = "file1";
    let version_id = put_object(&client, &bucket, key).await?;

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await?;

    Ok(())
}

/// Verifies Object Lock metadata is returned by HeadObject.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_get_obj_metadata() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_get_obj_metadata
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-metadata").await?;
    let key = "file1";
    put_object(&client, &bucket, key).await?;

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
        .send()
        .await?;
    put_governance_retention(&client, &bucket, key, date_2030()).await?;

    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(
        head.object_lock_retain_until_date().map(DateTime::secs),
        Some(RETAIN_2030)
    );
    assert_eq!(
        head.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(
            head.version_id()
                .ok_or_else(|| anyhow!("HeadObject response missing version ID"))?,
        )
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies Object Lock headers on upload become object metadata.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_uploading_obj() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_uploading_obj
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-uploading").await?;
    let key = "file1";

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(date_2030())
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await?;

    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(
        head.object_lock_retain_until_date().map(DateTime::secs),
        Some(RETAIN_2030)
    );
    assert_eq!(
        head.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );

    client
        .put_object_legal_hold()
        .bucket(&bucket)
        .key(key)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(
            head.version_id()
                .ok_or_else(|| anyhow!("HeadObject response missing version ID"))?,
        )
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies governance mode can be changed with bypass.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_changing_mode_from_governance_with_bypass() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_changing_mode_from_governance_with_bypass
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-change-governance-bypass").await?;
    let key = "file1";
    put_object_with_governance_retention(&client, &bucket, key, date_2030()).await?;

    client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .retention(retention(ObjectLockRetentionMode::Compliance, date_2030()))
        .bypass_governance_retention(true)
        .send()
        .await?;

    Ok(())
}

/// Verifies governance mode cannot be changed without bypass.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_changing_mode_from_governance_without_bypass() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_changing_mode_from_governance_without_bypass
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket =
        create_object_lock_bucket(&client, "object-lock-change-governance-no-bypass").await?;
    let key = "file1";
    put_object_with_governance_retention(&client, &bucket, key, date_2030()).await?;

    let error = client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .retention(retention(ObjectLockRetentionMode::Compliance, date_2030()))
        .send()
        .await
        .expect_err("changing governance mode without bypass should fail");
    assert_error_contains(&error, "AccessDenied");

    Ok(())
}

/// Verifies compliance mode cannot be changed to governance.
#[tokio::test]
#[serial(conformance)]
async fn test_object_lock_changing_mode_from_compliance() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_lock_changing_mode_from_compliance
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = create_object_lock_bucket(&client, "object-lock-change-compliance").await?;
    let key = "file1";
    put_object_with_compliance_retention(&client, &bucket, key, date_2030()).await?;

    let error = client
        .put_object_retention()
        .bucket(&bucket)
        .key(key)
        .retention(retention(ObjectLockRetentionMode::Governance, date_2030()))
        .send()
        .await
        .expect_err("changing compliance mode should fail");
    assert_error_contains(&error, "AccessDenied");

    Ok(())
}
