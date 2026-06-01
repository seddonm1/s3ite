//! Real-binary HTTP auth and owner-compatibility conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_auth`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use std::time::{Duration, SystemTime};

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{BucketCannedAcl, ObjectCannedAcl, Permission},
};
use reqwest::StatusCode;
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, put_object},
    raw_http, sdk,
    server::TestServer,
};

const SHARED_KEY: &str = "shared.txt";
const SHARED_CONTENT: &[u8] = b"shared through a presigned URL";

/// Verifies signed owner requests can read private objects.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_authenticated() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_authenticated
    let (_server, client, bucket) = shared_object().await?;

    let signed = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .send()
        .await?;
    assert_eq!(
        signed.body.collect().await?.into_bytes().as_ref(),
        SHARED_CONTENT
    );

    Ok(())
}

/// Verifies unsigned object reads are denied by the owner-only auth model.
#[tokio::test]
#[serial(conformance)]
async fn test_owner_unsigned_get_denied() -> Result<()> {
    let (server, _client, bucket) = shared_object().await?;

    let unsigned = raw_http::unsigned_get(&server, &format!("/{bucket}/{SHARED_KEY}")).await?;
    assert_status(&unsigned, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies a valid presigned GET URL reads object bytes.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_get_x_amz_expires_not_expired() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_not_expired
    let (_server, client, bucket) = shared_object().await?;
    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(100_000))?)
        .await?;
    let response = raw_http::get_url(presigned.uri()).await?;
    assert_status(&response, StatusCode::OK);
    assert_eq!(response.bytes().await?.as_ref(), SHARED_CONTENT);

    Ok(())
}

/// Verifies presigned GET URLs with zero expiry are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_get_x_amz_expires_out_range_zero() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_out_range_zero
    let (_server, client, bucket) = shared_object().await?;
    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;

    let zero_expires = presigned
        .uri()
        .replace("X-Amz-Expires=60", "X-Amz-Expires=0");
    let response = raw_http::get_url(&zero_expires).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies presigned GET URLs above the maximum expiry are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_get_x_amz_expires_out_max_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_out_max_range
    let (_server, client, bucket) = shared_object().await?;
    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;

    let too_large_expires = presigned
        .uri()
        .replace("X-Amz-Expires=60", "X-Amz-Expires=604801");
    let response = raw_http::get_url(&too_large_expires).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies presigned GET URLs below the minimum expiry are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_get_x_amz_expires_out_positive_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_out_positive_range
    let (_server, client, bucket) = shared_object().await?;
    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;

    let negative_expires = presigned
        .uri()
        .replace("X-Amz-Expires=60", "X-Amz-Expires=-7");
    let response = raw_http::get_url(&negative_expires).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies already-expired presigned GET URLs are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_presigned_get_expired_start_time_fails() -> Result<()> {
    let (_server, client, bucket) = shared_object().await?;
    let expired = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .presigned(
            PresigningConfig::builder()
                .start_time(SystemTime::now() - Duration::from_secs(3600))
                .expires_in(Duration::from_secs(1))
                .build()?,
        )
        .await?;
    let response = raw_http::get_url(expired.uri()).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies tampered presigned GET URLs are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_presigned_get_tampered_signature_fails() -> Result<()> {
    let (_server, client, bucket) = shared_object().await?;
    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(SHARED_KEY)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;

    let malformed_uri = format!("{}&s3ite-malformed-signature=true", presigned.uri());
    let response = raw_http::get_url(&malformed_uri).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    Ok(())
}

/// Verifies presigned PUT accepts owner-equivalent private ACL headers.
#[tokio::test]
#[serial(conformance)]
async fn test_presigned_put_with_private_acl() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_presigned_put_object_with_acl
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth");
    let key = "foo";

    create_bucket(&client, &bucket).await?;
    let presigned = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::Private)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;
    let response = raw_http::client()?
        .put(presigned.uri())
        .header("x-amz-acl", "private")
        .body("hello world")
        .send()
        .await?;
    assert_status(&response, StatusCode::OK);

    let body = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(body.as_ref(), b"hello world");

    Ok(())
}

/// Verifies already-expired presigned PUT URLs are rejected before writing an object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_put_authenticated_expired() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_put_authenticated_expired
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth");
    let key = "expired-put.txt";

    create_bucket(&client, &bucket).await?;
    let presigned = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .presigned(
            PresigningConfig::builder()
                .start_time(SystemTime::now() - Duration::from_secs(3600))
                .expires_in(Duration::from_secs(1))
                .build()?,
        )
        .await?;
    let response = raw_http::client()?
        .put(presigned.uri())
        .body("blocked")
        .send()
        .await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    let error = match client.head_object().bucket(&bucket).key(key).send().await {
        Ok(_) => bail!("expired presigned PUT should not create an object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NotFound");

    Ok(())
}

async fn shared_object() -> Result<(TestServer, Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth");
    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, SHARED_KEY, SHARED_CONTENT).await?;
    Ok((server, client, bucket))
}

/// Verifies synthetic bucket ACL reads return owner full control.
#[tokio::test]
#[serial(conformance)]
async fn test_get_bucket_acl_returns_owner_full_control() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    let bucket_acl = client.get_bucket_acl().bucket(&bucket).send().await?;
    assert_eq!(
        bucket_acl.owner().and_then(|owner| owner.id()),
        Some("s3ite")
    );
    assert_eq!(bucket_acl.grants().len(), 1);
    assert_eq!(
        bucket_acl.grants()[0].permission(),
        Some(&Permission::FullControl)
    );

    Ok(())
}

/// Verifies private bucket ACL writes are accepted as owner no-ops.
#[tokio::test]
#[serial(conformance)]
async fn test_put_bucket_acl_private_is_noop() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await?;

    Ok(())
}

/// Verifies public bucket ACL writes are rejected as unsupported.
#[tokio::test]
#[serial(conformance)]
async fn test_put_bucket_acl_public_read_is_rejected() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    let error = match client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
    {
        Ok(_) => bail!("public bucket ACL should be rejected"),
        Err(error) => error,
    };
    assert_error_contains(&error, "AccessControlListNotSupported");

    Ok(())
}

/// Verifies explicit bucket grant ACL headers are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_put_bucket_acl_grant_header_is_rejected() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    let error = match client
        .put_bucket_acl()
        .bucket(&bucket)
        .grant_read("uri=\"http://acs.amazonaws.com/groups/global/AllUsers\"")
        .send()
        .await
    {
        Ok(_) => bail!("bucket ACL grant headers should be rejected"),
        Err(error) => error,
    };
    assert_error_contains(&error, "AccessControlListNotSupported");

    Ok(())
}

/// Verifies private object ACL writes are accepted as owner no-ops.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_private_acl_is_noop() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("private-write.txt")
        .acl(ObjectCannedAcl::Private)
        .body(ByteStream::from_static(b"private"))
        .send()
        .await?;

    Ok(())
}

/// Verifies owner-equivalent copy ACL writes are accepted as no-ops.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_bucket_owner_full_control_acl_is_noop() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    put_object(&client, &bucket, "source.txt", b"owner-only").await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("private-copy.txt")
        .copy_source(format!("{bucket}/source.txt"))
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await?;

    Ok(())
}

/// Verifies synthetic object ACL reads return owner full control.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_acl_returns_owner_full_control() -> Result<()> {
    let (_server, client, bucket, key) = owner_object().await?;
    let object_acl = client
        .get_object_acl()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;
    assert_eq!(
        object_acl.owner().and_then(|owner| owner.id()),
        Some("s3ite")
    );
    assert_eq!(object_acl.grants().len(), 1);
    assert_eq!(
        object_acl.grants()[0].permission(),
        Some(&Permission::FullControl)
    );

    Ok(())
}

/// Verifies private PutObjectAcl requests are accepted as owner no-ops.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_acl_private_is_noop() -> Result<()> {
    let (_server, client, bucket, key) = owner_object().await?;
    client
        .put_object_acl()
        .bucket(&bucket)
        .key(&key)
        .acl(ObjectCannedAcl::Private)
        .send()
        .await?;

    Ok(())
}

/// Verifies bucket-owner PutObjectAcl requests are accepted as owner no-ops.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_acl_bucket_owner_full_control_is_noop() -> Result<()> {
    let (_server, client, bucket, key) = owner_object().await?;
    client
        .put_object_acl()
        .bucket(&bucket)
        .key(&key)
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await?;

    Ok(())
}

/// Verifies public PutObjectAcl requests are rejected as unsupported.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_acl_public_read_is_rejected() -> Result<()> {
    let (_server, client, bucket, key) = owner_object().await?;
    let error = match client
        .put_object_acl()
        .bucket(&bucket)
        .key(&key)
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
    {
        Ok(_) => bail!("public object ACL should be rejected"),
        Err(error) => error,
    };
    assert_error_contains(&error, "AccessControlListNotSupported");

    Ok(())
}

/// Verifies public object canned ACL writes are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_public_read_acl_is_rejected() -> Result<()> {
    let (_server, client, bucket) = owner_bucket().await?;
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("public.txt")
        .acl(ObjectCannedAcl::PublicRead)
        .body(ByteStream::from_static(b"public"))
        .send()
        .await
    {
        Ok(_) => bail!("public object write ACL should be rejected"),
        Err(error) => error,
    };
    assert_error_contains(&error, "AccessControlListNotSupported");

    Ok(())
}

/// Verifies public bucket canned ACL creation is rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_create_bucket_public_read_acl_is_rejected() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth-reject");

    let error = match client
        .create_bucket()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
    {
        Ok(_) => bail!("public bucket create ACL should be rejected"),
        Err(error) => error,
    };
    assert_error_contains(&error, "AccessControlListNotSupported");

    Ok(())
}

async fn owner_bucket() -> Result<(TestServer, Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth");
    create_bucket(&client, &bucket).await?;
    Ok((server, client, bucket))
}

async fn owner_object() -> Result<(TestServer, Client, String, String)> {
    let (server, client, bucket) = owner_bucket().await?;
    let key = "object.txt".to_string();
    put_object(&client, &bucket, &key, b"owner-only").await?;
    Ok((server, client, bucket, key))
}
