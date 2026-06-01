//! Real-binary HTTP auth and owner-compatibility conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_auth`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use std::time::{Duration, SystemTime};

use anyhow::{Result, bail};
use aws_sdk_s3::{
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

#[tokio::test]
#[serial(conformance)]
async fn test_owner_signed_unsigned_and_presigned_get() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_authenticated
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_not_expired
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_out_range_zero
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_get_x_amz_expires_out_max_range
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth");
    let key = "shared.txt";
    let content = b"shared through a presigned URL";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, key, content).await?;

    let signed = client.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(signed.body.collect().await?.into_bytes().as_ref(), content);

    let unsigned = raw_http::unsigned_get(&server, &format!("/{bucket}/{key}")).await?;
    assert_status(&unsigned, StatusCode::FORBIDDEN);

    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60))?)
        .await?;
    let response = raw_http::get_url(presigned.uri()).await?;
    assert_status(&response, StatusCode::OK);
    assert_eq!(response.bytes().await?.as_ref(), content);

    let zero_expires = presigned
        .uri()
        .replace("X-Amz-Expires=60", "X-Amz-Expires=0");
    let response = raw_http::get_url(&zero_expires).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    let too_large_expires = presigned
        .uri()
        .replace("X-Amz-Expires=60", "X-Amz-Expires=604801");
    let response = raw_http::get_url(&too_large_expires).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    let expired = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .presigned(
            PresigningConfig::builder()
                .start_time(SystemTime::now() - Duration::from_secs(3600))
                .expires_in(Duration::from_secs(1))
                .build()?,
        )
        .await?;
    let response = raw_http::get_url(expired.uri()).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    let malformed_uri = format!("{}&s3ite-malformed-signature=true", presigned.uri());
    let response = raw_http::get_url(&malformed_uri).await?;
    assert_status(&response, StatusCode::FORBIDDEN);

    Ok(())
}

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

#[tokio::test]
#[serial(conformance)]
async fn test_owner_only_acl_surface() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-auth");
    let rejected_bucket = bucket_name("s3ite-auth-reject");
    let key = "object.txt";

    create_bucket(&client, &bucket).await?;
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

    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await?;
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

    put_object(&client, &bucket, key, b"owner-only").await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("private-write.txt")
        .acl(ObjectCannedAcl::Private)
        .body(ByteStream::from_static(b"private"))
        .send()
        .await?;
    client
        .copy_object()
        .bucket(&bucket)
        .key("private-copy.txt")
        .copy_source(format!("{bucket}/private-write.txt"))
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await?;

    let object_acl = client
        .get_object_acl()
        .bucket(&bucket)
        .key(key)
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

    client
        .put_object_acl()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::Private)
        .send()
        .await?;
    client
        .put_object_acl()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await?;

    let error = match client
        .put_object_acl()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
    {
        Ok(_) => bail!("public object ACL should be rejected"),
        Err(error) => error,
    };
    assert_error_contains(&error, "AccessControlListNotSupported");

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

    let error = match client
        .create_bucket()
        .bucket(&rejected_bucket)
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
