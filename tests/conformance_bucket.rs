//! Real-binary HTTP conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_bucket`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes},
    raw_http, sdk,
    server::{TestServer, TestServerOptions},
};
use uuid::Uuid;

/// Verifies a fresh server reports no buckets.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_empty
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);

    let output = client.list_buckets().send().await?;
    assert!(output.buckets().is_empty());

    Ok(())
}

/// Verifies optional binary startup layers still serve ordinary path-style requests.
#[tokio::test]
#[serial(conformance)]
async fn test_server_domain_and_cors_startup_smoke() -> Result<()> {
    let server = TestServer::spawn_with(TestServerOptions {
        domain_name: Some("localhost".to_string()),
        permissive_cors: Some(true),
    })
    .await?;
    let client = sdk::client(&server);

    let output = client.list_buckets().send().await?;
    assert!(output.buckets().is_empty());

    Ok(())
}

/// Verifies the binary serves requests when the optional CORS layer is disabled.
#[tokio::test]
#[serial(conformance)]
async fn test_server_without_cors_startup_smoke() -> Result<()> {
    let server = TestServer::spawn_with(TestServerOptions {
        permissive_cors: Some(false),
        ..Default::default()
    })
    .await?;
    let client = sdk::client(&server);

    let output = client.list_buckets().send().await?;
    assert!(output.buckets().is_empty());

    Ok(())
}

/// Verifies bucket creation appears in listing and deletion removes it.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;

    let output = client.list_buckets().send().await?;
    assert!(
        output
            .buckets()
            .iter()
            .any(|listed| listed.name() == Some(bucket.as_str()))
    );

    client.delete_bucket().bucket(&bucket).send().await?;

    let output = client.list_buckets().send().await?;
    assert!(
        output
            .buckets()
            .iter()
            .all(|listed| listed.name() != Some(bucket.as_str()))
    );

    Ok(())
}

/// Verifies ListObjects v1 reports NoSuchBucket for a missing bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_notexist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_notexist
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-missing");

    let error = match client.list_objects().bucket(&bucket).send().await {
        Ok(_) => bail!("ListObjects for a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

/// Verifies ListObjectsV2 reports NoSuchBucket for a missing bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_bucketv2_notexist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucketv2_notexist
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-missing");

    let error = match client.list_objects_v2().bucket(&bucket).send().await {
        Ok(_) => bail!("ListObjectsV2 for a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

/// Verifies DeleteBucket reports NoSuchBucket for a missing bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_delete_notexist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_delete_notexist
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-missing");

    let error = match client.delete_bucket().bucket(&bucket).send().await {
        Ok(_) => bail!("DeleteBucket for a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

/// Verifies DeleteBucket refuses buckets that still contain objects.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_delete_nonempty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_delete_nonempty
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await?;

    let error = match client.delete_bucket().bucket(&bucket).send().await {
        Ok(_) => bail!("DeleteBucket for a non-empty bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "BucketNotEmpty");

    Ok(())
}

/// Verifies creating an existing owner bucket is accepted as idempotent.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_exists() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_exists
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;
    client.create_bucket().bucket(&bucket).send().await?;

    Ok(())
}

/// Verifies recreating an existing bucket does not remove its objects.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_recreate_not_overriding() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_recreate_not_overriding
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("mykey1")
        .body(ByteStream::from_static(b"mykey1"))
        .send()
        .await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("mykey2")
        .body(ByteStream::from_static(b"mykey2"))
        .send()
        .await?;

    client.create_bucket().bucket(&bucket).send().await?;

    let response = client.list_objects().bucket(&bucket).send().await?;
    let keys = response
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["mykey1", "mykey2"]);
    assert_eq!(
        get_object_bytes(&client, &bucket, "mykey1").await?,
        b"mykey1"
    );
    assert_eq!(
        get_object_bytes(&client, &bucket, "mykey2").await?,
        b"mykey2"
    );

    Ok(())
}

/// Verifies HeadBucket succeeds for an existing bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_head
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    client.create_bucket().bucket(&bucket).send().await?;
    client.head_bucket().bucket(&bucket).send().await?;

    Ok(())
}

/// Verifies HeadBucket reports not found for a missing bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_head_notexist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_head_notexist
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-missing");

    let error = match client.head_bucket().bucket(&bucket).send().await {
        Ok(_) => bail!("HeadBucket for a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(404)");

    Ok(())
}

/// Verifies GetBucketLocation returns the default us-east-1-style empty location.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_get_location() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_get_location
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-conf");

    create_bucket(&client, &bucket).await?;

    let response = client.get_bucket_location().bucket(&bucket).send().await?;
    assert_eq!(
        response
            .location_constraint()
            .map(aws_sdk_s3::types::BucketLocationConstraint::as_str),
        Some("")
    );

    Ok(())
}

/// Verifies bad credentials and unsigned ListBuckets requests are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_list_buckets_bad_auth() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_list_buckets_bad_auth
    let server = TestServer::spawn().await?;
    let unsigned = raw_http::unsigned_get(&server, "/").await?;
    assert_status(&unsigned, StatusCode::FORBIDDEN);

    let client = sdk::bad_secret_client(&server);

    let error = match client.list_buckets().send().await {
        Ok(_) => bail!("list buckets with bad credentials should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "403");

    Ok(())
}

/// Verifies invalid access-key credentials are rejected for ListBuckets.
#[tokio::test]
#[serial(conformance)]
async fn test_list_buckets_invalid_auth() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_list_buckets_invalid_auth
    let server = TestServer::spawn().await?;
    let client = sdk::bad_access_key_client(&server);

    let error = match client.list_buckets().send().await {
        Ok(_) => bail!("list buckets with an invalid access key should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "403");

    Ok(())
}

/// Verifies multiple created buckets appear in ListBuckets.
#[tokio::test]
#[serial(conformance)]
async fn test_buckets_create_then_list() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_buckets_create_then_list
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let buckets = (0..5)
        .map(|_| bucket_name("s3ite-conf"))
        .collect::<Vec<_>>();

    for bucket in &buckets {
        client.create_bucket().bucket(bucket).send().await?;
    }

    let response = client.list_buckets().send().await?;
    let listed = response
        .buckets()
        .iter()
        .filter_map(|bucket| bucket.name())
        .collect::<Vec<_>>();
    for bucket in &buckets {
        assert!(listed.contains(&bucket.as_str()));
    }

    Ok(())
}

/// Verifies ListBuckets supports max-buckets pagination and continuation tokens.
#[tokio::test]
#[serial(conformance)]
async fn test_list_buckets_paginated() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_list_buckets_paginated
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let prefix = format!("s3ite-paged-{}-", Uuid::new_v4().simple());

    let empty = client
        .list_buckets()
        .prefix(&prefix)
        .max_buckets(1)
        .send()
        .await?;
    assert!(empty.buckets().is_empty());
    assert_eq!(empty.continuation_token(), None);

    let bucket1 = format!("{prefix}a");
    create_bucket(&client, &bucket1).await?;
    let one = client
        .list_buckets()
        .prefix(&prefix)
        .max_buckets(1)
        .send()
        .await?;
    assert_eq!(
        one.buckets()
            .iter()
            .filter_map(|bucket| bucket.name())
            .collect::<Vec<_>>(),
        vec![bucket1.as_str()]
    );
    assert_eq!(one.continuation_token(), None);

    let bucket2 = format!("{prefix}b");
    create_bucket(&client, &bucket2).await?;
    let first = client
        .list_buckets()
        .prefix(&prefix)
        .max_buckets(1)
        .send()
        .await?;
    assert_eq!(
        first
            .buckets()
            .iter()
            .filter_map(|bucket| bucket.name())
            .collect::<Vec<_>>(),
        vec![bucket1.as_str()]
    );
    let token = first
        .continuation_token()
        .ok_or_else(|| anyhow::anyhow!("truncated ListBuckets response missing token"))?;

    let second = client
        .list_buckets()
        .prefix(&prefix)
        .continuation_token(token)
        .max_buckets(1)
        .send()
        .await?;
    assert_eq!(
        second
            .buckets()
            .iter()
            .filter_map(|bucket| bucket.name())
            .collect::<Vec<_>>(),
        vec![bucket2.as_str()]
    );
    assert_eq!(second.continuation_token(), None);

    Ok(())
}

/// Verifies ListBuckets creation times are recent.
#[tokio::test]
#[serial(conformance)]
async fn test_buckets_list_ctime() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_buckets_list_ctime
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let before = DateTime::from_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            .saturating_sub(86_400) as i64,
    );
    let buckets = (0..5)
        .map(|_| bucket_name("s3ite-conf"))
        .collect::<Vec<_>>();

    for bucket in &buckets {
        client.create_bucket().bucket(bucket).send().await?;
    }

    let response = client.list_buckets().send().await?;
    for bucket in response.buckets() {
        if bucket
            .name()
            .is_some_and(|name| buckets.iter().any(|b| b == name))
        {
            let creation_date = bucket
                .creation_date()
                .expect("listed bucket should include creation date");
            assert!(creation_date.secs() >= before.secs());
        }
    }

    Ok(())
}

/// Verifies valid bucket names may start with a letter.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_starts_alpha() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_starts_alpha
    create_valid_bucket(format!("a-s3ite-{}", Uuid::new_v4().simple())).await
}

/// Verifies valid bucket names may start with a digit.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_starts_digit() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_starts_digit
    create_valid_bucket(format!("0-s3ite-{}", Uuid::new_v4().simple())).await
}

/// Verifies valid bucket names may contain periods.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_contains_period() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_contains_period
    create_valid_bucket(format!("s3ite.{}", Uuid::new_v4().simple())).await
}

/// Verifies valid bucket names may contain hyphens.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_contains_hyphen() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_contains_hyphen
    create_valid_bucket(format!("s3ite-{}", Uuid::new_v4().simple())).await
}

/// Verifies valid bucket names can be sixty characters long.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_long_60() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_long_60
    create_valid_bucket(bucket_name_with_length(60)).await
}

/// Verifies valid bucket names can be sixty-one characters long.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_long_61() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_long_61
    create_valid_bucket(bucket_name_with_length(61)).await
}

/// Verifies valid bucket names can be sixty-two characters long.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_long_62() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_long_62
    create_valid_bucket(bucket_name_with_length(62)).await
}

/// Verifies valid bucket names can be sixty-three characters long.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_good_long_63() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_good_long_63
    create_valid_bucket(bucket_name_with_length(63)).await
}

/// Verifies the DNS-long bucket naming case remains accepted.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_dns_long() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_dns_long
    create_valid_bucket(bucket_name_with_length(63)).await
}

/// Verifies long valid bucket names can be listed.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_list_long_name() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_list_long_name
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name_with_length(61);

    client.create_bucket().bucket(&bucket).send().await?;
    let response = client.list_objects().bucket(&bucket).send().await?;
    assert!(response.contents().is_empty());

    Ok(())
}

/// Verifies bucket names cannot start with an underscore.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_bad_starts_nonalpha() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_bad_starts_nonalpha
    assert_invalid_bucket_name(&format!("_{}", bucket_name("s3ite-conf"))).await
}

/// Verifies one-character bucket names are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_bad_short_one() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_bad_short_one
    assert_invalid_bucket_name("a").await
}

/// Verifies two-character bucket names are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_bad_short_two() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_bad_short_two
    assert_invalid_bucket_name("aa").await
}

/// Verifies IPv4-shaped bucket names are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_bad_ip() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_bad_ip
    assert_invalid_bucket_name("192.168.5.123").await
}

/// Verifies bucket names cannot contain underscores.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_dns_underscore() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_dns_underscore
    assert_invalid_bucket_name("foo_bar").await
}

/// Verifies bucket names cannot end in a dash.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_dns_dash_at_end() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_dns_dash_at_end
    assert_invalid_bucket_name("foo-").await
}

/// Verifies bucket names cannot contain adjacent dots.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_dns_dot_dot() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_dns_dot_dot
    assert_invalid_bucket_name("foo..bar").await
}

/// Verifies bucket names cannot contain dot-dash transitions.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_dns_dot_dash() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_dns_dot_dash
    assert_invalid_bucket_name("foo.-bar").await
}

/// Verifies bucket names cannot contain dash-dot transitions.
#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_naming_dns_dash_dot() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_bucket_create_naming_dns_dash_dot
    assert_invalid_bucket_name("foo-.bar").await
}

async fn create_valid_bucket(bucket: String) -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    client.create_bucket().bucket(bucket).send().await?;
    Ok(())
}

async fn assert_invalid_bucket_name(bucket: &str) -> Result<()> {
    let server = TestServer::spawn().await?;
    let response =
        raw_http::signed_request(&server, Method::PUT, &format!("/{bucket}"), Vec::new()).await?;
    assert_status(&response, StatusCode::BAD_REQUEST);
    let body = response.text().await?;
    assert!(
        body.contains("InvalidBucketName"),
        "expected InvalidBucketName error, got {body}"
    );
    Ok(())
}

fn bucket_name_with_length(length: usize) -> String {
    let mut name = format!("s3ite-{}", Uuid::new_v4().simple());
    while name.len() < length {
        name.push('a');
    }
    name.truncate(length);
    name
}
