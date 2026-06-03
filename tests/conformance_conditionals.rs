//! Real-binary HTTP conditional request conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_conditionals`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::{ByteStream, DateTime},
    types::{
        BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    },
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_bucket_versioning, put_object},
    sdk,
    server::TestServer,
};

const KEY: &str = "foo";

/// Verifies GET succeeds when If-Match matches the current ETag.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifmatch_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmatch_good
    let (_server, client, bucket, state) = seeded_object().await?;

    let output = client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(&state.etag)
        .send()
        .await?;
    assert_eq!(output.body.collect().await?.into_bytes().as_ref(), b"bar");

    Ok(())
}

/// Verifies GET fails when If-Match does not match the current ETag.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifmatch_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmatch_failed
    let (_server, client, bucket, _state) = seeded_object().await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    Ok(())
}

/// Verifies GET returns 304 when If-None-Match matches the current ETag.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifnonematch_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifnonematch_good
    let (_server, client, bucket, state) = seeded_object().await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match(&state.etag)
        .send()
        .await
    {
        Ok(_) => bail!("matching If-None-Match should return 304"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");

    Ok(())
}

/// Verifies GET succeeds when If-None-Match does not match the current ETag.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifnonematch_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifnonematch_failed
    let (_server, client, bucket, _state) = seeded_object().await?;

    let output = client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match("\"ABCORZ\"")
        .send()
        .await?;
    assert_eq!(output.body.collect().await?.into_bytes().as_ref(), b"bar");

    Ok(())
}

/// Verifies GET succeeds when If-Modified-Since is before last modified.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifmodifiedsince_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmodifiedsince_good
    let (_server, client, bucket, state) = seeded_object().await?;

    let output = client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_modified_since(state.before)
        .send()
        .await?;
    assert_eq!(output.body.collect().await?.into_bytes().as_ref(), b"bar");

    Ok(())
}

/// Verifies GET returns 304 when If-Modified-Since is after last modified.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifmodifiedsince_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmodifiedsince_failed
    let (_server, client, bucket, state) = seeded_object().await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_modified_since(state.after)
        .send()
        .await
    {
        Ok(_) => bail!("unmodified object should return 304"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");

    Ok(())
}

/// Verifies GET fails when If-Unmodified-Since is before last modified.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifunmodifiedsince_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifunmodifiedsince_good
    let (_server, client, bucket, state) = seeded_object().await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_unmodified_since(state.before)
        .send()
        .await
    {
        Ok(_) => bail!("modified object should fail If-Unmodified-Since"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    Ok(())
}

/// Verifies GET succeeds when If-Unmodified-Since is after last modified.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_ifunmodifiedsince_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifunmodifiedsince_failed
    let (_server, client, bucket, state) = seeded_object().await?;

    let output = client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_unmodified_since(state.after)
        .send()
        .await?;
    assert_eq!(output.body.collect().await?.into_bytes().as_ref(), b"bar");

    Ok(())
}

/// Verifies 304 conditional read errors preserve the object ETag header.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_not_modified_errors_include_etag() -> Result<()> {
    let (_server, client, bucket, state) = seeded_object().await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match(&state.etag)
        .send()
        .await
    {
        Ok(_) => bail!("matching If-None-Match should return 304"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");
    let response = error
        .raw_response()
        .ok_or_else(|| anyhow::anyhow!("304 response missing raw response"))?;
    assert_eq!(response.headers().get("etag"), Some(state.etag.as_str()));

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_modified_since(state.after)
        .send()
        .await
    {
        Ok(_) => bail!("unmodified object should return 304"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");
    let response = error
        .raw_response()
        .ok_or_else(|| anyhow::anyhow!("304 response missing raw response"))?;
    assert_eq!(response.headers().get("etag"), Some(state.etag.as_str()));

    Ok(())
}

/// Verifies HeadObject honors the same conditional outcomes as GetObject.
#[tokio::test]
#[serial(conformance)]
async fn test_head_object_conditionals() -> Result<()> {
    let (_server, client, bucket, state) = seeded_object().await?;

    client
        .head_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(&state.etag)
        .send()
        .await?;

    let error = match client
        .head_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched HeadObject If-Match should fail"),
        Err(error) => error,
    };
    let response = error
        .raw_response()
        .ok_or_else(|| anyhow::anyhow!("412 response missing raw response"))?;
    assert_eq!(response.status().as_u16(), 412);

    let error = match client
        .head_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match(&state.etag)
        .send()
        .await
    {
        Ok(_) => bail!("matching HeadObject If-None-Match should return 304"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");
    let response = error
        .raw_response()
        .ok_or_else(|| anyhow::anyhow!("304 response missing raw response"))?;
    assert_eq!(response.headers().get("etag"), Some(state.etag.as_str()));

    let error = match client
        .head_object()
        .bucket(&bucket)
        .key(KEY)
        .if_modified_since(state.after)
        .send()
        .await
    {
        Ok(_) => bail!("unmodified HeadObject should return 304"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");

    let error = match client
        .head_object()
        .bucket(&bucket)
        .key(KEY)
        .if_unmodified_since(state.before)
        .send()
        .await
    {
        Ok(_) => bail!("modified HeadObject should fail If-Unmodified-Since"),
        Err(error) => error,
    };
    let response = error
        .raw_response()
        .ok_or_else(|| anyhow::anyhow!("412 response missing raw response"))?;
    assert_eq!(response.status().as_u16(), 412);

    Ok(())
}

/// Verifies a matching If-Match takes precedence over stale If-Unmodified-Since.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_if_match_allows_stale_if_unmodified_since() -> Result<()> {
    let (_server, client, bucket, state) = seeded_object().await?;

    let output = client
        .get_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(&state.etag)
        .if_unmodified_since(state.before)
        .send()
        .await?;
    assert_eq!(output.body.collect().await?.into_bytes().as_ref(), b"bar");

    Ok(())
}

/// Verifies PutObject succeeds when If-Match matches the current ETag.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifmatch_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifmatch_good
    let (_server, client, bucket, state) = seeded_object().await?;

    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(state.etag)
        .body(ByteStream::from_static(b"zar"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"zar");

    Ok(())
}

/// Verifies PutObject fails when If-Match does not match.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifmatch_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifmatch_failed
    let (_server, client, bucket, _state) = seeded_object().await?;

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("\"ABCORZ\"")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("mismatched If-Match should reject overwrite"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"bar");

    Ok(())
}

/// Verifies PutObject succeeds when If-None-Match does not match.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifnonmatch_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifnonmatch_good
    let (_server, client, bucket, _state) = seeded_object().await?;

    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match("\"ABCORZ\"")
        .body(ByteStream::from_static(b"zar"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"zar");

    Ok(())
}

/// Verifies PutObject fails when If-None-Match matches the current ETag.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifnonmatch_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifnonmatch_failed
    let (_server, client, bucket, state) = seeded_object().await?;

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match(state.etag)
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("matching If-None-Match should reject overwrite"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"bar");

    Ok(())
}

/// Verifies PutObject If-Match star overwrites an existing object.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifmatch_overwrite_existed_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifmatch_overwrite_existed_good
    let (_server, client, bucket, _state) = seeded_object().await?;

    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("*")
        .body(ByteStream::from_static(b"zar"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"zar");

    Ok(())
}

/// Verifies PutObject If-Match star fails when the object does not exist.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifmatch_nonexisted_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifmatch_nonexisted_failed
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("*")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("If-Match=* should fail for a missing object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

/// Verifies PutObject If-None-Match star creates a missing object.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifnonmatch_nonexisted_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifnonmatch_nonexisted_good
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match("*")
        .body(ByteStream::from_static(b"zar"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"zar");

    Ok(())
}

/// Verifies PutObject If-None-Match star refuses to overwrite an existing object.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_ifnonmatch_overwrite_existed_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifnonmatch_overwrite_existed_failed
    let (_server, client, bucket, _state) = seeded_object().await?;

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match("*")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("If-None-Match=* should reject an existing object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"bar");

    Ok(())
}

/// Verifies PutObject If-Match and If-None-Match follow current-object semantics.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    let first = client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match("*")
        .body(ByteStream::from_static(b"one"))
        .send()
        .await?;
    let first_etag = first
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    for if_none_match in ["*", first_etag.as_str()] {
        let error = match client
            .put_object()
            .bucket(&bucket)
            .key(KEY)
            .if_none_match(if_none_match)
            .body(ByteStream::from_static(b"blocked"))
            .send()
            .await
        {
            Ok(_) => bail!("If-None-Match should reject a matching current object"),
            Err(error) => error,
        };
        assert_error_contains(&error, "PreconditionFailed");
    }

    let second = client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match("badetag")
        .body(ByteStream::from_static(b"two"))
        .send()
        .await?;
    let second_etag = second
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(&first_etag)
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should reject a stale ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(&second_etag)
        .body(ByteStream::from_static(b"three"))
        .send()
        .await?;

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .send()
        .await?;
    for if_match in ["*", "badetag"] {
        let error = match client
            .put_object()
            .bucket(&bucket)
            .key(KEY)
            .if_match(if_match)
            .body(ByteStream::from_static(b"blocked"))
            .send()
            .await
        {
            Ok(_) => bail!("If-Match should fail after deleting the current object"),
            Err(error) => error,
        };
        assert_error_contains(&error, "NoSuchKey");
    }

    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_none_match(&second_etag)
        .body(ByteStream::from_static(b"four"))
        .send()
        .await?;
    let current = client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("*")
        .body(ByteStream::from_static(b"five"))
        .send()
        .await?;
    let current_etag = current
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("badetag")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should reject a bad current ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(current_etag)
        .body(ByteStream::from_static(b"six"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"six");

    Ok(())
}

/// Verifies versioned PutObject If-None-Match checks only the current object.
#[tokio::test]
#[serial(conformance)]
async fn test_put_current_object_if_none_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_current_object_if_none_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let response = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"data1"))
        .send()
        .await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();
    let response = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"data2"))
        .send()
        .await?;
    let latest_etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .send()
        .await
    {
        Ok(_) => bail!("If-None-Match=* should reject an existing current object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match(latest_etag)
        .send()
        .await
    {
        Ok(_) => bail!("If-None-Match should reject the current ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match(etag)
        .send()
        .await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("badetag")
        .send()
        .await?;

    Ok(())
}

/// Verifies versioned PutObject If-Match checks only the current object.
#[tokio::test]
#[serial(conformance)]
async fn test_put_current_object_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_current_object_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("*")
        .send()
        .await
    {
        Ok(_) => bail!("If-Match=* should fail for a missing current object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("badetag")
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should fail for a missing current object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    let response = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("deadbeef")
        .body(ByteStream::from_static(b"data1"))
        .send()
        .await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();
    let response = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("*")
        .body(ByteStream::from_static(b"data2"))
        .send()
        .await?;
    let latest_etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("badetag")
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should reject a mismatched current ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(etag)
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should not match an older version ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(latest_etag)
        .send()
        .await?;

    Ok(())
}

/// Verifies PutObject conditionals treat current delete markers as missing.
#[tokio::test]
#[serial(conformance)]
async fn test_put_object_current_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_current_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let response = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .send()
        .await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .send()
        .await
    {
        Ok(_) => bail!("If-None-Match=* should reject an existing current object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match(&etag)
        .send()
        .await
    {
        Ok(_) => bail!("If-None-Match should reject the current ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .send()
        .await?;

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), Some(true));

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("*")
        .send()
        .await
    {
        Ok(_) => bail!("If-Match=* should fail when current version is a delete marker"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("badetag")
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should fail when current version is a delete marker"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match(etag)
        .send()
        .await?;

    Ok(())
}

/// Verifies CopyObject succeeds when source If-Match matches.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_ifmatch_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifmatch_good
    let (_server, client, bucket, state) = seeded_object().await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar")
        .copy_source(format!("{bucket}/{KEY}"))
        .copy_source_if_match(state.etag)
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "bar").await?, b"bar");

    Ok(())
}

/// Verifies CopyObject fails when source If-Match does not match.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_ifmatch_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifmatch_failed
    let (_server, client, bucket, _state) = seeded_object().await?;

    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("bar")
        .copy_source(format!("{bucket}/{KEY}"))
        .copy_source_if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched copy source If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    Ok(())
}

/// Verifies CopyObject fails when source If-None-Match matches.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_ifnonematch_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifnonematch_good
    let (_server, client, bucket, state) = seeded_object().await?;

    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("bar")
        .copy_source(format!("{bucket}/{KEY}"))
        .copy_source_if_none_match(state.etag)
        .send()
        .await
    {
        Ok(_) => bail!("matching copy source If-None-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    Ok(())
}

/// Verifies CopyObject succeeds when source If-None-Match does not match.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_ifnonematch_failed() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifnonematch_failed
    let (_server, client, bucket, _state) = seeded_object().await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar")
        .copy_source(format!("{bucket}/{KEY}"))
        .copy_source_if_none_match("\"ABCORZ\"")
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "bar").await?, b"bar");

    Ok(())
}

/// Verifies CopyObject source date conditionals and ETag precedence.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_source_date_conditionals() -> Result<()> {
    let (_server, client, bucket, state) = seeded_object().await?;
    let copy_source = format!("{bucket}/{KEY}");

    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("after")
        .copy_source(&copy_source)
        .copy_source_if_modified_since(state.after)
        .send()
        .await
    {
        Ok(_) => bail!("unmodified copy source should fail If-Modified-Since"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .copy_object()
        .bucket(&bucket)
        .key("before")
        .copy_source(copy_source)
        .copy_source_if_match(&state.etag)
        .copy_source_if_unmodified_since(state.before)
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "before").await?, b"bar");

    Ok(())
}

/// Verifies multipart completion honors If-Match and If-None-Match.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_put_object_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_put_object_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    let response =
        successful_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some("*"))
            .await?;
    let mut etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();

    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        None,
        Some("*"),
        "PreconditionFailed",
    )
    .await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        None,
        Some(&etag),
        "PreconditionFailed",
    )
    .await?;

    let response = successful_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        None,
        Some("badetag"),
    )
    .await?;
    etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();
    let response =
        successful_conditional_multipart_upload(&client, &bucket, key, b"abc", Some(&etag), None)
            .await?;
    etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some("*"),
        None,
        "NoSuchKey",
    )
    .await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some("badetag"),
        None,
        "NoSuchKey",
    )
    .await?;

    let response =
        successful_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some(&etag))
            .await?;
    etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();
    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", Some("*"), None).await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some("badetag"),
        None,
        "PreconditionFailed",
    )
    .await?;
    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", Some(&etag), None)
        .await?;

    Ok(())
}

/// Verifies versioned multipart completion checks If-None-Match against latest.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_put_current_object_if_none_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_put_current_object_if_none_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let response =
        successful_conditional_multipart_upload(&client, &bucket, key, b"data1", None, Some("*"))
            .await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();
    let response =
        successful_conditional_multipart_upload(&client, &bucket, key, b"data2", None, None)
            .await?;
    let latest_etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();

    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        None,
        Some("*"),
        "PreconditionFailed",
    )
    .await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        None,
        Some(&latest_etag),
        "PreconditionFailed",
    )
    .await?;

    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some(&etag))
        .await?;
    successful_conditional_multipart_upload(&client, &bucket, key, b"abc", None, Some("badetag"))
        .await?;

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    Ok(())
}

/// Verifies versioned multipart completion checks If-Match against latest.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_put_current_object_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_put_current_object_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some("*"),
        None,
        "NoSuchKey",
    )
    .await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some("badetag"),
        None,
        "NoSuchKey",
    )
    .await?;

    let response = successful_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"data1",
        None,
        Some("deadbeef"),
    )
    .await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();
    let response =
        successful_conditional_multipart_upload(&client, &bucket, key, b"data2", Some("*"), None)
            .await?;
    let latest_etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("multipart completion missing ETag"))?
        .to_string();

    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some("badetag"),
        None,
        "PreconditionFailed",
    )
    .await?;
    failing_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some(&etag),
        None,
        "PreconditionFailed",
    )
    .await?;
    successful_conditional_multipart_upload(
        &client,
        &bucket,
        key,
        b"abc",
        Some(&latest_etag),
        None,
    )
    .await?;

    Ok(())
}

/// Verifies DeleteObject enforces If-Match before deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_if_match
    let (_server, client, bucket, state) = seeded_object_with_body(b"delete-me").await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched delete If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(get_object_bytes(&client, &bucket, KEY).await?, b"delete-me");

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match(state.etag)
        .send()
        .await?;
    assert_deleted(&client, &bucket, KEY).await?;

    Ok(())
}

/// Verifies DeleteObject enforces If-Match-Size before deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_if_match_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_if_match_size
    let (_server, client, bucket, _state) = seeded_object_with_body(b"delete-me").await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match_size(999)
        .send()
        .await
    {
        Ok(_) => bail!("mismatched delete size should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match_size(9)
        .send()
        .await?;
    assert_deleted(&client, &bucket, KEY).await?;

    Ok(())
}

/// Verifies DeleteObject enforces If-Match-Last-Modified-Time before deletion.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_if_match_last_modified_time() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_if_match_last_modified_time
    let (_server, client, bucket, state) = seeded_object_with_body(b"delete-me").await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match_last_modified_time(state.before)
        .send()
        .await
    {
        Ok(_) => bail!("mismatched delete Last-Modified should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .if_match_last_modified_time(state.last_modified)
        .send()
        .await?;
    assert_deleted(&client, &bucket, KEY).await?;

    Ok(())
}

/// Verifies DeleteObject conditionals on missing objects remain idempotent.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_if_match_missing_is_noop() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("missing")
        .if_match("\"ABCORZ\"")
        .send()
        .await?;

    Ok(())
}

/// Verifies DeleteObject If-Match evaluates the current version row.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_current_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_current_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("*")
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("badetag")
        .send()
        .await?;

    let response = client.put_object().bucket(&bucket).key(key).send().await?;
    let version = response
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("versioned PutObject missing version ID"))?
        .to_string();
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("badetag")
        .send()
        .await
    {
        Ok(_) => bail!("If-Match should reject a mismatched current ETag"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match(etag)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), Some(true));

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("*")
        .send()
        .await?;
    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("badetag")
        .send()
        .await
    {
        Ok(_) => bail!("concrete If-Match should fail against a current delete marker"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(version)
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("*")
        .send()
        .await?;

    Ok(())
}

/// Verifies DeleteObject Last-Modified conditions evaluate current delete markers.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_current_if_match_last_modified_time() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_current_if_match_last_modified_time
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client.put_object().bucket(&bucket).key(key).send().await?;
    let state = head_state(&client, &bucket, key).await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_last_modified_time(state.before)
        .send()
        .await
    {
        Ok(_) => bail!("mismatched current Last-Modified should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_last_modified_time(state.last_modified)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), Some(true));

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_last_modified_time(state.before)
        .send()
        .await
    {
        Ok(_) => bail!("current delete marker should still fail a mismatched Last-Modified"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    Ok(())
}

/// Verifies DeleteObject size conditions evaluate current delete markers.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_current_if_match_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_current_if_match_size
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client.put_object().bucket(&bucket).key(key).send().await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_size(9999)
        .send()
        .await
    {
        Ok(_) => bail!("mismatched current size should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_size(0)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), Some(true));

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_size(9999)
        .send()
        .await
    {
        Ok(_) => bail!("current delete marker should still fail a mismatched size"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    Ok(())
}

/// Verifies DeleteObject enforces If-Match against an exact object version.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_version_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_version_if_match
    let (_server, client, bucket, version, state) = seeded_versioned_object().await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched version delete If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match(&state.etag)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), None);

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match("*")
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match("\"ABCORZ\"")
        .send()
        .await?;

    let (version, _) = put_versioned_object(&client, &bucket, KEY).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(version)
        .if_match("*")
        .send()
        .await?;

    Ok(())
}

/// Verifies DeleteObject enforces If-Match-Last-Modified-Time for an exact version.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_version_if_match_last_modified_time() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_version_if_match_last_modified_time
    let (_server, client, bucket, version, state) = seeded_versioned_object().await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match_last_modified_time(state.before)
        .send()
        .await
    {
        Ok(_) => bail!("mismatched version delete Last-Modified should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match_last_modified_time(state.last_modified)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), None);

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match_last_modified_time(state.before)
        .send()
        .await?;

    Ok(())
}

/// Verifies DeleteObject enforces If-Match-Size for an exact version.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_version_if_match_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_version_if_match_size
    let (_server, client, bucket, version, _state) = seeded_versioned_object_with_body(b"").await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match_size(9999)
        .send()
        .await
    {
        Ok(_) => bail!("mismatched version delete size should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let response = client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match_size(0)
        .send()
        .await?;
    assert_eq!(response.delete_marker(), None);

    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&version)
        .if_match_size(9999)
        .send()
        .await?;

    Ok(())
}

/// Verifies DeleteObjects reports per-key If-Match failures.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "keep", b"keep").await?;
    put_object(&client, &bucket, "delete", b"delete").await?;

    let delete_etag = head_state(&client, &bucket, "delete").await?.etag;
    let delete = Delete::builder()
        .objects(
            ObjectIdentifier::builder()
                .key("keep")
                .e_tag("\"ABCORZ\"")
                .build()?,
        )
        .objects(
            ObjectIdentifier::builder()
                .key("delete")
                .e_tag(delete_etag)
                .build()?,
        )
        .build()?;

    let output = client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    assert_eq!(output.deleted().len(), 1);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].code(), Some("PreconditionFailed"));
    assert_eq!(get_object_bytes(&client, &bucket, "keep").await?, b"keep");
    assert_deleted(&client, &bucket, "delete").await?;

    Ok(())
}

/// Verifies DeleteObjects reports Last-Modified precondition failures.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_if_match_last_modified_time() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_if_match_last_modified_time
    let (_server, client, bucket, state) = seeded_object_with_body(b"").await?;

    let failed = ObjectIdentifier::builder()
        .key(KEY)
        .last_modified_time(state.before)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = ObjectIdentifier::builder()
        .key(KEY)
        .last_modified_time(state.last_modified)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(KEY));

    let missing = ObjectIdentifier::builder()
        .key(KEY)
        .last_modified_time(state.before)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(missing).build()?)
        .send()
        .await?;
    assert!(response.errors().is_empty());

    Ok(())
}

/// Verifies DeleteObjects reports size precondition failures.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_if_match_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_if_match_size
    let (_server, client, bucket, _state) = seeded_object_with_body(b"").await?;

    let failed = ObjectIdentifier::builder().key(KEY).size(9999).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = ObjectIdentifier::builder().key(KEY).size(0).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(KEY));

    let missing = ObjectIdentifier::builder().key(KEY).size(9999).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(missing).build()?)
        .send()
        .await?;
    assert!(response.errors().is_empty());

    Ok(())
}

/// Verifies DeleteObjects If-Match evaluates the current version row.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_current_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_current_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let response = client.put_object().bucket(&bucket).key(key).send().await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();

    let failed = ObjectIdentifier::builder()
        .key(key)
        .e_tag("badetag")
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = ObjectIdentifier::builder().key(key).e_tag(etag).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(key));
    assert_eq!(response.deleted()[0].delete_marker(), Some(true));

    let failed = ObjectIdentifier::builder()
        .key(key)
        .e_tag("badetag")
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    Ok(())
}

/// Verifies DeleteObjects Last-Modified conditions evaluate current delete markers.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_current_if_match_last_modified_time() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_current_if_match_last_modified_time
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client.put_object().bucket(&bucket).key(key).send().await?;
    let state = head_state(&client, &bucket, key).await?;

    let failed = ObjectIdentifier::builder()
        .key(key)
        .last_modified_time(state.before)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = ObjectIdentifier::builder()
        .key(key)
        .last_modified_time(state.last_modified)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(key));
    assert_eq!(response.deleted()[0].delete_marker(), Some(true));

    let failed = ObjectIdentifier::builder()
        .key(key)
        .last_modified_time(state.before)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    Ok(())
}

/// Verifies DeleteObjects size conditions evaluate current delete markers.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_current_if_match_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_current_if_match_size
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client.put_object().bucket(&bucket).key(key).send().await?;

    let failed = ObjectIdentifier::builder().key(key).size(9999).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = ObjectIdentifier::builder().key(key).size(0).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(key));
    assert_eq!(response.deleted()[0].delete_marker(), Some(true));

    let failed = ObjectIdentifier::builder().key(key).size(9999).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    Ok(())
}

/// Verifies DeleteObjects reports per-key If-Match failures for exact versions.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_version_if_match() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_version_if_match
    let (_server, client, bucket, version, state) = seeded_versioned_object().await?;

    let failed = delete_objects_for_version(KEY, &version)
        .e_tag("\"ABCORZ\"")
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = delete_objects_for_version(KEY, &version)
        .e_tag(state.etag)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(KEY));
    assert_eq!(response.deleted()[0].delete_marker(), None);

    let missing = delete_objects_for_version(KEY, &version)
        .e_tag("\"ABCORZ\"")
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(missing).build()?)
        .send()
        .await?;
    assert!(response.errors().is_empty());

    Ok(())
}

/// Verifies DeleteObjects reports Last-Modified precondition failures for exact versions.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_version_if_match_last_modified_time() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_version_if_match_last_modified_time
    let (_server, client, bucket, version, state) = seeded_versioned_object().await?;
    put_versioned_object(&client, &bucket, KEY).await?;

    let failed = delete_objects_for_version(KEY, &version)
        .last_modified_time(state.before)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = delete_objects_for_version(KEY, &version)
        .last_modified_time(state.last_modified)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(KEY));

    let missing = delete_objects_for_version(KEY, &version)
        .last_modified_time(state.before)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(missing).build()?)
        .send()
        .await?;
    assert!(response.errors().is_empty());

    Ok(())
}

/// Verifies DeleteObjects reports size precondition failures for exact versions.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_version_if_match_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_version_if_match_size
    let (_server, client, bucket, version, _state) = seeded_versioned_object_with_body(b"").await?;

    let failed = delete_objects_for_version(KEY, &version)
        .size(9999)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(failed).build()?)
        .send()
        .await?;
    assert_eq!(response.errors()[0].code(), Some("PreconditionFailed"));

    let matched = delete_objects_for_version(KEY, &version).size(0).build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(matched).build()?)
        .send()
        .await?;
    assert_eq!(response.deleted()[0].key(), Some(KEY));

    let missing = delete_objects_for_version(KEY, &version)
        .size(9999)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(Delete::builder().objects(missing).build()?)
        .send()
        .await?;
    assert!(response.errors().is_empty());

    Ok(())
}

#[derive(Debug)]
struct ObjectState {
    etag: String,
    last_modified: DateTime,
    before: DateTime,
    after: DateTime,
}

async fn seeded_object() -> Result<(TestServer, Client, String, ObjectState)> {
    seeded_object_with_body(b"bar").await
}

async fn seeded_object_with_body(
    body: &'static [u8],
) -> Result<(TestServer, Client, String, ObjectState)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, KEY, body).await?;
    let state = head_state(&client, &bucket, KEY).await?;
    Ok((server, client, bucket, state))
}

async fn seeded_versioned_object() -> Result<(TestServer, Client, String, String, ObjectState)> {
    seeded_versioned_object_with_body(b"bar").await
}

async fn seeded_versioned_object_with_body(
    body: &'static [u8],
) -> Result<(TestServer, Client, String, String, ObjectState)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let version_id = client
        .put_object()
        .bucket(&bucket)
        .key(KEY)
        .body(ByteStream::from_static(body))
        .send()
        .await?
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("versioned PutObject missing version ID"))?
        .to_string();
    let state = head_version_state(&client, &bucket, KEY, &version_id).await?;
    Ok((server, client, bucket, version_id, state))
}

async fn put_versioned_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<(String, String)> {
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;
    let etag = response
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();
    let version_id = response
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("versioned PutObject missing version ID"))?
        .to_string();
    Ok((version_id, etag))
}

async fn successful_conditional_multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let (upload_id, parts) = prepare_multipart_upload(client, bucket, key, body).await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let mut request = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload);
    if let Some(if_match) = if_match {
        request = request.if_match(if_match);
    }
    if let Some(if_none_match) = if_none_match {
        request = request.if_none_match(if_none_match);
    }
    Ok(request.send().await?)
}

async fn failing_conditional_multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    expected: &str,
) -> Result<()> {
    let error = match successful_conditional_multipart_upload(
        client,
        bucket,
        key,
        body,
        if_match,
        if_none_match,
    )
    .await
    {
        Ok(_) => bail!("conditional multipart completion should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, expected);
    Ok(())
}

async fn prepare_multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &[u8],
) -> Result<(String, Vec<CompletedPart>)> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string();
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await?;
    let part = CompletedPart::builder()
        .e_tag(
            response
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(1)
        .build();
    Ok((upload_id, vec![part]))
}

async fn head_state(client: &Client, bucket: &str, key: &str) -> Result<ObjectState> {
    let head = client.head_object().bucket(bucket).key(key).send().await?;
    let etag = head
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let last_modified = *head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;
    let before = DateTime::from_secs(last_modified.secs() - 1);
    let after = DateTime::from_secs(last_modified.secs() + 1);
    Ok(ObjectState {
        etag,
        last_modified,
        before,
        after,
    })
}

async fn head_version_state(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<ObjectState> {
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await?;
    let etag = head
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let last_modified = *head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;
    let before = DateTime::from_secs(last_modified.secs() - 1);
    let after = DateTime::from_secs(last_modified.secs() + 1);
    Ok(ObjectState {
        etag,
        last_modified,
        before,
        after,
    })
}

fn delete_objects_for_version(
    key: &str,
    version_id: &str,
) -> aws_sdk_s3::types::builders::ObjectIdentifierBuilder {
    ObjectIdentifier::builder().key(key).version_id(version_id)
}

async fn assert_deleted(client: &Client, bucket: &str, key: &str) -> Result<()> {
    let error = match client.get_object().bucket(bucket).key(key).send().await {
        Ok(_) => bail!("deleted object should be gone"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    Ok(())
}
