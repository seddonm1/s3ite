//! Real-binary HTTP conditional request conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_conditionals`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::{ByteStream, DateTime},
    types::{Delete, ObjectIdentifier},
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
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

async fn assert_deleted(client: &Client, bucket: &str, key: &str) -> Result<()> {
    let error = match client.get_object().bucket(bucket).key(key).send().await {
        Ok(_) => bail!("deleted object should be gone"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    Ok(())
}
