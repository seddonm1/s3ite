//! Real-binary HTTP conditional request conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_conditionals`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
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

#[tokio::test]
#[serial(conformance)]
async fn test_get_object_etag_conditionals() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmatch_good
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmatch_failed
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifnonematch_good
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifnonematch_failed
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;
    let etag = put
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("put object response missing ETag"))?
        .to_string();

    let matched = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_match(&etag)
        .send()
        .await?;
    assert_eq!(matched.body.collect().await?.into_bytes().as_ref(), b"bar");

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_none_match(&etag)
        .send()
        .await
    {
        Ok(_) => bail!("matching If-None-Match should return NotModified"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");

    let unmatched = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_none_match("\"ABCORZ\"")
        .send()
        .await?;
    assert_eq!(
        unmatched.body.collect().await?.into_bytes().as_ref(),
        b"bar"
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_get_object_date_conditionals() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmodifiedsince_good
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifmodifiedsince_failed
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifunmodifiedsince_good
    // Mirrors s3tests/functional/test_s3.py::test_get_object_ifunmodifiedsince_failed
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    let last_modified = head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;
    let before = DateTime::from_secs(last_modified.secs() - 1);
    let after = DateTime::from_secs(last_modified.secs() + 1);

    let modified = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_modified_since(before)
        .send()
        .await?;
    assert_eq!(modified.body.collect().await?.into_bytes().as_ref(), b"bar");

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_modified_since(after)
        .send()
        .await
    {
        Ok(_) => bail!("unmodified object should return NotModified"),
        Err(error) => error,
    };
    assert_error_contains(&error, "StatusCode(304)");

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_unmodified_since(before)
        .send()
        .await
    {
        Ok(_) => bail!("modified object should fail If-Unmodified-Since"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let unmodified = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .if_unmodified_since(after)
        .send()
        .await?;
    assert_eq!(
        unmodified.body.collect().await?.into_bytes().as_ref(),
        b"bar"
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_put_object_conditionals() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifmatch_good
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifmatch_failed
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifnonmatch_good
    // Mirrors s3tests/functional/test_s3.py::test_put_object_ifnonmatch_failed
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    let etag = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .if_match("\"ABCORZ\"")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("mismatched If-Match should reject overwrite"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .if_match(&etag)
        .body(ByteStream::from_static(b"zar"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"zar");

    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .if_none_match("\"ABCORZ\"")
        .body(ByteStream::from_static(b"zip"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"zip");

    let current_etag = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .if_none_match(&current_etag)
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    {
        Ok(_) => bail!("matching If-None-Match should reject overwrite"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"zip");

    client
        .put_object()
        .bucket(&bucket)
        .key("create-only")
        .if_none_match("*")
        .body(ByteStream::from_static(b"created"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &bucket, "create-only").await?,
        b"created"
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_source_conditionals() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifmatch_good
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifmatch_failed
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifnonematch_good
    // Mirrors s3tests/functional/test_s3.py::test_copy_object_ifnonematch_failed
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");
    let copy_source = format!("{bucket}/foo");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;
    let etag = put
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("put object response missing ETag"))?
        .to_string();

    client
        .copy_object()
        .bucket(&bucket)
        .key("matched")
        .copy_source(&copy_source)
        .copy_source_if_match(&etag)
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "matched").await?, b"bar");

    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("blocked")
        .copy_source(&copy_source)
        .copy_source_if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched copy source If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let error = match client
        .copy_object()
        .bucket(&bucket)
        .key("blocked")
        .copy_source(&copy_source)
        .copy_source_if_none_match(&etag)
        .send()
        .await
    {
        Ok(_) => bail!("matching copy source If-None-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    client
        .copy_object()
        .bucket(&bucket)
        .key("unmatched")
        .copy_source(&copy_source)
        .copy_source_if_none_match("\"ABCORZ\"")
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &bucket, "unmatched").await?,
        b"bar"
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_delete_object_conditionals() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_if_match
    // Mirrors s3tests/functional/test_s3.py::test_delete_object_if_match_size
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"delete-me").await?;
    let head = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    let etag = head
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let last_modified = head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .if_match("\"ABCORZ\"")
        .send()
        .await
    {
        Ok(_) => bail!("mismatched delete If-Match should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(
        get_object_bytes(&client, &bucket, "foo").await?,
        b"delete-me"
    );

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
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
        .key("foo")
        .if_match(etag)
        .if_match_last_modified_time(*last_modified)
        .if_match_size(9)
        .send()
        .await?;

    let error = match client.get_object().bucket(&bucket).key("foo").send().await {
        Ok(_) => bail!("deleted object should be gone"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_delete_objects_conditionals() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_objects_if_match
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cond");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "keep", b"keep").await?;
    put_object(&client, &bucket, "delete", b"delete").await?;

    let delete_etag = client
        .head_object()
        .bucket(&bucket)
        .key("delete")
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();

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

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("delete")
        .send()
        .await
    {
        Ok(_) => bail!("conditionally deleted object should be gone"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}
