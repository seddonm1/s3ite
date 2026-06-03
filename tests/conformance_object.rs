//! Real-binary HTTP object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_object`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::{ByteStream, DateTime, DateTimeFormat},
    types::{Delete, ObjectIdentifier},
};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    raw_http, sdk,
    server::TestServer,
};

/// Verifies HeadObject reports zero content length for an empty object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_head_zero_bytes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_head_zero_bytes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"").await?;

    let response = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    assert_eq!(response.content_length(), Some(0));

    Ok(())
}

/// Verifies partNumber=1 reads a normal object and higher parts fail.
#[tokio::test]
#[serial(conformance)]
async fn test_non_multipart_get_part() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_non_multipart_get_part
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("singlepart")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    let etag = put
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("singlepart")
        .part_number(2)
        .send()
        .await
    {
        Ok(_) => bail!("partNumber beyond normal object should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidPart");

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("singlepart")
        .part_number(1)
        .send()
        .await?;
    assert_eq!(response.e_tag(), Some(etag));
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        b"body"
    );

    Ok(())
}

/// Verifies PutObject returns the MD5-based ETag for a small object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_check_etag() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_check_etag
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    let response = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    assert_eq!(
        response.e_tag().map(|etag| etag.trim_matches('"')),
        Some("37b51d194a7513e45b56f6524f2d51f2")
    );

    Ok(())
}

/// Verifies PutObject accepts a valid Content-MD5 header.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_valid_content_md5() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("md5")
        .content_md5("N7UdGUp1E+RbVvZSTy1R8g==")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    assert_eq!(get_object_bytes(&client, &bucket, "md5").await?, b"bar");

    Ok(())
}

/// Verifies object writes, overwrites, reads, and deletes through HTTP.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_read_update_read_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_read_update_read_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    put_object(&client, &bucket, "foo", b"soup").await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"soup");

    client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;

    let error = match client.get_object().bucket(&bucket).key("foo").send().await {
        Ok(_) => bail!("deleted object should not be readable"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

/// Verifies signed owner PutObject creates a readable object.
#[tokio::test]
#[serial(conformance)]
async fn test_object_put_authenticated() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_put_authenticated
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    let response = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await?;
    assert!(response.e_tag().is_some());
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"foo");

    Ok(())
}

/// Verifies a failed PutObject does not poison later successful writes.
#[tokio::test]
#[serial(conformance)]
async fn test_100_continue_error_retry() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_100_continue_error_retry
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let missing_bucket = bucket_name("s3ite-missing");
    let bucket = bucket_name("s3ite-obj");

    let error = match client
        .put_object()
        .bucket(&missing_bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
    {
        Ok(_) => bail!("PutObject to a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    Ok(())
}

/// Verifies stored Content-Encoding strips transport-only aws-chunked markers.
#[tokio::test]
#[serial(conformance)]
async fn test_object_content_encoding_aws_chunked() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_content_encoding_aws_chunked
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let key = "encoding";

    create_bucket(&client, &bucket).await?;
    for (content_encoding, expected) in [
        ("gzip", Some("gzip")),
        ("deflate, gzip", Some("deflate, gzip")),
        ("gzip, aws-chunked", Some("gzip")),
        ("aws-chunked, gzip", Some("gzip")),
        ("aws-chunked", None),
        ("aws-chunked, aws-chunked", None),
    ] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .content_encoding(content_encoding)
            .body(ByteStream::from_static(b""))
            .send()
            .await?;
        let response = client.head_object().bucket(&bucket).key(key).send().await?;
        assert_eq!(response.content_encoding(), expected);
    }

    Ok(())
}

/// Verifies a PUT without metadata replaces previous user metadata.
#[tokio::test]
#[serial(conformance)]
async fn test_object_metadata_replaced_on_put() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_metadata_replaced_on_put
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .metadata("meta1", "bar")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;
    put_object(&client, &bucket, "foo", b"bar").await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    assert!(
        response
            .metadata()
            .is_none_or(|metadata| metadata.is_empty())
    );

    Ok(())
}

/// Verifies copying a zero-byte object preserves an empty body.
#[tokio::test]
#[serial(conformance)]
async fn test_object_copy_zero_size() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_copy_zero_size
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo123bar", b"").await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("bar321foo")
        .copy_source(format!("{bucket}/foo123bar"))
        .send()
        .await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("bar321foo")
        .send()
        .await?;
    assert_eq!(response.content_length(), Some(0));
    assert!(response.body.collect().await?.into_bytes().is_empty());

    Ok(())
}

/// Verifies ranged GET returns partial content and Content-Range.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_response_code() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_response_code
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let content = b"testcontent";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", content).await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=4-7")
        .send()
        .await?;

    assert_eq!(response.accept_ranges(), Some("bytes"));
    assert_eq!(response.content_range(), Some("bytes 4-7/11"));
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        &content[4..8]
    );

    Ok(())
}

/// Verifies a large ranged GET returns only the requested byte span.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_big_request_response_code() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_big_request_response_code
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let content = (0..8 * 1024 * 1024)
        .map(|index| b'a' + (index % 26) as u8)
        .collect::<Vec<_>>();

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("testobj")
        .body(ByteStream::from(content.clone()))
        .send()
        .await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=3145728-5242880")
        .send()
        .await?;

    assert_eq!(
        response.content_range(),
        Some("bytes 3145728-5242880/8388608")
    );
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        &content[3_145_728..=5_242_880]
    );

    Ok(())
}

/// Verifies an open-ended range skips leading bytes.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_skip_leading_bytes_response_code() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_skip_leading_bytes_response_code
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let content = b"testcontent";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", content).await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=4-")
        .send()
        .await?;

    assert_eq!(response.content_range(), Some("bytes 4-10/11"));
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        &content[4..]
    );

    Ok(())
}

/// Verifies a suffix range returns trailing bytes.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_return_trailing_bytes_response_code() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_return_trailing_bytes_response_code
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let content = b"testcontent";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", content).await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=-7")
        .send()
        .await?;

    assert_eq!(response.content_range(), Some("bytes 4-10/11"));
    assert_eq!(
        response.body.collect().await?.into_bytes().as_ref(),
        &content[4..]
    );

    Ok(())
}

/// Verifies unsatisfiable ranged GET fails with InvalidRange.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_invalid_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_invalid_range
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", b"testcontent").await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=40-50")
        .send()
        .await
    {
        Ok(_) => bail!("unsatisfiable range should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidRange");

    Ok(())
}

/// Verifies objects whose key ends with a slash are ordinary objects.
#[tokio::test]
#[serial(conformance)]
async fn test_object_trailing_slash_round_trip() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "folder/", b"folder/").await?;
    assert_eq!(
        get_object_bytes(&client, &bucket, "folder/").await?,
        b"folder/"
    );

    client
        .delete_object()
        .bucket(&bucket)
        .key("folder/")
        .send()
        .await?;
    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("folder/")
        .send()
        .await
    {
        Ok(_) => bail!("deleted trailing-slash object should not be readable"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

/// Verifies an unsatisfiable range against an empty object is InvalidRange.
#[tokio::test]
#[serial(conformance)]
async fn test_ranged_request_empty_object() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_ranged_request_empty_object
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "testobj", b"").await?;

    let error = match client
        .get_object()
        .bucket(&bucket)
        .key("testobj")
        .range("bytes=40-50")
        .send()
        .await
    {
        Ok(_) => bail!("unsatisfiable range on an empty object should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidRange");

    Ok(())
}

/// Verifies PutObject reports NoSuchBucket for a missing bucket.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_to_nonexist_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_to_nonexist_bucket
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-missing");

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await
    {
        Ok(_) => bail!("PutObject into a missing bucket should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

/// Verifies PutObject accepts chunked transfer encoding.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_with_chunked_transfer_encoding() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_with_chunked_transfer_encoding
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    let response = raw_http::signed_chunked_request(
        &server,
        Method::PUT,
        &format!("/{bucket}/foo"),
        b"bar".to_vec(),
    )
    .await?;
    assert_status(&response, StatusCode::OK);

    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    Ok(())
}

/// Verifies GetObject reports NoSuchKey for a missing key.
#[tokio::test]
#[serial(conformance)]
async fn test_object_read_not_exist() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_read_not_exist
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;

    let error = match client.get_object().bucket(&bucket).key("bar").send().await {
        Ok(_) => bail!("GetObject for a missing key should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

/// Verifies error XML RequestId matches the response header.
#[tokio::test]
#[serial(conformance)]
async fn test_object_requestid_matches_header_on_error() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_requestid_matches_header_on_error
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    let response =
        raw_http::signed_request(&server, Method::GET, &format!("/{bucket}/bar"), Vec::new())
            .await?;
    assert_status(&response, StatusCode::NOT_FOUND);

    let header_request_id = response
        .headers()
        .get("x-amz-request-id")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body = response.text().await?;
    let body_request_id = xml_tag(&body, "RequestId");
    assert_eq!(body_request_id, header_request_id.as_deref());

    Ok(())
}

/// Verifies multi-object delete removes keys and remains idempotent.
#[tokio::test]
#[serial(conformance)]
async fn test_multi_object_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multi_object_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-delete");
    let keys = ["key0", "key1", "key2"];

    create_bucket(&client, &bucket).await?;
    put_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v1(&client, &bucket).await?, keys.len());

    assert_delete_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v1(&client, &bucket).await?, 0);

    assert_delete_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v1(&client, &bucket).await?, 0);

    Ok(())
}

/// Verifies multi-object delete with ListObjectsV2 remains idempotent.
#[tokio::test]
#[serial(conformance)]
async fn test_multi_objectv2_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multi_objectv2_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-delete");
    let keys = ["key0", "key1", "key2"];

    create_bucket(&client, &bucket).await?;
    put_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v2(&client, &bucket).await?, keys.len());

    assert_delete_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v2(&client, &bucket).await?, 0);

    assert_delete_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v2(&client, &bucket).await?, 0);

    Ok(())
}

/// Verifies DeleteObjects rejects more than one thousand keys.
#[tokio::test]
#[serial(conformance)]
async fn test_multi_object_delete_key_limit() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multi_object_delete_key_limit
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-delete");
    let keys = many_keys();

    create_bucket(&client, &bucket).await?;
    put_string_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v1(&client, &bucket).await?, keys.len());

    let error = match client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete_for_keys(&keys)?)
        .send()
        .await
    {
        Ok(_) => bail!("DeleteObjects with more than one thousand keys should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "MalformedXML");

    Ok(())
}

/// Verifies DeleteObjects over the V2 listing shape rejects too many keys.
#[tokio::test]
#[serial(conformance)]
async fn test_multi_objectv2_delete_key_limit() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multi_objectv2_delete_key_limit
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-delete");
    let keys = many_keys();

    create_bucket(&client, &bucket).await?;
    put_string_objects(&client, &bucket, &keys).await?;
    assert_eq!(list_object_count_v2(&client, &bucket).await?, keys.len());

    let error = match client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete_for_keys(&keys)?)
        .send()
        .await
    {
        Ok(_) => bail!("DeleteObjects with more than one thousand keys should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "MalformedXML");

    Ok(())
}

/// Verifies PutObject stores Cache-Control metadata.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_cache_control() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_cache_control
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let cache_control = "public, max-age=14400";

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .cache_control(cache_control)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    let response = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    assert_eq!(response.cache_control(), Some(cache_control));

    Ok(())
}

/// Verifies PutObject stores Expires metadata.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_expires() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_expires
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");
    let expires = DateTime::from_secs(now_seconds()? + 6000);

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .expires(expires)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    let response = client
        .head_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    let expected_expires = expires.fmt(DateTimeFormat::HttpDate)?;
    assert_eq!(response.expires_string(), Some(expected_expires.as_str()));

    Ok(())
}

/// Verifies object metadata can be set to a non-empty value.
#[tokio::test]
#[serial(conformance)]
async fn test_object_set_get_metadata_none_to_good() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_set_get_metadata_none_to_good
    assert_eq!(set_get_metadata("mymeta").await?, "mymeta");
    Ok(())
}

/// Verifies object metadata can be set to an empty value.
#[tokio::test]
#[serial(conformance)]
async fn test_object_set_get_metadata_none_to_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_set_get_metadata_none_to_empty
    assert_eq!(set_get_metadata("").await?, "");
    Ok(())
}

/// Verifies object metadata can be overwritten with an empty value.
#[tokio::test]
#[serial(conformance)]
async fn test_object_set_get_metadata_overwrite_to_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_set_get_metadata_overwrite_to_empty
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    assert_eq!(
        set_get_metadata_in_bucket(&client, &bucket, "oldmeta").await?,
        "oldmeta"
    );
    assert_eq!(set_get_metadata_in_bucket(&client, &bucket, "").await?, "");

    Ok(())
}

/// Verifies a byte buffer upload is readable as the original body.
#[tokio::test]
#[serial(conformance)]
async fn test_object_write_file() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_write_file
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from(Vec::from(b"bar")))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, b"bar");

    Ok(())
}

/// Verifies GetObject response header overrides are applied.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_response_headers() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_response_headers
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;

    let response = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .response_cache_control("no-cache")
        .response_content_disposition("bla")
        .response_content_encoding("aaa")
        .response_content_language("esperanto")
        .response_content_type("foo/bar")
        .response_expires(DateTime::from_secs(123))
        .send()
        .await?;
    assert_eq!(response.content_type(), Some("foo/bar"));
    assert_eq!(response.content_disposition(), Some("bla"));
    assert_eq!(response.content_language(), Some("esperanto"));
    assert_eq!(response.content_encoding(), Some("aaa"));
    assert_eq!(response.cache_control(), Some("no-cache"));

    Ok(())
}

/// Verifies GetObject reports NoSuchBucket after the bucket is deleted.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_authenticated_bucket_gone() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_authenticated_bucket_gone
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    let error = match client.get_object().bucket(&bucket).key("foo").send().await {
        Ok(_) => bail!("GetObject after bucket deletion should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

/// Verifies GetObject reports NoSuchKey after the object is deleted.
#[tokio::test]
#[serial(conformance)]
async fn test_object_raw_authenticated_object_gone() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_raw_authenticated_object_gone
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;

    let error = match client.get_object().bucket(&bucket).key("foo").send().await {
        Ok(_) => bail!("GetObject after object deletion should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");

    Ok(())
}

/// Verifies DeleteObject reports NoSuchBucket after the bucket is deleted.
#[tokio::test]
#[serial(conformance)]
async fn test_object_delete_key_bucket_gone() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_delete_key_bucket_gone
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"bar").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    client.delete_bucket().bucket(&bucket).send().await?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await
    {
        Ok(_) => bail!("DeleteObject after bucket deletion should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchBucket");

    Ok(())
}

async fn put_objects(client: &Client, bucket: &str, keys: &[&str]) -> Result<()> {
    for key in keys {
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await?;
    }
    Ok(())
}

async fn put_string_objects(client: &Client, bucket: &str, keys: &[String]) -> Result<()> {
    for key in keys {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await?;
    }
    Ok(())
}

async fn assert_delete_objects(client: &Client, bucket: &str, keys: &[&str]) -> Result<()> {
    let response = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete_for_borrowed_keys(keys)?)
        .send()
        .await?;
    assert_eq!(response.deleted().len(), keys.len());
    assert!(response.errors().is_empty());
    Ok(())
}

async fn list_object_count_v1(client: &Client, bucket: &str) -> Result<usize> {
    let mut marker = None;
    let mut count = 0;

    loop {
        let mut request = client.list_objects().bucket(bucket);
        if let Some(value) = marker.as_deref() {
            request = request.marker(value);
        }
        let response = request.send().await?;
        count += response.contents().len();

        if !response.is_truncated().unwrap_or(false) {
            return Ok(count);
        }
        marker = response
            .next_marker()
            .map(ToOwned::to_owned)
            .or_else(|| response.contents().last()?.key().map(ToOwned::to_owned));
    }
}

async fn list_object_count_v2(client: &Client, bucket: &str) -> Result<usize> {
    let mut continuation_token = None;
    let mut count = 0;

    loop {
        let mut request = client.list_objects_v2().bucket(bucket);
        if let Some(value) = continuation_token.as_deref() {
            request = request.continuation_token(value);
        }
        let response = request.send().await?;
        count += response.contents().len();

        if !response.is_truncated().unwrap_or(false) {
            return Ok(count);
        }
        continuation_token = response.next_continuation_token().map(ToOwned::to_owned);
    }
}

fn delete_for_borrowed_keys(keys: &[&str]) -> Result<Delete> {
    let objects = keys
        .iter()
        .map(|key| object_identifier(key))
        .collect::<Result<Vec<_>>>()?;
    Ok(Delete::builder().set_objects(Some(objects)).build()?)
}

fn delete_for_keys(keys: &[String]) -> Result<Delete> {
    let objects = keys
        .iter()
        .map(|key| object_identifier(key))
        .collect::<Result<Vec<_>>>()?;
    Ok(Delete::builder().set_objects(Some(objects)).build()?)
}

fn object_identifier(key: &str) -> Result<ObjectIdentifier> {
    Ok(ObjectIdentifier::builder().key(key).build()?)
}

fn many_keys() -> Vec<String> {
    (0..=1000).map(|index| format!("key-{index}")).collect()
}

async fn set_get_metadata(metadata: &str) -> Result<String> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-obj");

    create_bucket(&client, &bucket).await?;
    set_get_metadata_in_bucket(&client, &bucket, metadata).await
}

async fn set_get_metadata_in_bucket(
    client: &Client,
    bucket: &str,
    metadata: &str,
) -> Result<String> {
    client
        .put_object()
        .bucket(bucket)
        .key("foo")
        .metadata("meta1", metadata)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    let response = client.get_object().bucket(bucket).key("foo").send().await?;
    let metadata = response
        .metadata()
        .and_then(|metadata| metadata.get("meta1"))
        .ok_or_else(|| anyhow::anyhow!("object response missing meta1 metadata"))?;
    Ok(metadata.to_owned())
}

fn now_seconds() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

fn xml_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(&body[start..end])
}
