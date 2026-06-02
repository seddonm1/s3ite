//! Real-binary HTTP malformed-header conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_headers`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use std::time::Duration;

use anyhow::{Context, Result};
use aws_sdk_s3::{Client, primitives::ByteStream};
use reqwest::{Method, Response, StatusCode};
use s3s::crypto::{Checksum, Sha1};
use serial_test::serial;
use support::{
    assertions::assert_status,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    raw_http, sdk,
    server::TestServer,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const BODY: &[u8] = b"bar";

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_md5_invalid_short() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_md5_invalid_short
    let response = put_object_with_header("Content-MD5", "YWJyYWNhZGFicmE=").await?;
    assert_error(response, StatusCode::BAD_REQUEST, "InvalidDigest").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_md5_bad() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_md5_bad
    let response = put_object_with_header("Content-MD5", "rL0Y20xC+Fzt72VPzMSk2A==").await?;
    assert_error(response, StatusCode::BAD_REQUEST, "BadDigest").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_md5_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_md5_empty
    let response = put_object_with_header("Content-MD5", "").await?;
    assert_error(response, StatusCode::BAD_REQUEST, "InvalidDigest").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_md5_none() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_md5_none
    let (server, client, bucket) = bucket("s3ite-header-object").await?;
    let key = "foo";

    let response = raw_http::signed_request(
        &server,
        Method::PUT,
        &format!("/{bucket}/{key}"),
        Vec::new(),
    )
    .await?;
    assert_status(&response, StatusCode::OK);

    put_object(&client, &bucket, key, BODY).await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, BODY);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_expect_mismatch() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_expect_mismatch
    assert_successful_put_with_header("Expect", "200").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_expect_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_expect_empty
    assert_successful_put_with_header("Expect", "").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_expect_none() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_expect_none
    let (server, client, bucket) = bucket("s3ite-header-object").await?;
    let key = "foo";

    let response = raw_http::signed_request(
        &server,
        Method::PUT,
        &format!("/{bucket}/{key}"),
        Vec::new(),
    )
    .await?;
    assert_status(&response, StatusCode::OK);

    put_object(&client, &bucket, key, BODY).await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, BODY);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_contentlength_negative() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_contentlength_negative
    let (server, _client, bucket) = bucket("s3ite-header-object").await?;
    let status = raw_http_status(
        &server,
        &format!("/{bucket}/foo"),
        &[("Content-Length", "-1")],
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_contenttype_invalid() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_contenttype_invalid
    assert_successful_put_with_header("Content-Type", "text/plain").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_contenttype_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_contenttype_empty
    let (_server, client, bucket) = bucket("s3ite-header-object").await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .content_type("")
        .body(ByteStream::from_static(BODY))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, BODY);
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_contenttype_none() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_contenttype_none
    let (_server, client, bucket) = bucket("s3ite-header-object").await?;
    put_object(&client, &bucket, "foo", BODY).await?;
    assert_eq!(get_object_bytes(&client, &bucket, "foo").await?, BODY);
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_put_bad_canned_acl() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_put_bad_canned_acl
    let (server, _client, bucket) = bucket("s3ite-header-bucket").await?;
    let response = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &format!("/{bucket}?acl"),
        Vec::new(),
        &[("x-amz-acl", "public-ready")],
    )
    .await?;
    assert_status(&response, StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_expect_mismatch() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_expect_mismatch
    assert_successful_create_bucket_with_header("Expect", "200").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_expect_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_expect_empty
    assert_successful_create_bucket_with_header("Expect", "").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_contentlength_negative() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_contentlength_negative
    let server = TestServer::spawn().await?;
    let bucket = bucket_name("s3ite-header-bucket");
    let status =
        raw_http_status(&server, &format!("/{bucket}"), &[("Content-Length", "-1")]).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_md5_invalid_garbage_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_md5_invalid_garbage_aws2
    let (server, _client, bucket) = bucket("s3ite-header-object").await?;
    let response = sig_v2_request(
        &server,
        Method::PUT,
        &format!("/{bucket}/foo"),
        BODY,
        &[("Content-MD5", "AWS HAHAHA")],
    )
    .await?;
    assert_error(response, StatusCode::BAD_REQUEST, "InvalidDigest").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_ua_empty_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_ua_empty_aws2
    assert_successful_sig_v2_put_with_headers(&[("User-Agent", "")]).await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_ua_none_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_ua_none_aws2
    assert_successful_sig_v2_put_with_headers(&[]).await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_date_invalid_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_date_invalid_aws2
    assert_sig_v2_object_date_error("Bad Date", StatusCode::FORBIDDEN, "AccessDenied").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_date_empty_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_date_empty_aws2
    assert_sig_v2_object_date_error("", StatusCode::FORBIDDEN, "AccessDenied").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_date_before_today_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_date_before_today_aws2
    assert_sig_v2_object_date_error(
        "Tue, 07 Jul 2010 21:53:04 GMT",
        StatusCode::FORBIDDEN,
        "RequestTimeTooSkewed",
    )
    .await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_date_before_epoch_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_date_before_epoch_aws2
    assert_sig_v2_object_date_error(
        "Tue, 07 Jul 1950 21:53:04 GMT",
        StatusCode::FORBIDDEN,
        "AccessDenied",
    )
    .await
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_create_bad_date_after_end_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_object_create_bad_date_after_end_aws2
    assert_sig_v2_object_date_error(
        "Tue, 07 Jul 9999 21:53:04 GMT",
        StatusCode::FORBIDDEN,
        "RequestTimeTooSkewed",
    )
    .await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_date_invalid_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_date_invalid_aws2
    assert_sig_v2_bucket_date_error("Bad Date", StatusCode::FORBIDDEN, "AccessDenied").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_date_empty_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_date_empty_aws2
    assert_sig_v2_bucket_date_error("", StatusCode::FORBIDDEN, "AccessDenied").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_date_before_today_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_date_before_today_aws2
    assert_sig_v2_bucket_date_error(
        "Tue, 07 Jul 2010 21:53:04 GMT",
        StatusCode::FORBIDDEN,
        "RequestTimeTooSkewed",
    )
    .await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_date_after_today_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_date_after_today_aws2
    assert_sig_v2_bucket_date_error(
        "Tue, 07 Jul 2030 21:53:04 GMT",
        StatusCode::FORBIDDEN,
        "RequestTimeTooSkewed",
    )
    .await
}

#[tokio::test]
#[serial(conformance)]
async fn test_bucket_create_bad_date_before_epoch_aws2() -> Result<()> {
    // Mirrors s3tests/functional/test_headers.py::test_bucket_create_bad_date_before_epoch_aws2
    assert_sig_v2_bucket_date_error(
        "Tue, 07 Jul 1950 21:53:04 GMT",
        StatusCode::FORBIDDEN,
        "AccessDenied",
    )
    .await
}

async fn bucket(prefix: &str) -> Result<(TestServer, Client, String)> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name(prefix);
    create_bucket(&client, &bucket).await?;
    Ok((server, client, bucket))
}

async fn put_object_with_header(name: &str, value: &str) -> Result<Response> {
    let (server, _client, bucket) = bucket("s3ite-header-object").await?;
    raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &format!("/{bucket}/foo"),
        BODY.to_vec(),
        &[(name, value)],
    )
    .await
}

async fn assert_successful_put_with_header(name: &str, value: &str) -> Result<()> {
    let (server, client, bucket) = bucket("s3ite-header-object").await?;
    let key = "foo";
    let response = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &format!("/{bucket}/{key}"),
        Vec::new(),
        &[(name, value)],
    )
    .await?;
    assert_status(&response, StatusCode::OK);

    put_object(&client, &bucket, key, BODY).await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, BODY);
    Ok(())
}

async fn assert_successful_create_bucket_with_header(name: &str, value: &str) -> Result<()> {
    let server = TestServer::spawn().await?;
    let bucket = bucket_name("s3ite-header-bucket");
    let response = raw_http::signed_request_with_headers(
        &server,
        Method::PUT,
        &format!("/{bucket}"),
        Vec::new(),
        &[(name, value)],
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    Ok(())
}

async fn assert_successful_sig_v2_put_with_headers(headers: &[(&str, &str)]) -> Result<()> {
    let (server, client, bucket) = bucket("s3ite-header-object").await?;
    let key = "foo";
    let response = sig_v2_request(
        &server,
        Method::PUT,
        &format!("/{bucket}/{key}"),
        &[],
        headers,
    )
    .await?;
    assert_status(&response, StatusCode::OK);

    put_object(&client, &bucket, key, BODY).await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, BODY);
    Ok(())
}

async fn assert_sig_v2_object_date_error(
    value: &str,
    status: StatusCode,
    code: &str,
) -> Result<()> {
    let (server, _client, bucket) = bucket("s3ite-header-object").await?;
    let response = sig_v2_request(
        &server,
        Method::PUT,
        &format!("/{bucket}/foo"),
        BODY,
        &[("x-amz-date", value)],
    )
    .await?;
    assert_error(response, status, code).await
}

async fn assert_sig_v2_bucket_date_error(
    value: &str,
    status: StatusCode,
    code: &str,
) -> Result<()> {
    let server = TestServer::spawn().await?;
    let bucket = bucket_name("s3ite-header-bucket");
    let response = sig_v2_request(
        &server,
        Method::PUT,
        &format!("/{bucket}"),
        &[],
        &[("x-amz-date", value)],
    )
    .await?;
    assert_error(response, status, code).await
}

async fn assert_error(response: Response, expected: StatusCode, code: &str) -> Result<()> {
    assert_status(&response, expected);
    let body = response.text().await?;
    assert!(
        body.contains(code),
        "expected error code {code:?}, got {body}"
    );
    Ok(())
}

async fn sig_v2_request(
    server: &TestServer,
    method: Method,
    path: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Result<Response> {
    let url = format!("{}{}", server.endpoint(), path);
    let mut headers = headers
        .iter()
        .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
        .collect::<Vec<_>>();
    if header_value(&headers, "x-amz-date").is_none() {
        headers.push((
            "x-amz-date".to_string(),
            time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc2822)
                .context("format SigV2 date")?,
        ));
    }
    let signature = sig_v2_signature(
        server.secret_key(),
        &sig_v2_string_to_sign(&method, &headers, path),
    );
    let authorization = format!("AWS {}:{signature}", server.access_key());

    let client = raw_http::client()?;
    let mut request = client
        .request(method, &url)
        .header("Authorization", authorization)
        .body(body.to_vec());
    for (name, value) in headers {
        request = request.header(name, value);
    }
    Ok(request.send().await?)
}

fn sig_v2_string_to_sign(method: &Method, headers: &[(String, String)], resource: &str) -> String {
    let mut output = String::new();
    output.push_str(method.as_str());
    output.push('\n');
    output.push_str(header_value(headers, "content-md5").unwrap_or_default());
    output.push('\n');
    output.push_str(header_value(headers, "content-type").unwrap_or_default());
    output.push('\n');
    if header_value(headers, "x-amz-date").is_some() {
        output.push('\n');
    } else {
        output.push_str(header_value(headers, "date").unwrap_or_default());
        output.push('\n');
    }
    output.push_str(&canonical_amz_headers(headers));
    output.push_str(resource);
    output
}

fn canonical_amz_headers(headers: &[(String, String)]) -> String {
    let mut values = headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.to_ascii_lowercase();
            name.starts_with("x-amz-")
                .then(|| (name, value.trim().to_string()))
        })
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut output = String::new();
    let mut index = 0;
    while index < values.len() {
        let name = &values[index].0;
        output.push_str(name);
        output.push(':');
        output.push_str(&values[index].1);
        index += 1;
        while index < values.len() && values[index].0 == *name {
            output.push(',');
            output.push_str(&values[index].1);
            index += 1;
        }
        output.push('\n');
    }
    output
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn sig_v2_signature(secret_key: &str, string_to_sign: &str) -> String {
    base64_encode(&hmac_sha1(secret_key.as_bytes(), string_to_sign.as_bytes()))
}

fn hmac_sha1(key: &[u8], data: &[u8]) -> [u8; 20] {
    let mut key_block = [0_u8; 64];
    if key.len() > key_block.len() {
        key_block[..20].copy_from_slice(&Sha1::checksum(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_key = [0x36_u8; 64];
    let mut outer_key = [0x5c_u8; 64];
    for index in 0..key_block.len() {
        inner_key[index] ^= key_block[index];
        outer_key[index] ^= key_block[index];
    }

    let mut inner = Sha1::new();
    inner.update(&inner_key);
    inner.update(data);
    let inner = inner.finalize();

    let mut outer = Sha1::new();
    outer.update(&outer_key);
    outer.update(&inner);
    outer.finalize()
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(char::from(TABLE[usize::from(first >> 2)]));
        output.push(char::from(
            TABLE[usize::from(((first & 0b0000_0011) << 4) | (second >> 4))],
        ));
        if chunk.len() > 1 {
            output.push(char::from(
                TABLE[usize::from(((second & 0b0000_1111) << 2) | (third >> 6))],
            ));
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(char::from(TABLE[usize::from(third & 0b0011_1111)]));
        } else {
            output.push('=');
        }
    }
    output
}

async fn raw_http_status(
    server: &TestServer,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<StatusCode> {
    let address = server
        .endpoint()
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("unexpected endpoint {}", server.endpoint()))?;
    let mut stream = TcpStream::connect(address).await?;
    let mut request = format!("PUT {path} HTTP/1.1\r\nHost: {address}\r\n");
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.shutdown().await?;

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response)).await??;
    let response = String::from_utf8_lossy(&response);
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow::anyhow!("missing HTTP status line: {response}"))?;
    let status = status.parse::<u16>()?;
    StatusCode::from_u16(status).map_err(Into::into)
}
