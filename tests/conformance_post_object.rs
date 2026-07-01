//! Real-binary HTTP POST Object conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_post_object`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.
//!
//! Tests whose names match `test_post_object_*` are conversions from
//! `s3tests/functional/test_s3.py`, with per-test comments marking exact
//! mirrors or adapted SigV4 form-flow ports. Extra s3ite coverage uses an
//! `s3ite_` prefix so it does not masquerade as a 1:1 upstream conversion.
//!
//! Not directly converted: `test_post_object_anonymous_request` depends on
//! anonymous/public ACL behavior that s3ite intentionally does not support, and
//! `test_post_object_set_key_from_filename` depends on `${filename}` expansion
//! that `s3s 0.13.0` does not expose to s3ite's POST adapter.

mod support;

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use aws_sdk_s3::types::{
    ChecksumMode, ObjectLockLegalHoldStatus, ObjectLockMode, ServerSideEncryption, Tag,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Response, StatusCode, Url, header};
use serde_json::{Value, json};
use serial_test::serial;
use sha2::{Digest, Sha256};
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes},
    sdk,
    server::TestServer,
    sse::{spawn_sse_server, sse_s3_bucket_configuration},
};

type HmacSha256 = Hmac<Sha256>;

const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const POST_EXPIRATION: &str = "2030-01-01T00:00:00.000Z";

#[derive(Clone, Copy)]
struct ExtraField<'a> {
    name: &'a str,
    value: &'a str,
}

/// Current SigV4 POST signing dates shared by policy fields and signatures.
struct PostSigningContext {
    date: String,
    amz_date: String,
}

struct PostFormOptions<'a> {
    key: Option<&'a str>,
    file_body: Option<&'a [u8]>,
    signing: PostSigningContext,
    extra_fields: Vec<ExtraField<'a>>,
    content_length_range: Option<(u64, u64)>,
    omit_policy_condition_for: Option<&'a str>,
    extra_policy_conditions: Vec<Value>,
    signature_override: Option<&'a str>,
    access_key_override: Option<&'a str>,
    custom_policy: Option<Value>,
    omit_signature: bool,
}

impl<'a> PostFormOptions<'a> {
    /// Builds a signed POST form fixture using current UTC SigV4 date fields.
    fn new(key: &'a str, file_body: &'a [u8]) -> Self {
        Self {
            key: Some(key),
            file_body: Some(file_body),
            signing: PostSigningContext::now(),
            extra_fields: Vec::new(),
            content_length_range: None,
            omit_policy_condition_for: None,
            extra_policy_conditions: Vec::new(),
            signature_override: None,
            access_key_override: None,
            custom_policy: None,
            omit_signature: false,
        }
    }

    fn extra_fields(mut self, fields: Vec<ExtraField<'a>>) -> Self {
        self.extra_fields = fields;
        self
    }

    fn content_length_range(mut self, min: u64, max: u64) -> Self {
        self.content_length_range = Some((min, max));
        self
    }

    fn omit_policy_condition_for(mut self, name: &'a str) -> Self {
        self.omit_policy_condition_for = Some(name);
        self
    }

    fn extra_policy_condition(mut self, condition: Value) -> Self {
        self.extra_policy_conditions.push(condition);
        self
    }

    fn signature_override(mut self, signature: &'a str) -> Self {
        self.signature_override = Some(signature);
        self
    }

    fn access_key_override(mut self, access_key: &'a str) -> Self {
        self.access_key_override = Some(access_key);
        self
    }

    fn custom_policy(mut self, policy: Value) -> Self {
        self.custom_policy = Some(policy);
        self
    }

    fn omit_signature(mut self) -> Self {
        self.omit_signature = true;
        self
    }

    fn without_key(mut self) -> Self {
        self.key = None;
        self
    }

    fn without_file(mut self) -> Self {
        self.file_body = None;
        self
    }
}

impl PostSigningContext {
    /// Builds current UTC SigV4 POST date fields accepted by `s3s` clock-skew validation.
    fn now() -> Self {
        let now = time::OffsetDateTime::now_utc();
        let date = format!(
            "{:04}{:02}{:02}",
            now.year(),
            u8::from(now.month()),
            now.day()
        );
        let amz_date = format!(
            "{date}T{:02}{:02}{:02}Z",
            now.hour(),
            now.minute(),
            now.second()
        );
        Self { date, amz_date }
    }
}

fn field<'a>(name: &'a str, value: &'a str) -> ExtraField<'a> {
    ExtraField { name, value }
}

fn tag_map(tags: &[Tag]) -> BTreeMap<String, String> {
    tags.iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect()
}

/// Builds the SigV4 credential scope used by POST policy fields.
fn sigv4_credential(access_key: &str, signing: &PostSigningContext) -> String {
    let date = &signing.date;
    format!("{access_key}/{date}/{REGION}/{SERVICE}/aws4_request")
}

/// Builds policy conditions that require the generated SigV4 authentication fields.
fn sigv4_auth_conditions(credential: &str, signing: &PostSigningContext) -> [Value; 3] {
    [
        json!(["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]),
        json!(["eq", "$x-amz-credential", credential]),
        json!(["eq", "$x-amz-date", signing.amz_date]),
    ]
}

fn post_policy(conditions: Vec<Value>) -> Value {
    json!({
        "expiration": POST_EXPIRATION,
        "conditions": conditions,
    })
}

fn post_policy_with_expiration(expiration: &str, conditions: Vec<Value>) -> Value {
    json!({
        "expiration": expiration,
        "conditions": conditions,
    })
}

/// Builds the common POST policy conditions for bucket, auth, and optional key matching.
fn base_policy_conditions(
    bucket: &str,
    credential: &str,
    signing: &PostSigningContext,
    key_condition: Option<Value>,
) -> Vec<Value> {
    let mut conditions = vec![json!({ "bucket": bucket })];
    conditions.extend(sigv4_auth_conditions(credential, signing));
    if let Some(key_condition) = key_condition {
        conditions.push(key_condition);
    }
    conditions
}

async fn send_signed_post(
    server: &TestServer,
    bucket: &str,
    options: PostFormOptions<'_>,
) -> Result<Response> {
    let (boundary, body) = build_signed_post_body(server, bucket, options)?;
    send_post_body(server, bucket, &boundary, body).await
}

async fn send_post_body(
    server: &TestServer,
    bucket: &str,
    boundary: &str,
    body: Vec<u8>,
) -> Result<Response> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build raw HTTP client")?;
    Ok(client
        .post(format!("{}/{bucket}", server.endpoint()))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await?)
}

/// Builds a multipart form body and policy signature for a POST Object request.
fn build_signed_post_body(
    server: &TestServer,
    bucket: &str,
    options: PostFormOptions<'_>,
) -> Result<(String, Vec<u8>)> {
    let access_key = options.access_key_override.unwrap_or(server.access_key());
    let credential = sigv4_credential(access_key, &options.signing);
    let mut fields = vec![
        ("bucket".to_string(), bucket.to_string()),
        (
            "x-amz-algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        ("x-amz-credential".to_string(), credential.clone()),
        ("x-amz-date".to_string(), options.signing.amz_date.clone()),
    ];

    if let Some(key) = options.key {
        fields.push(("key".to_string(), key.to_string()));
    }
    for extra in &options.extra_fields {
        fields.push((extra.name.to_ascii_lowercase(), extra.value.to_string()));
    }

    let policy = options
        .custom_policy
        .clone()
        .map_or_else(|| build_policy(bucket, &credential, &fields, &options), Ok)?;
    let policy_json = serde_json::to_string(&policy)?;
    let policy_base64 = BASE64_STANDARD.encode(policy_json.as_bytes());
    let signature = sign_post_policy(server.secret_key(), &policy_base64, &options.signing)?;
    fields.push(("policy".to_string(), policy_base64));
    if !options.omit_signature {
        fields.push((
            "x-amz-signature".to_string(),
            options.signature_override.unwrap_or(&signature).to_string(),
        ));
    }

    let boundary = "s3ite-post-object-boundary".to_string();
    let mut body = Vec::new();
    for (name, value) in &fields {
        append_form_field(&mut body, &boundary, name, value);
    }
    if let Some(file_body) = options.file_body {
        append_file_field(&mut body, &boundary, file_body);
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    Ok((boundary, body))
}

/// Builds the default POST policy from submitted form fields and requested test options.
fn build_policy(
    bucket: &str,
    credential: &str,
    fields: &[(String, String)],
    options: &PostFormOptions<'_>,
) -> Result<Value> {
    let omitted = options
        .omit_policy_condition_for
        .map(str::to_ascii_lowercase);
    let mut conditions = vec![
        json!({ "bucket": bucket }),
        json!(["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]),
        json!(["eq", "$x-amz-credential", credential]),
        json!(["eq", "$x-amz-date", options.signing.amz_date]),
    ];

    for (name, value) in fields {
        if matches!(
            name.as_str(),
            "bucket" | "x-amz-algorithm" | "x-amz-credential" | "x-amz-date"
        ) || omitted.as_deref() == Some(name.as_str())
        {
            continue;
        }
        conditions.push(json!(["eq", format!("${name}"), value]));
    }

    if let Some((min, max)) = options.content_length_range {
        conditions.push(json!(["content-length-range", min, max]));
    }
    conditions.extend(options.extra_policy_conditions.iter().cloned());

    Ok(json!({
        "expiration": POST_EXPIRATION,
        "conditions": conditions,
    }))
}

/// Signs a base64-encoded POST policy with the SigV4 key derived from the test date.
fn sign_post_policy(
    secret_key: &str,
    policy_base64: &str,
    signing: &PostSigningContext,
) -> Result<String> {
    let date_key = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        signing.date.as_bytes(),
    )?;
    let region_key = hmac_sha256(&date_key, REGION.as_bytes())?;
    let service_key = hmac_sha256(&region_key, SERVICE.as_bytes())?;
    let signing_key = hmac_sha256(&service_key, b"aws4_request")?;
    let signature = hmac_sha256(&signing_key, policy_base64.as_bytes())?;
    Ok(hex_lower(&signature))
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).context("create HMAC-SHA256")?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn sha256_base64(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(Sha256::digest(bytes))
}

fn append_form_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(b"\r\n");
}

fn append_file_field(body: &mut Vec<u8>, boundary: &str, file_body: &[u8]) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"upload.bin\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    body.extend_from_slice(file_body);
    body.extend_from_slice(b"\r\n");
}

async fn assert_object_absent(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<()> {
    let error = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("object should not exist");
    assert_error_contains(&error, "NoSuchKey");
    Ok(())
}

async fn assert_bucket_empty(client: &aws_sdk_s3::Client, bucket: &str) -> Result<()> {
    let response = client.list_objects_v2().bucket(bucket).send().await?;
    assert!(response.contents().is_empty());
    Ok(())
}

async fn assert_post_error(
    response: Response,
    expected_status: StatusCode,
    expected_code: &str,
) -> Result<()> {
    assert_eq!(response.status(), expected_status);
    let body = response.text().await?;
    assert_eq!(xml_error_code(&body), Some(expected_code), "got {body}");
    Ok(())
}

/// Extracts the first S3 XML error code from a POST response body.
fn xml_error_code(body: &str) -> Option<&str> {
    body.split("<Code>")
        .nth(1)
        .and_then(|value| value.split("</Code>").next())
}

/// Verifies signed browser-form POST uploads create ordinary readable objects.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_authenticated_request() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_authenticated_request: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post");
    let key = "uploads/basic.txt";
    let body = b"posted object body";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(&server, &bucket, PostFormOptions::new(key, body)).await?;
    assert_status(&response, StatusCode::NO_CONTENT);

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);
    Ok(())
}

/// Verifies authenticated POST uploads do not require a Content-Type form field.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_authenticated_no_content_type() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_authenticated_no_content_type: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-no-ct");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(&server, &bucket, PostFormOptions::new(key, b"bar")).await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"bar");

    Ok(())
}

/// Verifies POST form auth rejects an unknown access key.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_authenticated_request_bad_access_key() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_authenticated_request_bad_access_key: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-bad-access");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![field("content-type", "text/plain")])
            .access_key_override("foo"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST uploads larger than the parser's initial form chunk are stored.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_upload_larger_than_chunk() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_upload_larger_than_chunk: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-large");
    let key = "foo.txt";
    let body = b"foo".repeat(1024 * 1024);

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, &body)
            .extra_fields(vec![field("content-type", "text/plain")])
            .content_length_range(0, 5 * 1024 * 1024),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);

    Ok(())
}

/// Verifies POST uploads across the historical RGW chunk boundary are stored.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_upload_size_rgw_chunk_size_bug() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_upload_size_rgw_chunk_size_bug: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-rgw-chunk");
    let key = "foo.txt";
    let rgw_max_chunk_size = 4 * 1024 * 1024;
    let min_size = rgw_max_chunk_size;
    let max_size = rgw_max_chunk_size * 3;
    let body = b"x".repeat(rgw_max_chunk_size + 200);

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, &body)
            .extra_fields(vec![field("content-type", "text/plain")])
            .content_length_range(min_size as u64, max_size as u64),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);

    Ok(())
}

/// Verifies POST can store user metadata from form fields.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_user_specified_header() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_user_specified_header: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-meta");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar").extra_fields(vec![
            field("content-type", "text/plain"),
            field("x-amz-meta-foo", "barclamp"),
        ]),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);

    let object = client.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        object.metadata().and_then(|metadata| metadata.get("foo")),
        Some(&"barclamp".to_string())
    );

    Ok(())
}

/// Verifies authenticated POST uploads can persist object tags.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_tags_authenticated_request() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_tags_authenticated_request: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-tags");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));
    conditions.push(json!(["starts-with", "$x-amz-tagging", ""]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![
                field("content-type", "text/plain"),
                field("x-amz-tagging", "0=0&1=1"),
            ])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"bar");

    let tags = client
        .get_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(
        tag_map(tags.tag_set()),
        BTreeMap::from([
            ("0".to_string(), "0".to_string()),
            ("1".to_string(), "1".to_string())
        ])
    );

    Ok(())
}

/// Verifies POST uploads validate SHA256 checksum form fields.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_upload_checksum() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_upload_checksum: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-checksum");
    let key = "foo_cksum_test.txt";
    let body = b"x".repeat(2 * 1024 * 1024);
    let checksum = sha256_base64(&body);

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, &body)
            .extra_fields(vec![
                field("content-type", "text/plain"),
                field("x-amz-checksum-sha256", &checksum),
            ])
            .content_length_range(0, 5 * 1024 * 1024),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);

    let object = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(object.checksum_sha256(), Some(checksum.as_str()));
    assert_eq!(
        object.body.collect().await?.into_bytes().as_ref(),
        body.as_slice()
    );

    let bad_body = b"y".repeat(2 * 1024 * 1024);
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, &bad_body)
            .extra_fields(vec![
                field("content-type", "text/plain"),
                field("x-amz-checksum-sha256", "sailorjerry"),
            ])
            .content_length_range(0, 5 * 1024 * 1024),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "BadDigest").await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, body);

    Ok(())
}

/// Verifies bucket-default SSE-S3 applies to authenticated POST uploads.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_s3_default_post_object_authenticated_request() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_sse_s3_default_post_object_authenticated_request: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = spawn_sse_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-default-sse");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["starts-with", "$x-amz-server-side-encryption", ""]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    client
        .put_bucket_encryption()
        .bucket(&bucket)
        .server_side_encryption_configuration(sse_s3_bucket_configuration()?)
        .send()
        .await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);

    let object = client.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        object.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );
    assert_eq!(object.body.collect().await?.into_bytes().as_ref(), b"bar");

    Ok(())
}

/// Verifies POST form fields feed s3ite checksum and object-tag persistence.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_post_object_checksum_and_tags() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-fields");
    let key = "uploads/fields.txt";
    let body = b"body with metadata and tags";
    let checksum = sha256_base64(body);

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, body).extra_fields(vec![
            field("x-amz-sdk-checksum-algorithm", "SHA256"),
            field("x-amz-checksum-sha256", &checksum),
            field("x-amz-tagging", "team=red&space=a+b"),
        ]),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);

    let object = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(object.checksum_sha256(), Some(checksum.as_str()));
    assert_eq!(object.body.collect().await?.into_bytes().as_ref(), body);

    let tags = client
        .get_object_tagging()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(
        tag_map(tags.tag_set()),
        BTreeMap::from([
            ("space".to_string(), "a b".to_string()),
            ("team".to_string(), "red".to_string()),
        ])
    );

    Ok(())
}

/// Verifies POST form fields feed s3ite Object Lock metadata persistence.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_post_object_object_lock_headers() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-lock");
    let key = "uploads/locked.txt";

    client
        .create_bucket()
        .bucket(&bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"locked post body").extra_fields(vec![
            field("x-amz-object-lock-mode", "GOVERNANCE"),
            field(
                "x-amz-object-lock-retain-until-date",
                "2030-01-01T00:00:00Z",
            ),
            field("x-amz-object-lock-legal-hold", "ON"),
        ]),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);

    let object = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(object.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(
        object.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    assert!(object.object_lock_retain_until_date().is_some());

    Ok(())
}

/// Verifies `x-ignore-*` POST form fields are allowed without a policy condition.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_ignored_header() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_ignored_header: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-ignore");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![
                field("content-type", "text/plain"),
                field("x-ignore-foo", "bar"),
            ])
            .omit_policy_condition_for("x-ignore-foo"),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"bar");

    Ok(())
}

/// Verifies POST policy field and operator names are treated case-insensitively.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_case_insensitive_condition_fields() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_case_insensitive_condition_fields: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-case-fields");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = vec![
        json!({ "bUcKeT": bucket }),
        json!(["StArTs-WiTh", "$KeY", "foo"]),
        json!(["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"]),
        json!(["content-length-range", 0, 1024]),
    ];
    conditions.extend(sigv4_auth_conditions(&credential, &options.signing));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"bar");

    Ok(())
}

/// Verifies escaped POST policy field values match literal object keys.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_escaped_field_values() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_escaped_field_values: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-escaped");
    let key = r"\$foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar").extra_fields(vec![field("content-type", "text/plain")]),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"bar");

    Ok(())
}

/// Verifies a policy condition for a missing request field fails the upload.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_request_missing_policy_specified_field() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_request_missing_policy_specified_field: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-missing-field");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![field("content-type", "text/plain")])
            .extra_policy_condition(json!(["starts-with", "$x-amz-meta-foo", "bar"])),
    )
    .await?;
    assert_post_error(response, StatusCode::FORBIDDEN, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies a request field value that violates its policy condition is rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_invalid_request_field_value() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_invalid_request_field_value: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-field-value");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![
                field("content-type", "text/plain"),
                field("x-amz-meta-foo", "barclamp"),
            ])
            .extra_policy_condition(json!(["eq", "$x-amz-meta-foo", ""])),
    )
    .await?;
    assert_post_error(response, StatusCode::FORBIDDEN, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST policy `content-length-range` rejects too-large files.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_upload_size_limit_exceeded() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_upload_size_limit_exceeded: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-range-max");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar").content_length_range(0, 0),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "EntityTooLarge").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST policy `content-length-range` rejects too-small files.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_upload_size_below_minimum() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_upload_size_below_minimum: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-range-min");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar").content_length_range(512, 1000),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "EntityTooSmall").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies a tampered SigV4 POST signature is rejected before object creation.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_invalid_signature() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_invalid_signature: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-sig");
    let key = "uploads/tampered.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"tampered")
            .signature_override("0000000000000000000000000000000000000000000000000000000000000000"),
    )
    .await?;
    assert_post_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST form auth rejects an invalid access key.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_invalid_access_key() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_invalid_access_key: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-invalid-access");
    let key = r"\$foo.txt";
    let bad_access_key = server.access_key().chars().rev().collect::<String>();

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![field("content-type", "text/plain")])
            .access_key_override(&bad_access_key),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies invalid POST policy expiration formats are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_invalid_date_format() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_invalid_date_format: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-date-format");
    let key = r"\$foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", r"\$foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy_with_expiration(
                "2026-06-09 00:00:00 +00:00",
                conditions,
            )),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies `success_action_status=200` returns 200 while still writing the object.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_post_object_success_status_200() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-200");
    let key = "uploads/status-200.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"status 200")
            .extra_fields(vec![field("success_action_status", "200")]),
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    assert_eq!(
        get_object_bytes(&client, &bucket, key).await?,
        b"status 200"
    );

    Ok(())
}

/// Verifies `success_action_status=201` returns POST response XML.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_set_success_code() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_set_success_code: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-201");
    let key = "uploads/status-201.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"status 201")
            .extra_fields(vec![field("success_action_status", "201")]),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response.text().await?;
    assert!(body.contains("<PostResponse"));
    assert!(body.contains(&format!("<Bucket>{bucket}</Bucket>")));
    assert!(body.contains(&format!("<Key>{key}</Key>")));
    assert!(body.contains("<ETag>"));
    assert_eq!(
        get_object_bytes(&client, &bucket, key).await?,
        b"status 201"
    );

    Ok(())
}

/// Verifies unsupported success status values fall back to the default 204.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_set_invalid_success_code() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_set_invalid_success_code: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-404");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar").extra_fields(vec![field("success_action_status", "404")]),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.text().await?, "");
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"bar");

    Ok(())
}

/// Verifies policy object fields are case-sensitive.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_condition_is_case_sensitive() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_condition_is_case_sensitive: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-cond-case");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(json!({
                "expiration": POST_EXPIRATION,
                "CONDITIONS": conditions,
            })),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies the POST policy `expiration` field name is case-sensitive.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_expires_is_case_sensitive() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_expires_is_case_sensitive: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-exp-case");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(json!({
                "EXPIRATION": POST_EXPIRATION,
                "conditions": conditions,
            })),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies expired POST policies are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_expired_policy() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_expired_policy: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-expired");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy_with_expiration(
                "2020-01-01T00:00:00.000Z",
                conditions,
            )),
    )
    .await?;
    assert_post_error(response, StatusCode::FORBIDDEN, "AccessDenied").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies a POST policy bucket mismatch is rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_wrong_bucket() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_wrong_bucket: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-right-bucket");
    let bad_bucket = bucket_name("s3ite-post-wrong-bucket");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    create_bucket(&client, &bad_bucket).await?;
    let response = send_signed_post(
        &server,
        &bad_bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_post_error(response, StatusCode::FORBIDDEN, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;
    assert_object_absent(&client, &bad_bucket, key).await?;

    Ok(())
}

/// Verifies `success_action_redirect` returns a 303 Location response.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_success_redirect_action() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_success_redirect_action: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-redirect");
    let key = "uploads/redirect.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"redirect").extra_fields(vec![field(
            "success_action_redirect",
            "https://example.com/done",
        )]),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(header::LOCATION)
        .context("missing Location header")?
        .to_str()?;
    let redirect = Url::parse(location)?;
    let query = redirect.query_pairs().collect::<BTreeMap<_, _>>();
    assert_eq!(
        redirect.as_str().split('?').next(),
        Some("https://example.com/done")
    );
    assert_eq!(
        query.get("bucket").map(std::borrow::Cow::as_ref),
        Some(bucket.as_str())
    );
    assert_eq!(
        query.get("key").map(std::borrow::Cow::as_ref),
        Some("uploads/redirect.txt")
    );
    let etag = query
        .get("etag")
        .context("redirect Location missing etag query parameter")?;
    assert!(!etag.is_empty());
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"redirect");

    Ok(())
}

/// Verifies malformed POST requests fail without creating objects.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_post_object_malformed_form_fails_without_write() -> Result<()> {
    // s3ite-specific raw parser guard for malformed browser POST bodies.
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-malformed");

    create_bucket(&client, &bucket).await?;
    let response = send_post_body(
        &server,
        &bucket,
        "broken-boundary",
        b"not multipart form data".to_vec(),
    )
    .await?;
    if response.status().is_success() {
        bail!("malformed POST form unexpectedly succeeded");
    }
    assert_bucket_empty(&client, &bucket).await?;

    Ok(())
}

/// Verifies a signed POST without a key is rejected without writing an object.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_no_key_specified() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_no_key_specified: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-no-key");

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new("ignored", b"missing key").without_key(),
    )
    .await?;
    if response.status().is_success() {
        bail!("missing-key POST form unexpectedly succeeded");
    }
    assert_bucket_empty(&client, &bucket).await?;

    Ok(())
}

/// Verifies a POST with a policy but no signature is rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_missing_signature() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_missing_signature: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-no-sig");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![field("content-type", "text/plain")])
            .omit_signature(),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "AccessDenied").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies a policy missing the bucket condition is rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_missing_policy_condition() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_missing_policy_condition: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-miss-cond");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = vec![json!(["starts-with", "$key", "foo"])];
    conditions.extend(sigv4_auth_conditions(&credential, &options.signing));
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_post_error(response, StatusCode::FORBIDDEN, "AccessDenied").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST policies must include `expiration`.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_missing_expires_condition() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_missing_expires_condition: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-no-exp");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0, 1024]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(json!({ "conditions": conditions })),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST policies must include a condition list.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_missing_conditions_list() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_missing_conditions_list: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-no-conditions");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(json!({ "expiration": POST_EXPIRATION })),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies POST policies with an empty condition object are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_empty_conditions() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_empty_conditions: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-empty");
    let key = "foo.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"bar")
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(vec![json!({})])),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies malformed content-length-range conditions are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_missing_content_length_argument() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_missing_content_length_argument: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-short-range");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", 0]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies invalid content-length-range values are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_post_object_invalid_content_length_argument() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_post_object_invalid_content_length_argument: uses s3ite SigV4 POST form path instead of Ceph SigV2 form fields
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-invalid-range");
    let key = "foo.txt";
    let options = PostFormOptions::new(key, b"bar");
    let credential = sigv4_credential(server.access_key(), &options.signing);
    let mut conditions = base_policy_conditions(
        &bucket,
        &credential,
        &options.signing,
        Some(json!(["starts-with", "$key", "foo"])),
    );
    conditions.push(json!(["starts-with", "$Content-Type", "text/plain"]));
    conditions.push(json!(["content-length-range", -1, 0]));

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        options
            .extra_fields(vec![field("content-type", "text/plain")])
            .custom_policy(post_policy(conditions)),
    )
    .await?;
    assert_post_error(response, StatusCode::BAD_REQUEST, "InvalidPolicyDocument").await?;
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}

/// Verifies a signed POST without a file part is rejected without writing an object.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_post_object_missing_file_fails_without_write() -> Result<()> {
    // s3ite-specific raw parser guard for a POST form without a file part.
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-post-no-file");
    let key = "uploads/missing-file.txt";

    create_bucket(&client, &bucket).await?;
    let response = send_signed_post(
        &server,
        &bucket,
        PostFormOptions::new(key, b"missing file").without_file(),
    )
    .await?;
    if response.status().is_success() {
        bail!("missing-file POST form unexpectedly succeeded");
    }
    assert_object_absent(&client, &bucket, key).await?;

    Ok(())
}
