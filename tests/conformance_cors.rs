//! Real-binary HTTP bucket CORS conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_cors`, or set `S3ITE_TEST_BIN` to an
//! existing binary path.

mod support;

use std::time::Duration;

use anyhow::Result;
use aws_sdk_s3::{
    Client,
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{CorsConfiguration, CorsRule, ObjectCannedAcl},
};
use reqwest::{Method, Response, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket},
    raw_http, sdk,
    server::{TestServer, TestServerOptions},
};

const OWNER_ID: &str = "s3ite";
const WRONG_OWNER: &str = "not-s3ite";

fn cors_rule(
    origins: &[&str],
    methods: &[&str],
    allowed_headers: &[&str],
    expose_headers: &[&str],
) -> Result<CorsRule> {
    cors_rule_with_options(
        origins,
        methods,
        allowed_headers,
        expose_headers,
        Some("rule"),
        Some(600),
    )
}

fn ceph_cors_rule(origins: &[&str], methods: &[&str]) -> Result<CorsRule> {
    cors_rule_with_options(origins, methods, &[], &[], None, None)
}

fn cors_rule_with_options(
    origins: &[&str],
    methods: &[&str],
    allowed_headers: &[&str],
    expose_headers: &[&str],
    id: Option<&str>,
    max_age_seconds: Option<i32>,
) -> Result<CorsRule> {
    let mut builder = CorsRule::builder();
    if let Some(id) = id {
        builder = builder.id(id);
    }
    if let Some(max_age_seconds) = max_age_seconds {
        builder = builder.max_age_seconds(max_age_seconds);
    }
    for origin in origins {
        builder = builder.allowed_origins(*origin);
    }
    for method in methods {
        builder = builder.allowed_methods(*method);
    }
    for header in allowed_headers {
        builder = builder.allowed_headers(*header);
    }
    for header in expose_headers {
        builder = builder.expose_headers(*header);
    }
    Ok(builder.build()?)
}

fn cors_config(rules: Vec<CorsRule>) -> Result<CorsConfiguration> {
    Ok(CorsConfiguration::builder()
        .set_cors_rules(Some(rules))
        .build()?)
}

fn bucket_path(bucket: &str) -> String {
    format!("/{}", raw_http::path_segment(bucket))
}

fn object_path(bucket: &str, key: &str) -> String {
    format!(
        "/{}/{}",
        raw_http::path_segment(bucket),
        raw_http::path_segment(key)
    )
}

fn header(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn assert_cors_response(
    response: &Response,
    status: StatusCode,
    allow_origin: Option<&str>,
    allow_methods: Option<&str>,
) {
    assert_status(response, status);
    assert_eq!(
        header(response, "access-control-allow-origin").as_deref(),
        allow_origin
    );
    assert_eq!(
        header(response, "access-control-allow-methods").as_deref(),
        allow_methods
    );
}

async fn signed_cors_request(
    server: &TestServer,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<Response> {
    raw_http::signed_request_with_headers(server, method, path, Vec::new(), headers).await
}

async fn unsigned_cors_request(
    server: &TestServer,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<Response> {
    let client = raw_http::client()?;
    let mut request = client.request(method, format!("{}{}", server.endpoint(), path));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    Ok(request.send().await?)
}

async fn options_request(
    server: &TestServer,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<Response> {
    unsigned_cors_request(server, Method::OPTIONS, path, headers).await
}

async fn preflight(
    server: &TestServer,
    path: &str,
    origin: &str,
    method: &str,
    request_headers: Option<&str>,
) -> Result<Response> {
    let mut headers = vec![
        ("Origin", origin),
        ("Access-Control-Request-Method", method),
    ];
    if let Some(request_headers) = request_headers {
        headers.push(("Access-Control-Request-Headers", request_headers));
    }
    options_request(server, path, &headers).await
}

async fn assert_presigned_options_method(
    client: &Client,
    bucket: &str,
    url: &str,
    method: &str,
) -> Result<()> {
    let missing_cors = raw_http::client()?
        .request(Method::OPTIONS, url)
        .send()
        .await?;
    assert_cors_response(&missing_cors, StatusCode::BAD_REQUEST, None, None);

    client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(cors_config(vec![ceph_cors_rule(&["example"], &[method])?])?)
        .send()
        .await?;

    let allowed = raw_http::client()?
        .request(Method::OPTIONS, url)
        .header("Origin", "example")
        .header("Access-Control-Request-Method", method)
        .send()
        .await?;
    assert_cors_response(&allowed, StatusCode::OK, Some("example"), Some(method));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_set_cors() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_set_cors
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-set");

    create_bucket(&client, &bucket).await?;
    let missing = client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("empty bucket CORS config should fail");
    assert_error_contains(&missing, "NoSuchCORSConfiguration");

    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(vec![ceph_cors_rule(
            &["*.get", "*.put"],
            &["GET", "PUT"],
        )?])?)
        .send()
        .await?;

    let output = client.get_bucket_cors().bucket(&bucket).send().await?;
    let rules = output.cors_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_methods(), ["GET", "PUT"]);
    assert_eq!(rules[0].allowed_origins(), ["*.get", "*.put"]);

    client.delete_bucket_cors().bucket(&bucket).send().await?;
    let deleted = client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("deleted bucket CORS config should fail");
    assert_error_contains(&deleted, "NoSuchCORSConfiguration");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_cors_origin_response() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_cors_origin_response: uses s3ite owner-only auth instead of public ACL setup
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-origin");

    create_bucket(&client, &bucket).await?;
    let missing = client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("empty bucket CORS config should fail");
    assert_error_contains(&missing, "NoSuchCORSConfiguration");

    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(vec![
            ceph_cors_rule(&["*suffix"], &["GET"])?,
            ceph_cors_rule(&["start*end"], &["GET"])?,
            ceph_cors_rule(&["prefix*"], &["GET"])?,
            ceph_cors_rule(&["*.put"], &["PUT"])?,
        ])?)
        .send()
        .await?;

    let bucket_path = bucket_path(&bucket);
    let object_path = object_path(&bucket, "bar");

    let response = signed_cors_request(&server, Method::GET, &bucket_path, &[]).await?;
    assert_cors_response(&response, StatusCode::OK, None, None);

    let response = signed_cors_request(
        &server,
        Method::GET,
        &bucket_path,
        &[("Origin", "foo.suffix")],
    )
    .await?;
    assert_cors_response(&response, StatusCode::OK, Some("foo.suffix"), Some("GET"));

    let response =
        signed_cors_request(&server, Method::GET, &bucket_path, &[("Origin", "foo.bar")]).await?;
    assert_cors_response(&response, StatusCode::OK, None, None);

    let response = signed_cors_request(
        &server,
        Method::GET,
        &bucket_path,
        &[("Origin", "foo.suffix.get")],
    )
    .await?;
    assert_cors_response(&response, StatusCode::OK, None, None);

    for origin in ["startend", "start1end", "start12end"] {
        let response =
            signed_cors_request(&server, Method::GET, &bucket_path, &[("Origin", origin)]).await?;
        assert_cors_response(&response, StatusCode::OK, Some(origin), Some("GET"));
    }

    let response = signed_cors_request(
        &server,
        Method::GET,
        &bucket_path,
        &[("Origin", "0start12end")],
    )
    .await?;
    assert_cors_response(&response, StatusCode::OK, None, None);

    for origin in ["prefix", "prefix.suffix"] {
        let response =
            signed_cors_request(&server, Method::GET, &bucket_path, &[("Origin", origin)]).await?;
        assert_cors_response(&response, StatusCode::OK, Some(origin), Some("GET"));
    }

    let response = signed_cors_request(
        &server,
        Method::GET,
        &bucket_path,
        &[("Origin", "bla.prefix")],
    )
    .await?;
    assert_cors_response(&response, StatusCode::OK, None, None);

    let response = signed_cors_request(
        &server,
        Method::GET,
        &object_path,
        &[("Origin", "foo.suffix")],
    )
    .await?;
    assert_cors_response(
        &response,
        StatusCode::NOT_FOUND,
        Some("foo.suffix"),
        Some("GET"),
    );

    let response = unsigned_cors_request(
        &server,
        Method::PUT,
        &object_path,
        &[
            ("Origin", "foo.suffix"),
            ("Access-Control-Request-Method", "GET"),
            ("content-length", "0"),
        ],
    )
    .await?;
    assert_cors_response(
        &response,
        StatusCode::FORBIDDEN,
        Some("foo.suffix"),
        Some("GET"),
    );

    let response = unsigned_cors_request(
        &server,
        Method::PUT,
        &object_path,
        &[
            ("Origin", "foo.suffix"),
            ("Access-Control-Request-Method", "PUT"),
            ("content-length", "0"),
        ],
    )
    .await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    let response = unsigned_cors_request(
        &server,
        Method::PUT,
        &object_path,
        &[
            ("Origin", "foo.suffix"),
            ("Access-Control-Request-Method", "DELETE"),
            ("content-length", "0"),
        ],
    )
    .await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    let response = unsigned_cors_request(
        &server,
        Method::PUT,
        &object_path,
        &[("Origin", "foo.suffix"), ("content-length", "0")],
    )
    .await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    let response = unsigned_cors_request(
        &server,
        Method::PUT,
        &object_path,
        &[("Origin", "foo.put"), ("content-length", "0")],
    )
    .await?;
    assert_cors_response(
        &response,
        StatusCode::FORBIDDEN,
        Some("foo.put"),
        Some("PUT"),
    );

    let response = signed_cors_request(
        &server,
        Method::GET,
        &object_path,
        &[("Origin", "foo.suffix")],
    )
    .await?;
    assert_cors_response(
        &response,
        StatusCode::NOT_FOUND,
        Some("foo.suffix"),
        Some("GET"),
    );

    let response = options_request(&server, &bucket_path, &[]).await?;
    assert_cors_response(&response, StatusCode::BAD_REQUEST, None, None);

    let response = options_request(&server, &bucket_path, &[("Origin", "foo.suffix")]).await?;
    assert_cors_response(&response, StatusCode::BAD_REQUEST, None, None);

    let response = options_request(&server, &bucket_path, &[("Origin", "bla")]).await?;
    assert_cors_response(&response, StatusCode::BAD_REQUEST, None, None);

    let response = options_request(
        &server,
        &object_path,
        &[
            ("Origin", "foo.suffix"),
            ("Access-Control-Request-Method", "GET"),
            ("content-length", "0"),
        ],
    )
    .await?;
    assert_cors_response(&response, StatusCode::OK, Some("foo.suffix"), Some("GET"));

    let response = preflight(&server, &bucket_path, "foo.bar", "GET", None).await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    let response = preflight(&server, &bucket_path, "foo.suffix.get", "GET", None).await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    for origin in ["startend", "start1end", "start12end"] {
        let response = preflight(&server, &bucket_path, origin, "GET", None).await?;
        assert_cors_response(&response, StatusCode::OK, Some(origin), Some("GET"));
    }

    let response = preflight(&server, &bucket_path, "0start12end", "GET", None).await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    for origin in ["prefix", "prefix.suffix"] {
        let response = preflight(&server, &bucket_path, origin, "GET", None).await?;
        assert_cors_response(&response, StatusCode::OK, Some(origin), Some("GET"));
    }

    let response = preflight(&server, &bucket_path, "bla.prefix", "GET", None).await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    let response = preflight(&server, &bucket_path, "foo.put", "GET", None).await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    let response = preflight(&server, &bucket_path, "foo.put", "PUT", None).await?;
    assert_cors_response(&response, StatusCode::OK, Some("foo.put"), Some("PUT"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_cors_origin_wildcard() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_cors_origin_wildcard: uses s3ite owner-only auth instead of public ACL setup
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-wildcard");

    create_bucket(&client, &bucket).await?;
    let missing = client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("empty bucket CORS config should fail");
    assert_error_contains(&missing, "NoSuchCORSConfiguration");

    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(vec![ceph_cors_rule(&["*"], &["GET"])?])?)
        .send()
        .await?;

    let path = bucket_path(&bucket);
    let response = signed_cors_request(&server, Method::GET, &path, &[]).await?;
    assert_cors_response(&response, StatusCode::OK, None, None);

    let response =
        signed_cors_request(&server, Method::GET, &path, &[("Origin", "example.origin")]).await?;
    assert_cors_response(&response, StatusCode::OK, Some("*"), Some("GET"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_cors_header_option() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_cors_header_option: uses s3ite owner-only auth instead of public ACL setup
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-header");

    create_bucket(&client, &bucket).await?;
    let missing = client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("empty bucket CORS config should fail");
    assert_error_contains(&missing, "NoSuchCORSConfiguration");

    client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(vec![cors_rule_with_options(
            &["*"],
            &["GET"],
            &[],
            &["x-amz-meta-header1"],
            None,
            None,
        )?])?)
        .send()
        .await?;

    let response = options_request(
        &server,
        &object_path(&bucket, "bar"),
        &[
            ("Origin", "example.origin"),
            ("Access-Control-Request-Headers", "x-amz-meta-header2"),
            ("Access-Control-Request-Method", "GET"),
        ],
    )
    .await?;
    assert_cors_response(&response, StatusCode::FORBIDDEN, None, None);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_cors_presigned_get_object() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_cors_presigned_get_object: uses s3ite owner-only auth instead of public ACL setup
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-presign-get");
    let key = "foo";

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;

    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(100_000))?)
        .await?;
    assert_presigned_options_method(&client, &bucket, presigned.uri(), "GET").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_cors_presigned_put_object() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_cors_presigned_put_object: uses s3ite owner-only auth instead of public ACL setup
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-presign-put");

    create_bucket(&client, &bucket).await?;
    let presigned = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(100_000))?)
        .await?;
    assert_presigned_options_method(&client, &bucket, presigned.uri(), "PUT").await
}

#[tokio::test]
#[serial(conformance)]
async fn test_cors_presigned_put_object_with_acl() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_cors_presigned_put_object_with_acl: uses s3ite owner-only auth instead of public ACL setup
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-presign-put-acl");

    create_bucket(&client, &bucket).await?;
    let presigned = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .acl(ObjectCannedAcl::Private)
        .presigned(PresigningConfig::expires_in(Duration::from_secs(100_000))?)
        .await?;
    assert_presigned_options_method(&client, &bucket, presigned.uri(), "PUT").await
}

/// s3ite-specific: validates rule shapes beyond the direct Ceph CORS ports.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_bucket_cors_rejects_invalid_rules() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-invalid");

    create_bucket(&client, &bucket).await?;
    let invalid_method = client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(vec![cors_rule(
            &["https://app.example.test"],
            &["PATCH"],
            &[],
            &[],
        )?])?)
        .send()
        .await
        .expect_err("invalid CORS method should fail");
    assert_error_contains(&invalid_method, "InvalidRequest");

    let mut too_many_rules = Vec::new();
    for index in 0..101 {
        too_many_rules.push(
            CorsRule::builder()
                .id(index.to_string())
                .allowed_origins("https://app.example.test")
                .allowed_methods("GET")
                .build()?,
        );
    }
    let too_many = client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(too_many_rules)?)
        .send()
        .await
        .expect_err("too many CORS rules should fail");
    assert_error_contains(&too_many, "InvalidRequest");

    let overlong_id = client
        .put_bucket_cors()
        .bucket(&bucket)
        .cors_configuration(cors_config(vec![
            CorsRule::builder()
                .id("x".repeat(256))
                .allowed_origins("https://app.example.test")
                .allowed_methods("GET")
                .build()?,
        ])?)
        .send()
        .await
        .expect_err("overlong CORS rule ID should fail");
    assert_error_contains(&overlong_id, "InvalidRequest");

    Ok(())
}

/// s3ite-specific: CORS APIs honor storage and owner guardrails.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_bucket_cors_missing_read_only_and_expected_owner_errors() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-errors");

    let missing = client
        .get_bucket_cors()
        .bucket(&bucket)
        .send()
        .await
        .expect_err("missing bucket should fail");
    assert_error_contains(&missing, "NoSuchBucket");

    create_bucket(&client, &bucket).await?;
    let denied = client
        .put_bucket_cors()
        .bucket(&bucket)
        .expected_bucket_owner(WRONG_OWNER)
        .cors_configuration(cors_config(vec![cors_rule(
            &["https://app.example.test"],
            &["GET"],
            &[],
            &[],
        )?])?)
        .send()
        .await
        .expect_err("wrong expected owner should fail");
    assert_error_contains(&denied, "AccessDenied");

    client
        .put_bucket_cors()
        .bucket(&bucket)
        .expected_bucket_owner(OWNER_ID)
        .cors_configuration(cors_config(vec![cors_rule(
            &["https://app.example.test"],
            &["GET"],
            &[],
            &[],
        )?])?)
        .send()
        .await?;

    let read_only_server = TestServer::spawn_with(TestServerOptions {
        config: Some("read_only: true\n".to_string()),
        ..Default::default()
    })
    .await?;
    let read_only_client = sdk::client(&read_only_server);
    let read_only = read_only_client
        .put_bucket_cors()
        .bucket(bucket_name("s3ite-cors-ro"))
        .cors_configuration(cors_config(vec![cors_rule(
            &["https://app.example.test"],
            &["GET"],
            &[],
            &[],
        )?])?)
        .send()
        .await
        .expect_err("read-only bucket CORS mutation should fail");
    assert_error_contains(&read_only, "MethodNotAllowed");

    Ok(())
}

/// s3ite-specific: bucket CORS is isolated per bucket and overrides fallback.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_bucket_cors_is_per_bucket() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket_a = bucket_name("s3ite-cors-a");
    let bucket_b = bucket_name("s3ite-cors-b");

    create_bucket(&client, &bucket_a).await?;
    create_bucket(&client, &bucket_b).await?;
    client
        .put_bucket_cors()
        .bucket(&bucket_a)
        .cors_configuration(cors_config(vec![cors_rule(
            &["https://a.example.test"],
            &["GET"],
            &[],
            &[],
        )?])?)
        .send()
        .await?;
    client
        .put_bucket_cors()
        .bucket(&bucket_b)
        .cors_configuration(cors_config(vec![cors_rule(
            &["https://b.example.test"],
            &["GET"],
            &[],
            &[],
        )?])?)
        .send()
        .await?;

    let path_a = bucket_path(&bucket_a);
    let allowed_a = preflight(&server, &path_a, "https://a.example.test", "GET", None).await?;
    assert_cors_response(
        &allowed_a,
        StatusCode::OK,
        Some("https://a.example.test"),
        Some("GET"),
    );

    let blocked_a = preflight(&server, &path_a, "https://b.example.test", "GET", None).await?;
    assert_cors_response(&blocked_a, StatusCode::FORBIDDEN, None, None);

    let path_b = bucket_path(&bucket_b);
    let allowed_b = preflight(&server, &path_b, "https://b.example.test", "GET", None).await?;
    assert_cors_response(
        &allowed_b,
        StatusCode::OK,
        Some("https://b.example.test"),
        Some("GET"),
    );

    Ok(())
}

/// s3ite-specific: buckets without CORS config still use server-level fallback.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_bucket_cors_falls_back_to_server_cors_when_unconfigured() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-cors-fallback");

    create_bucket(&client, &bucket).await?;

    let response = preflight(
        &server,
        &bucket_path(&bucket),
        "https://fallback.example.test",
        "PUT",
        Some("authorization"),
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    assert_eq!(
        header(&response, "access-control-allow-origin").as_deref(),
        Some("https://fallback.example.test")
    );
    assert_eq!(
        header(&response, "access-control-allow-credentials").as_deref(),
        Some("true")
    );

    Ok(())
}
