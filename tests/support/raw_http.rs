use std::{
    future::Future,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{
        PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation,
        SigningInstructions, SigningSettings, UriPathNormalizationMode, sign,
    },
    sign::v4,
};
use bytes::Bytes;
use futures::stream;
use hyper::Request;
use reqwest::{Client, Method, Response, StatusCode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use super::server::TestServer;

pub fn client() -> Result<Client> {
    Ok(Client::builder().build()?)
}

pub async fn unsigned_request(server: &TestServer, method: Method, path: &str) -> Result<Response> {
    let client = client()?;
    let url = format!("{}{}", server.endpoint(), path);
    Ok(client.request(method, url).send().await?)
}

pub async fn unsigned_get(server: &TestServer, path: &str) -> Result<Response> {
    unsigned_request(server, Method::GET, path).await
}

pub async fn request_url(method: Method, url: &str) -> Result<Response> {
    let client = client()?;
    Ok(client.request(method, url).send().await?)
}

pub async fn get_url(url: &str) -> Result<Response> {
    request_url(Method::GET, url).await
}

pub fn path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            byte => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                encoded.push('%');
                encoded.push(HEX[(byte >> 4) as usize] as char);
                encoded.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    encoded
}

pub struct RawHttpResponse {
    pub status: StatusCode,
    pub body: String,
}

pub async fn signed_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    body: Vec<u8>,
) -> Result<Response> {
    signed_request_with_headers(server, method, path_and_query, body, &[]).await
}

pub async fn signed_request_with_headers(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Result<Response> {
    let url = format!("{}{}", server.endpoint(), path_and_query);
    let content_length = body.len().to_string();
    let mut request_headers = Vec::with_capacity(headers.len() + 1);
    request_headers.extend(headers.iter().copied());
    if matches!(method, Method::PUT | Method::POST)
        && !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
    {
        request_headers.push(("Content-Length", content_length.as_str()));
    }
    let signing_headers = signing_headers(server, &method, &url, &body, &request_headers)?;

    let client = client()?;
    let mut request = client.request(method, &url).body(body);
    for (name, value) in request_headers {
        request = request.header(name, value);
    }
    for (name, value) in signing_headers {
        request = request.header(name, value);
    }

    Ok(request.send().await?)
}

pub async fn signed_chunked_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    body: Vec<u8>,
) -> Result<Response> {
    let url = format!("{}{}", server.endpoint(), path_and_query);
    let signing_headers = signing_headers(server, &method, &url, &body, &[])?;
    let stream = stream::once(async move { Ok::<_, std::io::Error>(Bytes::from(body)) });

    let client = client()?;
    let mut request = client
        .request(method, &url)
        .body(reqwest::Body::wrap_stream(stream));
    for (name, value) in signing_headers {
        request = request.header(name, value);
    }

    Ok(request.send().await?)
}

/// Sends a SigV4 `STREAMING-UNSIGNED-PAYLOAD-TRAILER` request with one trailer field.
///
/// The payload is framed as a single `aws-chunked` data chunk followed by the
/// terminating chunk and the trailer. When `trailer_value` is `None`, the
/// trailer is declared in `x-amz-trailer` but omitted from the body.
pub async fn unsigned_trailer_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    payload: &[u8],
    trailer_name: &str,
    trailer_value: Option<&str>,
) -> Result<Response> {
    let trailers = trailer_value
        .map(|value| vec![(trailer_name, value)])
        .unwrap_or_default();
    trailer_request(
        server,
        method,
        path_and_query,
        TrailerBody {
            payload,
            declared_trailer: trailer_name,
            trailers: &trailers,
            extra_headers: &[],
        },
    )
    .await
}

/// One `aws-chunked` body with its declared and actual trailer lines.
pub struct TrailerBody<'a> {
    /// Decoded object bytes, framed as a single chunk.
    pub payload: &'a [u8],
    /// Value of the `x-amz-trailer` request header.
    pub declared_trailer: &'a str,
    /// Trailer lines sent after the final chunk, in order.
    pub trailers: &'a [(&'a str, &'a str)],
    /// Additional signed request headers.
    pub extra_headers: &'a [(&'a str, &'a str)],
}

/// Sends a `STREAMING-UNSIGNED-PAYLOAD-TRAILER` request with arbitrary trailer lines.
pub async fn trailer_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    body: TrailerBody<'_>,
) -> Result<Response> {
    aws_chunked_request(
        server,
        method,
        path_and_query,
        AwsChunkedRequest {
            chunks: &[body.payload],
            decoded_length: body.payload.len(),
            declared_trailer: body.declared_trailer,
            trailers: body.trailers,
            extra_headers: body.extra_headers,
            http_chunked: false,
        },
    )
    .await
}

/// The AWS-framed body and the independent HTTP and decoded lengths.
pub struct AwsChunkedRequest<'a> {
    pub chunks: &'a [&'a [u8]],
    pub decoded_length: usize,
    pub declared_trailer: &'a str,
    pub trailers: &'a [(&'a str, &'a str)],
    pub extra_headers: &'a [(&'a str, &'a str)],
    pub http_chunked: bool,
}

/// Sends actual aws-chunked frames, optionally inside HTTP chunked transfer.
pub async fn aws_chunked_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    body: AwsChunkedRequest<'_>,
) -> Result<Response> {
    let url = format!("{}{}", server.endpoint(), path_and_query);
    let mut framed = Vec::new();
    for chunk in body.chunks {
        framed.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        framed.extend_from_slice(chunk);
        framed.extend_from_slice(b"\r\n");
    }
    framed.extend_from_slice(b"0\r\n");
    for (name, value) in body.trailers {
        framed.extend_from_slice(format!("{name}:{value}\r\n").as_bytes());
    }
    framed.extend_from_slice(b"\r\n");

    let content_length = framed.len().to_string();
    let decoded_length = body.decoded_length.to_string();
    let mut headers = vec![
        ("Content-Encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", decoded_length.as_str()),
        ("x-amz-trailer", body.declared_trailer),
    ];
    if body.http_chunked {
        headers.push(("Transfer-Encoding", "chunked"));
    } else {
        headers.push(("Content-Length", content_length.as_str()));
    }
    headers.extend_from_slice(body.extra_headers);
    let mut signing_settings = SigningSettings::default();
    signing_settings.percent_encoding_mode = PercentEncodingMode::Single;
    signing_settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let signing = sign_request(
        server,
        &method,
        &url,
        SignableBody::Precomputed("STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string()),
        &headers,
        signing_settings,
    )?;

    let client = client()?;
    let transport_body = if body.http_chunked {
        reqwest::Body::wrap_stream(stream::once(async move {
            Ok::<_, std::io::Error>(Bytes::from(framed))
        }))
    } else {
        reqwest::Body::from(framed)
    };
    let mut request = client.request(method, &url).body(transport_body);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    for (name, value) in signing.headers() {
        request = request.header(name, value);
    }
    Ok(request.send().await?)
}

/// Sends signed request headers and half-closes the client write side.
pub async fn signed_header_only_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
) -> Result<RawHttpResponse> {
    signed_header_only_exchange(server, method, path_and_query, headers, true).await
}

/// Sends Expect: 100-continue without sending a body or closing the write side.
pub async fn signed_expect_continue_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
) -> Result<RawHttpResponse> {
    let mut headers = headers.to_vec();
    headers.push(("Expect", "100-continue"));
    signed_header_only_exchange(server, method, path_and_query, &headers, false).await
}

/// Reads a raw response after sending only the signed HTTP/1.1 request headers.
async fn signed_header_only_exchange(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
    half_close: bool,
) -> Result<RawHttpResponse> {
    let url = format!("{}{}", server.endpoint(), path_and_query);
    let signing_headers = signing_headers(server, &method, &url, &[], headers)?;
    let authority = server
        .endpoint()
        .strip_prefix("http://")
        .context("test server endpoint should be HTTP")?;

    let mut request = format!(
        "{} {path_and_query} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n",
        method.as_str()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    for (name, value) in signing_headers {
        if headers
            .iter()
            .any(|(provided, _)| provided.eq_ignore_ascii_case(&name))
        {
            continue;
        }
        request.push_str(&name);
        request.push_str(": ");
        request.push_str(&value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let mut stream = tokio::net::TcpStream::connect(authority).await?;
    stream.write_all(request.as_bytes()).await?;
    if half_close {
        stream.shutdown().await?;
    }

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .context("timed out waiting for raw response")??;
    parse_raw_response(&response)
}

fn parse_raw_response(response: &[u8]) -> Result<RawHttpResponse> {
    let text = String::from_utf8_lossy(response);
    let status_line = text
        .lines()
        .next()
        .context("raw response missing status line")?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .context("raw response status line missing code")?
        .parse::<u16>()?;
    let status = StatusCode::from_u16(status_code)?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok(RawHttpResponse { status, body })
}

pub async fn signed_streaming_request_after_first_chunk<F, Fut>(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    body: Vec<u8>,
    chunk_size: usize,
    headers: &[(&str, &str)],
    after_first_chunk: F,
) -> Result<Response>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let url = format!("{}{}", server.endpoint(), path_and_query);
    let content_length = body.len().to_string();
    let mut request_headers = Vec::with_capacity(headers.len() + 1);
    request_headers.extend(headers.iter().copied());
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
    {
        request_headers.push(("Content-Length", content_length.as_str()));
    }
    let signing_headers = signing_headers(server, &method, &url, &body, &request_headers)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let sender_task = tokio::spawn(async move {
        let chunk_size = chunk_size.max(1);
        let mut after_first_chunk = Some(after_first_chunk);
        for (index, chunk) in body.chunks(chunk_size).enumerate() {
            sender
                .send(Ok::<_, std::io::Error>(Bytes::copy_from_slice(chunk)))
                .await
                .context("send streaming request chunk")?;
            if index == 0
                && let Some(after_first_chunk) = after_first_chunk.take()
            {
                after_first_chunk().await?;
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let stream = stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|item| (item, receiver))
    });

    let client = client()?;
    let mut request = client
        .request(method, &url)
        .body(reqwest::Body::wrap_stream(stream));
    for (name, value) in request_headers {
        request = request.header(name, value);
    }
    for (name, value) in signing_headers {
        request = request.header(name, value);
    }

    let response = request.send().await?;
    if sender_task.is_finished() {
        sender_task.await??;
    } else {
        sender_task.abort();
    }
    Ok(response)
}

fn signing_headers(
    server: &TestServer,
    method: &Method,
    url: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Result<Vec<(String, String)>> {
    signing_headers_with_body(server, method, url, SignableBody::Bytes(body), headers)
}

/// Signs a raw request, including any body representation used by the test.
fn signing_headers_with_body(
    server: &TestServer,
    method: &Method,
    url: &str,
    body: SignableBody<'_>,
    headers: &[(&str, &str)],
) -> Result<Vec<(String, String)>> {
    let mut signing_settings = SigningSettings::default();
    signing_settings.percent_encoding_mode = PercentEncodingMode::Single;
    signing_settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;

    Ok(
        sign_request(server, method, url, body, headers, signing_settings)?
            .headers()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect(),
    )
}

/// Presigns an S3 operation for which the Rust SDK has no presigning builder.
pub fn presigned_url(server: &TestServer, method: Method, path_and_query: &str) -> Result<String> {
    let url = format!("{}{path_and_query}", server.endpoint());
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(Duration::from_secs(60));
    let instructions = sign_request(
        server,
        &method,
        &url,
        SignableBody::UnsignedPayload,
        &[],
        settings,
    )?;
    let mut request = Request::builder().method(method).uri(url).body(())?;
    instructions.apply_to_request_http1x(&mut request);
    Ok(request.uri().to_string())
}

/// Signs with the test owner's credentials and the selected header or query settings.
fn sign_request(
    server: &TestServer,
    method: &Method,
    url: &str,
    body: SignableBody<'_>,
    headers: &[(&str, &str)],
    signing_settings: SigningSettings,
) -> Result<SigningInstructions> {
    let credentials = Credentials::new(
        server.access_key(),
        server.secret_key(),
        None,
        None,
        "s3ite-test",
    );
    let identity = credentials.into();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()?
        .into();
    let signable_request =
        SignableRequest::new(method.as_str(), url, headers.iter().copied(), body)?;
    let (signing_instructions, _signature) = sign(signable_request, &signing_params)?.into_parts();
    Ok(signing_instructions)
}
