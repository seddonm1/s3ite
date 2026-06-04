use std::{
    future::Future,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{
        PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
        UriPathNormalizationMode, sign,
    },
    sign::v4,
};
use bytes::Bytes;
use futures::stream;
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

pub async fn signed_header_only_request(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
) -> Result<RawHttpResponse> {
    signed_header_only_request_with_body(
        server,
        method,
        path_and_query,
        headers,
        SignableBody::Bytes(&[]),
    )
    .await
}

async fn signed_header_only_request_with_body(
    server: &TestServer,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: SignableBody<'_>,
) -> Result<RawHttpResponse> {
    let url = format!("{}{}", server.endpoint(), path_and_query);
    let signing_headers = signing_headers_with_body(server, &method, &url, body, headers)?;
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
    stream.shutdown().await?;

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
    Ok(signing_instructions
        .headers()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect())
}
