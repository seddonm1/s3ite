use std::{future::Future, time::SystemTime};

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
use reqwest::{Client, Method, Response};

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
    let signing_headers = signing_headers(server, &method, &url, &body, headers)?;

    let client = client()?;
    let mut request = client.request(method, &url).body(body);
    for (name, value) in headers {
        request = request.header(*name, *value);
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
    let signing_headers = signing_headers(server, &method, &url, &body, headers)?;
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
    for (name, value) in headers {
        request = request.header(*name, *value);
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
    let signable_request = SignableRequest::new(
        method.as_str(),
        url,
        headers.iter().copied(),
        SignableBody::Bytes(body),
    )?;
    let (signing_instructions, _signature) = sign(signable_request, &signing_params)?.into_parts();
    Ok(signing_instructions
        .headers()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect())
}
