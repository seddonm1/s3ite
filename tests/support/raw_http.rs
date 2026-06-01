use anyhow::Result;
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
use std::time::SystemTime;

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
