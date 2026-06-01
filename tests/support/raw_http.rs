use anyhow::Result;
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
