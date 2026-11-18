//! A loopback KMS JSON service used only by the real-binary conformance tests.

use crate::support::{
    server::{TestServer, TestServerOptions},
    sse::SSE_S3_KEY,
};
use anyhow::{Context, Result, ensure};
use aws_sdk_s3::{
    Client,
    types::{
        ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
        ServerSideEncryptionRule,
    },
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, task::JoinHandle};
use uuid::Uuid;

pub const PRIMARY: &str = "testkey-1";
pub const SECONDARY: &str = "testkey-2";
pub const ALGORITHM: &str = "x-amz-server-side-encryption";
pub const KEY_ID: &str = "x-amz-server-side-encryption-aws-kms-key-id";

#[derive(Default)]
struct State {
    keys: HashMap<String, (String, Value, String)>,
    failure: Option<&'static str>,
}

pub struct KmsServer {
    pub server: TestServer,
    listener: JoinHandle<()>,
    state: Arc<Mutex<State>>,
}

impl Drop for KmsServer {
    /// Stops accepting requests when this test releases its service.
    fn drop(&mut self) {
        self.listener.abort();
    }
}

impl KmsServer {
    /// Runs the release-path SDK against an isolated KMS endpoint and credential set.
    pub async fn start() -> Result<Self> {
        let socket = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = socket.local_addr()?;
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let listener = tokio::spawn(async move {
            while let Ok((stream, _)) = socket.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let handler = service_fn(|request| handle(request, state.clone()));
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), handler)
                        .await;
                });
            }
        });
        let server = TestServer::spawn_with(TestServerOptions {
            config: Some(format!("kms_region: us-east-1\nsse_s3_key: {SSE_S3_KEY}")),
            env: vec![
                ("AWS_ENDPOINT_URL_KMS", format!("http://{addr}")),
                ("AWS_ACCESS_KEY_ID", "kms-test-identity".to_owned()),
                ("AWS_SECRET_ACCESS_KEY", "kms-test-secret".to_owned()),
                ("AWS_MAX_ATTEMPTS", "1".to_owned()),
            ],
            ..Default::default()
        })
        .await?;
        Ok(Self {
            server,
            listener,
            state,
        })
    }

    /// Injects a provider failure on subsequent requests.
    pub fn fail_with(&self, failure: Option<&'static str>) {
        self.state.lock().unwrap().failure = failure;
    }
}

/// Handles signed GenerateDataKey and Decrypt calls with key- and context-bound opaque tokens.
async fn handle(
    request: Request<Incoming>,
    state: Arc<Mutex<State>>,
) -> Result<Response<Full<Bytes>>> {
    ensure!(request.method() == "POST");
    let authorization = request.headers()["authorization"].to_str()?;
    ensure!(
        authorization.contains("Credential=kms-test-identity/")
            && authorization.contains("/us-east-1/kms/aws4_request")
    );
    let target = request.headers()["x-amz-target"].to_str()?.to_owned();
    let payload: Value = serde_json::from_slice(&request.into_body().collect().await?.to_bytes())?;
    let mut state = state.lock().unwrap();
    let result = state.apply(&target, &payload);
    let (status, json) = match result {
        Ok(response) => (200, response),
        Err(code) => (400, json!({"__type":code,"message":"mock KMS error"})),
    };
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/x-amz-json-1.1")
        .body(Full::new(Bytes::from(json.to_string())))?)
}

impl State {
    /// Associates each generated plaintext key with its own token and encryption context.
    fn apply(&mut self, target: &str, request: &Value) -> Result<Value, &'static str> {
        if let Some(code) = self.failure {
            return Err(code);
        }
        let id = match request["KeyId"].as_str() {
            Some("alias/primary") if target == "TrentService.GenerateDataKey" => PRIMARY,
            Some(id) => id,
            None => return Err("NotFoundException"),
        };
        if ![PRIMARY, SECONDARY].contains(&id) {
            return Err("NotFoundException");
        }
        let context = &request["EncryptionContext"];
        if !context["aws:s3:arn"]
            .as_str()
            .is_some_and(|arn| arn.starts_with("arn:aws:s3:::"))
        {
            return Err("InvalidCiphertextException");
        }
        match target {
            "TrentService.GenerateDataKey" if request["KeySpec"] == "AES_256" => {
                let mut key = [0; 32];
                getrandom::fill(&mut key).unwrap();
                let plaintext = STANDARD.encode(key);
                let token = STANDARD.encode(Uuid::new_v4().as_bytes());
                self.keys.insert(
                    token.clone(),
                    (id.to_owned(), context.clone(), plaintext.clone()),
                );
                Ok(json!({"KeyId":id,"CiphertextBlob":token,"Plaintext":plaintext}))
            }
            "TrentService.Decrypt" => {
                let (original, bound, key) = request["CiphertextBlob"]
                    .as_str()
                    .and_then(|v| self.keys.get(v))
                    .ok_or("InvalidCiphertextException")?;
                if original != id || bound != context {
                    return Err("IncorrectKeyException");
                }
                Ok(json!({"KeyId":id,"Plaintext":key}))
            }
            _ => Err("ValidationException"),
        }
    }
}

/// Sets the primary test KMS key as the bucket encryption default.
pub async fn set_default(client: &Client, bucket: &str) -> Result<()> {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::AwsKms)
        .kms_master_key_id(PRIMARY)
        .build()?;
    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(default)
        .build();
    let config = ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()
        .context("bucket encryption rule")?;
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(config)
        .send()
        .await?;
    Ok(())
}
