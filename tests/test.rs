#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

use md5::Digest;
use md5::Md5;
use s3ite::Sqlite;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;

use std::env;
use std::fs;
use std::ops::Deref;

use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::BucketLocationConstraint;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::types::CreateBucketConfiguration;
use aws_sdk_s3::Client;

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tracing::{debug, error};
use uuid::Uuid;

const FS_ROOT: &str = concat!(env!("CARGO_TARGET_TMPDIR"), "/s3s-sqlite-tests-aws");
const DOMAIN_NAME: &str = "localhost:8014";
const REGION: &str = "us-west-2";

static TRACING: Lazy<()> = Lazy::new(|| {
    use tracing_subscriber::EnvFilter;

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "it_aws=debug,s3s_sqlite=debug,s3s=debug");
    }

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .init();
});

pub struct TestContext {
    pub client: Client,
}

impl TestContext {
    pub async fn new() -> Self {
        Lazy::force(&TRACING);

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        fs::create_dir_all(FS_ROOT).unwrap();
        let fs = Sqlite::new(FS_ROOT).await.unwrap();

        // Setup S3 service
        let service = {
            let mut b = S3ServiceBuilder::new(fs);
            b.set_auth(SimpleAuth::from_single(
                cred.access_key_id(),
                cred.secret_access_key(),
            ));
            b.set_base_domain(DOMAIN_NAME);
            b.build()
        };

        // Convert to aws http connector
        let conn = s3s_aws::Connector::from(service.into_shared());

        // Setup aws sdk config
        let config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_connector(conn)
            .region(Region::new(REGION))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build();

        Self {
            client: Client::new(&config),
        }
    }
}

impl Deref for TestContext {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        fs::remove_dir_all(FS_ROOT).unwrap();
    }
}

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    LOCK.lock().await
}

async fn create_bucket(c: &Client, bucket: &str) -> Result<()> {
    let location = BucketLocationConstraint::from(REGION);
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(location)
        .build();

    c.create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket)
        .send()
        .await?;

    debug!("created bucket: {bucket:?}");
    Ok(())
}

async fn delete_object(c: &Client, bucket: &str, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

pub fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input, hex_simd::AsciiCase::Lower)
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_buckets() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new().await;

    let buckets = vec![
        format!("test-list-buckets-{}", Uuid::new_v4()),
        format!("test-list-buckets-{}", Uuid::new_v4()),
        format!("test-list-buckets-{}", Uuid::new_v4()),
        format!("test-list-buckets-{}", Uuid::new_v4()),
        format!("test-list-buckets-{}", Uuid::new_v4()),
    ];

    for bucket in &buckets {
        create_bucket(&context, bucket).await?;
    }

    // test
    let result = context.list_buckets().send().await;
    match result {
        Ok(ref ans) => debug!(?ans),
        Err(ref err) => error!(?err),
    }
    let list_buckets = result?;

    list_buckets.buckets().unwrap().iter().all(|list_bucket| {
        buckets
            .iter()
            .any(|bucket| Some(bucket.as_str()) == list_bucket.name())
    });

    for bucket in &buckets {
        delete_bucket(&context, bucket).await?;
    }

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_single_object() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new().await;

    let bucket = format!("test-single-object-{}", Uuid::new_v4());
    let key = "sample.txt";
    let content = "hello world\n你好世界\n";
    let content_bytes = content.as_bytes();
    let mut md5_hash = Md5::new();
    md5_hash.update(content_bytes);
    let content_md5 = hex(md5_hash.finalize());

    create_bucket(&context, &bucket).await?;

    {
        context
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(content_bytes))
            .content_md5(content_md5)
            .send()
            .await?;
    }

    {
        let get_object_output = context.get_object().bucket(&bucket).key(key).send().await?;
        let content_length: usize = get_object_output.content_length().try_into().unwrap();
        let body = get_object_output.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&context, &bucket, key).await?;
    }

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_multipart() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new().await;

    let bucket = format!("test-multipart-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let key = "sample.txt";
    let content = "abcdefghijklmnopqrstuvwxyz/0123456789/!@#$%^&*();\n";
    let content_bytes = content.as_bytes();
    let mut md5_hash = Md5::new();
    md5_hash.update(content_bytes);
    let content_md5 = hex(md5_hash.finalize());

    let upload_id = {
        let result = context
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        match &result {
            Ok(ref ans) => debug!(?ans),
            Err(ref err) => error!(?err),
        }
        result?.upload_id.unwrap()
    };
    let upload_id = upload_id.as_str();

    let upload_parts = {
        let body = ByteStream::from_static(content_bytes);
        let part_number = 1;

        let result = context
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .body(body)
            .part_number(part_number)
            .content_md5(content_md5)
            .send()
            .await;

        match &result {
            Ok(ref ans) => debug!(?ans),
            Err(ref err) => error!(?err),
        }

        let part = CompletedPart::builder()
            .e_tag(result?.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build();

        vec![part]
    };

    {
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _ = context
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .multipart_upload(upload)
            .upload_id(upload_id)
            .send()
            .await?;
    }

    {
        let result = context.get_object().bucket(&bucket).key(key).send().await;
        match result {
            Ok(get_object_output) => {
                debug!(?get_object_output);

                let content_length: usize = get_object_output.content_length().try_into().unwrap();
                let body = get_object_output.body.collect().await?.into_bytes();

                assert_eq!(content_length, content.len());
                assert_eq!(body.as_ref(), content.as_bytes());
            }
            Err(ref err) => error!(?err),
        }
    }

    {
        delete_object(&context, &bucket, key).await?;
        delete_bucket(&context, &bucket).await?;
    }

    Ok(())
}
