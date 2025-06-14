#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

use std::{env, fs, ops::Deref};

use anyhow::Result;
use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    config::{Credentials, Region},
    primitives::ByteStream,
    types::{
        BucketLocationConstraint, CompletedMultipartUpload, CompletedPart,
        CreateBucketConfiguration,
    },
    Client,
};
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use s3ite::{Config, Sqlite};
use s3s::{auth::SimpleAuth, host::MultiDomain, service::S3ServiceBuilder};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, error};
use uuid::Uuid;

const FS_ROOT: &str = concat!(env!("CARGO_TARGET_TMPDIR"), "/s3s-sqlite-tests-aws");
const DOMAIN_NAME: &str = "localhost:8014";
const REGION: &str = "us-west-2";

static TRACING: Lazy<()> = Lazy::new(|| {
    use tracing_subscriber::EnvFilter;

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "it_aws=error,s3s_sqlite=debug,s3s=error");
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
    pub async fn new(config: Option<Config>) -> Self {
        Lazy::force(&TRACING);

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        fs::create_dir_all(FS_ROOT).unwrap();
        let mut config = config.unwrap_or_default();
        config.root = FS_ROOT.into();

        let fs = Sqlite::new(&config).await.unwrap();

        // Setup S3 service
        let service = {
            let mut b = S3ServiceBuilder::new(fs);
            b.set_auth(SimpleAuth::from_single(
                cred.access_key_id(),
                cred.secret_access_key(),
            ));
            b.set_host(MultiDomain::new(&[DOMAIN_NAME]).unwrap());
            b.build()
        };

        // Convert to aws http client
        let client = s3s_aws::Client::from(service.into_shared());

        // Setup aws sdk config
        let config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_client(client)
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
        println!("drop testcontext");
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

pub fn base64(input: impl AsRef<[u8]>) -> String {
    let base64 = base64_simd::STANDARD;
    base64.encode_to_string(input)
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_buckets() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await;

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

    list_buckets.buckets().iter().all(|list_bucket| {
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
    let context = TestContext::new(None).await;

    let bucket = format!("test-single-object-{}", Uuid::new_v4());
    let key = "sample.txt";
    let content = "hello world\n你好世界\n";
    let content_bytes = content.as_bytes();
    let mut md5_hash = Md5::new();
    md5_hash.update(content_bytes);
    let content_md5 = base64(md5_hash.finalize());

    // create_bucket
    create_bucket(&context, &bucket).await?;

    // put_object
    context
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(content_bytes))
        .content_md5(content_md5)
        .send()
        .await?;

    // get_object
    let get_object_output = context.get_object().bucket(&bucket).key(key).send().await?;
    let content_length = get_object_output.content_length().unwrap();
    let body = get_object_output.body.collect().await?.into_bytes();

    assert_eq!(content_length as usize, content.len());
    assert_eq!(body.as_ref(), content.as_bytes());

    // delete_object
    delete_object(&context, &bucket, key).await?;

    // println!("ok");
    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_multipart() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await;

    let bucket = format!("test-multipart-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let key = "sample.txt";
    let content = "abcdefghijklmnopqrstuvwxyz/0123456789/!@#$%^&*();\n";
    let content_bytes = content.as_bytes();
    let mut md5_hash = Md5::new();
    md5_hash.update(content_bytes);
    let content_md5 = base64(md5_hash.finalize());

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

                let content_length = get_object_output.content_length().unwrap();
                let body = get_object_output.body.collect().await?.into_bytes();

                assert_eq!(content_length as usize, content.len());
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

#[tokio::test]
#[tracing::instrument]
async fn test_read_only_object() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(Some(Config {
        read_only: true,
        ..Default::default()
    }))
    .await;

    let bucket = format!("test-single-object-{}", Uuid::new_v4());

    match create_bucket(&context, &bucket).await {
        Err(err)
            if err
                .root_cause()
                .to_string()
                .contains("database is in read-only mode") => {}
        other => panic!("{other:?}"),
    };

    Ok(())
}
