#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::expect_used, //
    clippy::must_use_candidate, //
    clippy::unwrap_used, //
)]

use std::{collections::HashMap, fs, ops::Deref, path::PathBuf, sync::LazyLock};

use anyhow::Result;
use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
    primitives::{ByteStream, DateTime},
    types::{
        BucketLocationConstraint, CompletedMultipartUpload, CompletedPart,
        CreateBucketConfiguration, Delete, EncodingType, ObjectIdentifier,
    },
};
use s3ite::{Bucket, Config, Sqlite};
use s3s::{auth::SimpleAuth, host::MultiDomain, service::S3ServiceBuilder};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, error};
use uuid::Uuid;

const FS_ROOT: &str = concat!(env!("CARGO_TARGET_TMPDIR"), "/s3s-sqlite-tests-aws");
const DOMAIN_NAME: &str = "localhost:8014";
const REGION: &str = "us-west-2";

static TRACING: LazyLock<()> = LazyLock::new(|| {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("it_aws=error,s3s_sqlite=debug,s3s=error"));

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_test_writer()
        .init();
});

pub struct TestContext {
    pub client: Client,
    root: PathBuf,
    cleanup: bool,
}

impl TestContext {
    pub async fn new(config: Option<Config>) -> Result<Self> {
        Self::new_with_root(config, PathBuf::from(FS_ROOT), true).await
    }

    pub async fn new_with_root(
        config: Option<Config>,
        root: PathBuf,
        cleanup: bool,
    ) -> Result<Self> {
        LazyLock::force(&TRACING);

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        fs::create_dir_all(&root)?;
        let mut config = config.unwrap_or_default();
        config.root = root.clone();

        let fs = Sqlite::new(&config).await?;

        // Setup S3 service
        let service = {
            let mut b = S3ServiceBuilder::new(fs);
            b.set_auth(SimpleAuth::from_single(
                cred.access_key_id(),
                cred.secret_access_key(),
            ));
            b.set_host(MultiDomain::new(&[DOMAIN_NAME])?);
            b.build()
        };

        // Convert to aws http client
        let client = s3s_aws::Client::from(service);

        // Setup aws sdk config
        let config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_client(client)
            .region(Region::new(REGION))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build();

        Ok(Self {
            client: Client::new(&config),
            root,
            cleanup,
        })
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
        if self.cleanup {
            fs::remove_dir_all(&self.root).ok();
        }
    }
}

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
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

async fn put_object(c: &Client, bucket: &str, key: &str, body: &'static [u8]) -> Result<()> {
    c.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    Ok(())
}

async fn get_object_bytes(c: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    Ok(c.get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes()
        .to_vec())
}

async fn get_object_part_bytes(
    c: &Client,
    bucket: &str,
    key: &str,
    part_number: i32,
) -> Result<Vec<u8>> {
    Ok(c.get_object()
        .bucket(bucket)
        .key(key)
        .part_number(part_number)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes()
        .to_vec())
}

async fn create_multipart_upload_id(c: &Client, bucket: &str, key: &str) -> Result<String> {
    c.create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))
}

fn object_identifier(key: &str) -> Result<ObjectIdentifier> {
    Ok(ObjectIdentifier::builder().key(key).build()?)
}

async fn upload_part(
    c: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: &'static [u8],
) -> Result<CompletedPart> {
    upload_part_body(
        c,
        bucket,
        key,
        upload_id,
        part_number,
        ByteStream::from_static(body),
    )
    .await
}

async fn upload_part_body(
    c: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: ByteStream,
) -> Result<CompletedPart> {
    let part = c
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(body)
        .send()
        .await?;

    Ok(CompletedPart::builder()
        .e_tag(
            part.e_tag
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(part_number)
        .build())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

fn assert_error_contains(error: impl std::fmt::Debug, text: &str) {
    let error = format!("{error:?}");
    assert!(error.contains(text), "expected {text:?} in {error}");
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_buckets() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

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
    match &result {
        Ok(ans) => debug!(?ans),
        Err(err) => error!(?err),
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
    let context = TestContext::new(None).await?;

    let bucket = format!("test-single-object-{}", Uuid::new_v4());
    let key = "sample.txt";
    let content = "hello world\n你好世界\n";
    let content_bytes = content.as_bytes();
    let content_md5 = "SpRKmvVRaPLiBjkHxCGwYQ==";
    let content_etag = "4a944a9af55168f2e2063907c421b061";

    // create_bucket
    create_bucket(&context, &bucket).await?;
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
    let content_length = get_object_output
        .content_length()
        .ok_or_else(|| anyhow::anyhow!("get object response missing content length"))?;
    let body = get_object_output.body.collect().await?.into_bytes();

    assert_eq!(content_length as usize, content.len());
    assert_eq!(body.as_ref(), content.as_bytes());

    // head_object
    let head_object_output = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(
        head_object_output
            .e_tag()
            .map(|etag| etag.trim_matches('"')),
        Some(content_etag)
    );

    // delete_object
    delete_object(&context, &bucket, key).await?;

    // println!("ok");
    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_get_object_ranges() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-ranges-{}", Uuid::new_v4());
    let key = "sample.txt";
    let content = b"abcdef";

    create_bucket(&context, &bucket).await?;
    context
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(content))
        .send()
        .await?;

    let full = context.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(full.accept_ranges(), Some("bytes"));
    assert_eq!(full.content_length(), Some(content.len() as i64));
    assert_eq!(
        full.body.collect().await?.into_bytes().as_ref(),
        content.as_slice()
    );

    let first = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=0-0")
        .send()
        .await?;
    assert_eq!(first.content_length(), Some(1));
    assert_eq!(first.content_range(), Some("bytes 0-0/6"));
    assert_eq!(first.body.collect().await?.into_bytes().as_ref(), b"a");

    let middle = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=1-3")
        .send()
        .await?;
    assert_eq!(middle.content_length(), Some(3));
    assert_eq!(middle.content_range(), Some("bytes 1-3/6"));
    assert_eq!(middle.body.collect().await?.into_bytes().as_ref(), b"bcd");

    let suffix = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=-2")
        .send()
        .await?;
    assert_eq!(suffix.content_length(), Some(2));
    assert_eq!(suffix.content_range(), Some("bytes 4-5/6"));
    assert_eq!(suffix.body.collect().await?.into_bytes().as_ref(), b"ef");

    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=99-100")
        .send()
        .await
    else {
        anyhow::bail!("unsatisfiable range should fail");
    };
    assert_error_contains(err, "InvalidRange");

    let empty_key = "empty.txt";
    context
        .put_object()
        .bucket(&bucket)
        .key(empty_key)
        .body(ByteStream::from_static(b""))
        .send()
        .await?;
    let empty = context
        .get_object()
        .bucket(&bucket)
        .key(empty_key)
        .send()
        .await?;
    assert_eq!(empty.content_length(), Some(0));
    assert!(empty.body.collect().await?.into_bytes().is_empty());

    delete_object(&context, &bucket, key).await?;
    delete_object(&context, &bucket, empty_key).await?;
    delete_bucket(&context, &bucket).await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_get_and_head_object_conditionals() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-read-conditions-{}", Uuid::new_v4());
    let key = "conditional.txt";

    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, key, b"conditional").await?;

    let head = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    let etag = head
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let last_modified = head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;
    let before = DateTime::from_secs(last_modified.secs() - 1);
    let after = DateTime::from_secs(last_modified.secs() + 1);

    context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .if_unmodified_since(before)
        .send()
        .await?;

    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match(&etag)
        .send()
        .await
    else {
        anyhow::bail!("matching If-None-Match should return NotModified");
    };
    assert_error_contains(err, "NotModified");

    let Err(err) = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await
    else {
        anyhow::bail!("mismatched If-Match should fail");
    };
    assert_error_contains(err, "PreconditionFailed");

    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .if_modified_since(after)
        .send()
        .await
    else {
        anyhow::bail!("unmodified object should return NotModified");
    };
    assert_error_contains(err, "NotModified");

    let Err(err) = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .if_unmodified_since(before)
        .send()
        .await
    else {
        anyhow::bail!("modified object should fail If-Unmodified-Since");
    };
    assert_error_contains(err, "PreconditionFailed");

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_put_object_conditionals_are_transactional() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-put-conditions-{}", Uuid::new_v4());
    let key = "conditional-put.txt";

    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, key, b"old").await?;

    let etag = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();

    let Err(err) = context
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    else {
        anyhow::bail!("If-None-Match should reject overwrite");
    };
    assert_error_contains(err, "PreconditionFailed");
    assert_eq!(get_object_bytes(&context, &bucket, key).await?, b"old");

    let Err(err) = context
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match("\"00000000000000000000000000000000\"")
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    else {
        anyhow::bail!("mismatched If-Match should reject overwrite");
    };
    assert_error_contains(err, "PreconditionFailed");
    assert_eq!(get_object_bytes(&context, &bucket, key).await?, b"old");

    context
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(etag)
        .body(ByteStream::from_static(b"new"))
        .send()
        .await?;
    assert_eq!(get_object_bytes(&context, &bucket, key).await?, b"new");

    context
        .put_object()
        .bucket(&bucket)
        .key("create-only.txt")
        .if_none_match("*")
        .body(ByteStream::from_static(b"created"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&context, &bucket, "create-only.txt").await?,
        b"created"
    );

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_copy_source_conditionals() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-copy-conditions-{}", Uuid::new_v4());
    let source = "source.txt";
    let target = "target.txt";

    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, source, b"copy-me").await?;

    let head = context
        .head_object()
        .bucket(&bucket)
        .key(source)
        .send()
        .await?;
    let etag = head
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let last_modified = head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;
    let before = DateTime::from_secs(last_modified.secs() - 1);
    let after = DateTime::from_secs(last_modified.secs() + 1);
    let copy_source = format!("{bucket}/{source}");

    let Err(err) = context
        .copy_object()
        .bucket(&bucket)
        .key(target)
        .copy_source(&copy_source)
        .copy_source_if_match("\"00000000000000000000000000000000\"")
        .send()
        .await
    else {
        anyhow::bail!("mismatched copy source If-Match should fail");
    };
    assert_error_contains(err, "PreconditionFailed");

    let Err(err) = context
        .copy_object()
        .bucket(&bucket)
        .key(target)
        .copy_source(&copy_source)
        .copy_source_if_none_match(&etag)
        .send()
        .await
    else {
        anyhow::bail!("matching copy source If-None-Match should fail");
    };
    assert_error_contains(err, "PreconditionFailed");

    let Err(err) = context
        .copy_object()
        .bucket(&bucket)
        .key(target)
        .copy_source(&copy_source)
        .copy_source_if_modified_since(after)
        .send()
        .await
    else {
        anyhow::bail!("unmodified copy source should fail If-Modified-Since");
    };
    assert_error_contains(err, "PreconditionFailed");

    context
        .copy_object()
        .bucket(&bucket)
        .key(target)
        .copy_source(&copy_source)
        .copy_source_if_match(etag)
        .copy_source_if_unmodified_since(before)
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&context, &bucket, target).await?,
        b"copy-me"
    );

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_delete_object_conditionals() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-delete-conditions-{}", Uuid::new_v4());
    let key = "delete.txt";

    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, key, b"delete-me").await?;

    let head = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    let etag = head
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let last_modified = head
        .last_modified()
        .ok_or_else(|| anyhow::anyhow!("head object response missing Last-Modified"))?;

    let Err(err) = context
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await
    else {
        anyhow::bail!("mismatched delete If-Match should fail");
    };
    assert_error_contains(err, "PreconditionFailed");
    assert_eq!(
        get_object_bytes(&context, &bucket, key).await?,
        b"delete-me"
    );

    let Err(err) = context
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match_size(999)
        .send()
        .await
    else {
        anyhow::bail!("mismatched delete size should fail");
    };
    assert_error_contains(err, "PreconditionFailed");

    context
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .if_match(etag)
        .if_match_last_modified_time(*last_modified)
        .if_match_size(9)
        .send()
        .await?;

    context
        .delete_object()
        .bucket(&bucket)
        .key("missing.txt")
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_delete_objects_conditionals_report_per_key_errors() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-del-conds-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, "keep.txt", b"keep").await?;
    put_object(&context, &bucket, "delete.txt", b"delete").await?;

    let delete_etag = context
        .head_object()
        .bucket(&bucket)
        .key("delete.txt")
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();

    let delete = Delete::builder()
        .objects(
            ObjectIdentifier::builder()
                .key("keep.txt")
                .e_tag("\"00000000000000000000000000000000\"")
                .build()?,
        )
        .objects(
            ObjectIdentifier::builder()
                .key("delete.txt")
                .e_tag(delete_etag)
                .build()?,
        )
        .build()?;

    let output = context
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    assert_eq!(output.deleted().len(), 1);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(
        get_object_bytes(&context, &bucket, "keep.txt").await?,
        b"keep"
    );

    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key("delete.txt")
        .send()
        .await
    else {
        anyhow::bail!("conditionally deleted object should be gone");
    };
    assert_error_contains(err, "NoSuchKey");

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_objects_prefix_delimiter_and_pagination() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-list-objects-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let keys = [
        "literal/%/one",
        "literal/_/two",
        "root/%/one",
        "root/_/two",
        "root/a/one",
        "root/a/two",
        "root/file",
        "alt:a:x",
        "alt:a:y",
        "alt:b",
        "page/1",
        "page/2",
        "page/3",
    ];
    for key in keys {
        put_object(&context, &bucket, key, b"x").await?;
    }

    let percent = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("literal/%")
        .send()
        .await?;
    let percent_keys = percent
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(percent_keys, vec!["literal/%/one"]);

    let underscore = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("literal/_")
        .send()
        .await?;
    let underscore_keys = underscore
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(underscore_keys, vec!["literal/_/two"]);

    let root = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("root/")
        .delimiter("/")
        .send()
        .await?;
    let root_prefixes = root
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    let root_keys = root
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(root_prefixes, vec!["root/%/", "root/_/", "root/a/"]);
    assert_eq!(root_keys, vec!["root/file"]);
    assert_eq!(root.key_count(), Some(4));

    let alternate = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("alt:")
        .delimiter(":")
        .send()
        .await?;
    let alternate_prefixes = alternate
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    let alternate_keys = alternate
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(alternate_prefixes, vec!["alt:a:"]);
    assert_eq!(alternate_keys, vec!["alt:b"]);
    assert_eq!(alternate.key_count(), Some(2));

    let first_page = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("page/")
        .max_keys(2)
        .send()
        .await?;
    assert_eq!(first_page.key_count(), Some(2));
    assert_eq!(first_page.is_truncated(), Some(true));
    let first_page_keys = first_page
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(first_page_keys, vec!["page/1", "page/2"]);

    let second_page = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("page/")
        .continuation_token(
            first_page
                .next_continuation_token()
                .ok_or_else(|| anyhow::anyhow!("first page missing continuation token"))?,
        )
        .send()
        .await?;
    let second_page_keys = second_page
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(second_page_keys, vec!["page/3"]);
    assert_eq!(second_page.is_truncated(), Some(false));

    let v1_first_page = context
        .list_objects()
        .bucket(&bucket)
        .prefix("page/")
        .max_keys(1)
        .send()
        .await?;
    assert_eq!(v1_first_page.next_marker(), Some("page/1"));

    let v1_marker = context
        .list_objects()
        .bucket(&bucket)
        .prefix("page/")
        .marker("page/1")
        .send()
        .await?;
    let v1_marker_keys = v1_marker
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(v1_marker_keys, vec!["page/2", "page/3"]);

    for key in keys {
        delete_object(&context, &bucket, key).await?;
    }
    delete_bucket(&context, &bucket).await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_objects_ceph_edge_semantics() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-list-ceph-edges-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    for key in ["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"] {
        put_object(&context, &bucket, key, b"x").await?;
    }

    let encoded_v2 = context
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .encoding_type(EncodingType::Url)
        .send()
        .await?;
    let encoded_v2_keys = encoded_v2
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    let encoded_v2_prefixes = encoded_v2
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    assert_eq!(encoded_v2.delimiter(), Some("/"));
    assert_eq!(encoded_v2_keys, vec!["asdf%2Bb"]);
    assert_eq!(encoded_v2_prefixes, vec!["foo%2B1/", "foo/", "quux%20ab/"]);

    let encoded_v1 = context
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .encoding_type(EncodingType::Url)
        .send()
        .await?;
    let encoded_v1_keys = encoded_v1
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    let encoded_v1_prefixes = encoded_v1
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    assert_eq!(encoded_v1.delimiter(), Some("/"));
    assert_eq!(encoded_v1_keys, vec!["asdf%2Bb"]);
    assert_eq!(encoded_v1_prefixes, vec!["foo%2B1/", "foo/", "quux%20ab/"]);

    for key in ["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"] {
        delete_object(&context, &bucket, key).await?;
    }
    for key in [
        "asdf",
        "boo/bar",
        "boo/baz/xyzzy",
        "cquux/thud",
        "cquux/bla",
    ] {
        put_object(&context, &bucket, key, b"x").await?;
    }

    let v1_first = context
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .max_keys(1)
        .send()
        .await?;
    assert_eq!(v1_first.next_marker(), Some("asdf"));
    let v1_second = context
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .marker(
            v1_first
                .next_marker()
                .ok_or_else(|| anyhow::anyhow!("first v1 page missing next marker"))?,
        )
        .max_keys(1)
        .send()
        .await?;
    assert_eq!(v1_second.next_marker(), Some("boo/"));
    let v1_third = context
        .list_objects()
        .bucket(&bucket)
        .delimiter("/")
        .marker(
            v1_second
                .next_marker()
                .ok_or_else(|| anyhow::anyhow!("second v1 page missing next marker"))?,
        )
        .max_keys(1)
        .send()
        .await?;
    let v1_third_prefixes = v1_third
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    assert_eq!(v1_third_prefixes, vec!["cquux/"]);
    assert_eq!(v1_third.is_truncated(), Some(false));
    assert_eq!(v1_third.next_marker(), None);

    let v2_first = context
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .max_keys(1)
        .send()
        .await?;
    let v2_second = context
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .continuation_token(
            v2_first
                .next_continuation_token()
                .ok_or_else(|| anyhow::anyhow!("first v2 page missing continuation token"))?,
        )
        .max_keys(1)
        .send()
        .await?;
    let v2_third = context
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .continuation_token(
            v2_second
                .next_continuation_token()
                .ok_or_else(|| anyhow::anyhow!("second v2 page missing continuation token"))?,
        )
        .max_keys(1)
        .send()
        .await?;
    let v2_third_prefixes = v2_third
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    assert_eq!(v2_third_prefixes, vec!["cquux/"]);
    assert_eq!(v2_third.is_truncated(), Some(false));

    let empty_delimiter = context
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("")
        .send()
        .await?;
    assert_eq!(empty_delimiter.delimiter(), None);

    let empty_token = context
        .list_objects_v2()
        .bucket(&bucket)
        .continuation_token("")
        .send()
        .await?;
    assert_eq!(empty_token.continuation_token(), Some(""));

    let fetch_owner = context
        .list_objects_v2()
        .bucket(&bucket)
        .fetch_owner(true)
        .send()
        .await?;
    assert!(
        fetch_owner
            .contents()
            .iter()
            .all(|object| object.owner().is_some())
    );

    for key in [
        "asdf",
        "boo/bar",
        "boo/baz/xyzzy",
        "cquux/thud",
        "cquux/bla",
    ] {
        delete_object(&context, &bucket, key).await?;
    }
    delete_bucket(&context, &bucket).await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_object_versions_for_unversioned_bucket() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-list-object-versions-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let keys = ["version/a", "version/b", "version/c", "version/dir/child"];
    for key in keys {
        put_object(&context, &bucket, key, b"x").await?;
    }

    let first_page = context
        .list_object_versions()
        .bucket(&bucket)
        .prefix("version/")
        .max_keys(2)
        .send()
        .await?;
    let first_page_keys = first_page
        .versions()
        .iter()
        .filter_map(|version| version.key())
        .collect::<Vec<_>>();
    assert_eq!(first_page_keys, vec!["version/a", "version/b"]);
    assert_eq!(first_page.is_truncated(), Some(true));
    assert_eq!(first_page.next_key_marker(), Some("version/b"));
    assert_eq!(first_page.next_version_id_marker(), Some("null"));
    assert!(
        first_page
            .versions()
            .iter()
            .all(|version| version.is_latest().unwrap_or(false))
    );
    assert!(
        first_page
            .versions()
            .iter()
            .all(|version| version.version_id() == Some("null"))
    );

    let second_page =
        context
            .list_object_versions()
            .bucket(&bucket)
            .prefix("version/")
            .key_marker(
                first_page
                    .next_key_marker()
                    .ok_or_else(|| anyhow::anyhow!("first version page missing next key marker"))?,
            )
            .version_id_marker(first_page.next_version_id_marker().ok_or_else(|| {
                anyhow::anyhow!("first version page missing next version ID marker")
            })?)
            .send()
            .await?;
    let second_page_keys = second_page
        .versions()
        .iter()
        .filter_map(|version| version.key())
        .collect::<Vec<_>>();
    assert_eq!(second_page_keys, vec!["version/c", "version/dir/child"]);
    assert_eq!(second_page.is_truncated(), Some(false));

    let grouped = context
        .list_object_versions()
        .bucket(&bucket)
        .prefix("version/")
        .delimiter("/")
        .send()
        .await?;
    let grouped_prefixes = grouped
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    let grouped_keys = grouped
        .versions()
        .iter()
        .filter_map(|version| version.key())
        .collect::<Vec<_>>();
    assert_eq!(grouped_prefixes, vec!["version/dir/"]);
    assert_eq!(grouped_keys, vec!["version/a", "version/b", "version/c"]);

    for key in keys {
        delete_object(&context, &bucket, key).await?;
    }
    delete_bucket(&context, &bucket).await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_delete_and_trailing_slash_conformance() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-delete-conformance-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    put_object(&context, &bucket, "folder/", b"folder/").await?;
    let folder = context
        .get_object()
        .bucket(&bucket)
        .key("folder/")
        .send()
        .await?;
    assert_eq!(
        folder.body.collect().await?.into_bytes().as_ref(),
        b"folder/"
    );

    let Err(err) = context.delete_bucket().bucket(&bucket).send().await else {
        anyhow::bail!("deleting a non-empty bucket should fail");
    };
    assert_error_contains(err, "BucketNotEmpty");

    let delete = Delete::builder()
        .objects(object_identifier("folder/")?)
        .objects(object_identifier("missing")?)
        .build()?;
    let deleted = context
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    let deleted_keys = deleted
        .deleted()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    assert_eq!(deleted_keys, vec!["folder/", "missing"]);

    let delete_again = Delete::builder()
        .objects(object_identifier("folder/")?)
        .objects(object_identifier("missing")?)
        .build()?;
    let deleted_again = context
        .delete_objects()
        .bucket(&bucket)
        .delete(delete_again)
        .send()
        .await?;
    assert_eq!(deleted_again.deleted().len(), 2);

    let too_many = (0..=1000)
        .map(|index| object_identifier(&format!("key-{index}")))
        .collect::<Result<Vec<_>>>()?;
    let too_many = Delete::builder().set_objects(Some(too_many)).build()?;
    let Err(err) = context
        .delete_objects()
        .bucket(&bucket)
        .delete(too_many)
        .send()
        .await
    else {
        anyhow::bail!("deleting too many objects should fail");
    };
    assert_error_contains(err, "MalformedXML");

    delete_bucket(&context, &bucket).await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_multipart() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-multipart-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let key = "sample.txt";
    let content = "abcdefghijklmnopqrstuvwxyz/0123456789/!@#$%^&*();\n";
    let content_bytes = content.as_bytes();
    let content_md5 = "H6dHQaYjxrasUfXrWk55IQ==";

    let upload_id = {
        let result = context
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        match &result {
            Ok(ans) => debug!(?ans),
            Err(err) => error!(?err),
        }
        result?
            .upload_id
            .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
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
            Ok(ans) => debug!(?ans),
            Err(err) => error!(?err),
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

        context
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

                let content_length = get_object_output
                    .content_length()
                    .ok_or_else(|| anyhow::anyhow!("get object response missing content length"))?;
                let body = get_object_output.body.collect().await?.into_bytes();

                assert_eq!(content_length as usize, content.len());
                assert_eq!(body.as_ref(), content.as_bytes());
            }
            Err(err) => error!(?err),
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
async fn test_multipart_completion_validation_and_etag() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-multipart-validation-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let small_key = "small.txt";
    let small_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(small_key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let small_part1 =
        upload_part(&context, &bucket, small_key, &small_upload_id, 1, b"one").await?;
    let small_part2 =
        upload_part(&context, &bucket, small_key, &small_upload_id, 2, b"two").await?;
    let small_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![small_part1, small_part2]))
        .build();
    let Err(err) = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(small_key)
        .upload_id(&small_upload_id)
        .multipart_upload(small_upload)
        .send()
        .await
    else {
        anyhow::bail!("small multipart completion should fail");
    };
    assert_error_contains(err, "EntityTooSmall");

    let key = "valid.txt";
    let upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let first_part = vec![b'a'; 5 * 1024 * 1024];
    let mut expected = first_part.clone();
    expected.extend_from_slice(b"world");
    let part1 = upload_part_body(
        &context,
        &bucket,
        key,
        &upload_id,
        1,
        ByteStream::from(first_part),
    )
    .await?;
    let part2 = upload_part(&context, &bucket, key, &upload_id, 2, b"world").await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part1.clone(), part2.clone()]))
        .build();
    let complete = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(upload)
        .send()
        .await?;
    let complete_etag = complete
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("complete multipart response missing ETag"))?;
    assert!(complete_etag.trim_matches('"').ends_with("-2"));

    let object = context.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        object.body.collect().await?.into_bytes().as_ref(),
        expected.as_slice()
    );

    let missing_key = "missing.txt";
    let missing_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(missing_key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let missing_part1 = upload_part(
        &context,
        &bucket,
        missing_key,
        &missing_upload_id,
        1,
        b"one",
    )
    .await?;
    let _missing_part2 = upload_part(
        &context,
        &bucket,
        missing_key,
        &missing_upload_id,
        2,
        b"two",
    )
    .await?;
    let missing_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![missing_part1]))
        .build();
    let Err(err) = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(missing_key)
        .upload_id(&missing_upload_id)
        .multipart_upload(missing_upload)
        .send()
        .await
    else {
        anyhow::bail!("completion with missing part should fail");
    };
    assert_error_contains(err, "InvalidPart");

    let wrong_key = "wrong-etag.txt";
    let wrong_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(wrong_key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let _wrong_part =
        upload_part(&context, &bucket, wrong_key, &wrong_upload_id, 1, b"one").await?;
    let wrong_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![
            CompletedPart::builder()
                .part_number(1)
                .e_tag("00000000000000000000000000000000")
                .build(),
        ]))
        .build();
    let Err(err) = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(wrong_key)
        .upload_id(&wrong_upload_id)
        .multipart_upload(wrong_upload)
        .send()
        .await
    else {
        anyhow::bail!("completion with wrong ETag should fail");
    };
    assert_error_contains(err, "InvalidPart");

    let order_key = "out-of-order.txt";
    let order_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(order_key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let order_part1 =
        upload_part(&context, &bucket, order_key, &order_upload_id, 1, b"one").await?;
    let order_part2 =
        upload_part(&context, &bucket, order_key, &order_upload_id, 2, b"two").await?;
    let order_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![order_part2, order_part1]))
        .build();
    let Err(err) = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(order_key)
        .upload_id(&order_upload_id)
        .multipart_upload(order_upload)
        .send()
        .await
    else {
        anyhow::bail!("out-of-order multipart completion should fail");
    };
    assert_error_contains(err, "InvalidPartOrder");

    delete_object(&context, &bucket, key).await?;
    delete_bucket(&context, &bucket).await?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_complete_multipart_upload_conditionals() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-multipart-conditions-{}", Uuid::new_v4());
    let key = "conditional-multipart.txt";

    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, key, b"old").await?;
    let old_etag = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();

    let blocked_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let blocked_first = vec![b'a'; 5 * 1024 * 1024];
    let blocked_part1 = upload_part_body(
        &context,
        &bucket,
        key,
        &blocked_upload_id,
        1,
        ByteStream::from(blocked_first),
    )
    .await?;
    let blocked_part2 =
        upload_part(&context, &bucket, key, &blocked_upload_id, 2, b"blocked").await?;
    let blocked_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![blocked_part1, blocked_part2]))
        .build();
    let Err(err) = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&blocked_upload_id)
        .multipart_upload(blocked_upload)
        .if_none_match("*")
        .send()
        .await
    else {
        anyhow::bail!("If-None-Match should reject multipart overwrite");
    };
    assert_error_contains(err, "PreconditionFailed");
    assert_eq!(get_object_bytes(&context, &bucket, key).await?, b"old");

    let upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    let first_part = vec![b'b'; 5 * 1024 * 1024];
    let mut expected = first_part.clone();
    expected.extend_from_slice(b"new");
    let part1 = upload_part_body(
        &context,
        &bucket,
        key,
        &upload_id,
        1,
        ByteStream::from(first_part),
    )
    .await?;
    let part2 = upload_part(&context, &bucket, key, &upload_id, 2, b"new").await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part1, part2]))
        .build();
    context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(upload)
        .if_match(old_etag)
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&context, &bucket, key).await?,
        expected.as_slice()
    );

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_parts_pagination_etags_and_upload_id_errors() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-list-parts-{}", Uuid::new_v4());
    let key = "parts.txt";
    create_bucket(&context, &bucket).await?;

    let upload_id = create_multipart_upload_id(&context, &bucket, key).await?;
    let part1 = upload_part(&context, &bucket, key, &upload_id, 1, b"one").await?;
    let part2 = upload_part(&context, &bucket, key, &upload_id, 2, b"two").await?;
    let part3 = upload_part(&context, &bucket, key, &upload_id, 3, b"three").await?;

    let first = context
        .list_parts()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .max_parts(2)
        .send()
        .await?;
    assert_eq!(first.parts().len(), 2);
    assert_eq!(first.is_truncated(), Some(true));
    assert_eq!(first.next_part_number_marker(), Some("2"));
    assert_eq!(first.parts()[0].part_number(), Some(1));
    assert_eq!(first.parts()[0].e_tag(), part1.e_tag());
    assert_eq!(first.parts()[1].part_number(), Some(2));
    assert_eq!(first.parts()[1].e_tag(), part2.e_tag());

    let second = context
        .list_parts()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number_marker("2")
        .send()
        .await?;
    assert_eq!(second.parts().len(), 1);
    assert_eq!(second.is_truncated(), Some(false));
    assert_eq!(second.parts()[0].part_number(), Some(3));
    assert_eq!(second.parts()[0].e_tag(), part3.e_tag());

    let Err(err) = context
        .list_parts()
        .bucket(&bucket)
        .key(key)
        .upload_id("not-a-uuid")
        .send()
        .await
    else {
        anyhow::bail!("malformed upload ID should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    let Err(err) = context
        .list_parts()
        .bucket(&bucket)
        .key(key)
        .upload_id(Uuid::new_v4().to_string())
        .send()
        .await
    else {
        anyhow::bail!("missing upload ID should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_multipart_uploads_prefix_delimiter_and_pagination() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-list-uploads-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let page_a = create_multipart_upload_id(&context, &bucket, "page/a").await?;
    let _page_b = create_multipart_upload_id(&context, &bucket, "page/b").await?;
    let aborted = create_multipart_upload_id(&context, &bucket, "page/aborted").await?;
    context
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("page/aborted")
        .upload_id(aborted)
        .send()
        .await?;

    let completed = create_multipart_upload_id(&context, &bucket, "page/completed").await?;
    let completed_part =
        upload_part(&context, &bucket, "page/completed", &completed, 1, b"done").await?;
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![completed_part]))
        .build();
    context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("page/completed")
        .upload_id(&completed)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    let first = context
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("page/")
        .max_uploads(1)
        .send()
        .await?;
    assert_eq!(first.uploads().len(), 1);
    assert_eq!(first.uploads()[0].key(), Some("page/a"));
    assert_eq!(first.uploads()[0].upload_id(), Some(page_a.as_str()));
    assert_eq!(first.is_truncated(), Some(true));

    let second = context
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("page/")
        .key_marker(
            first
                .next_key_marker()
                .ok_or_else(|| anyhow::anyhow!("first upload page missing key marker"))?,
        )
        .upload_id_marker(
            first
                .next_upload_id_marker()
                .ok_or_else(|| anyhow::anyhow!("first upload page missing upload ID marker"))?,
        )
        .send()
        .await?;
    let second_keys = second
        .uploads()
        .iter()
        .filter_map(|upload| upload.key())
        .collect::<Vec<_>>();
    assert_eq!(second_keys, vec!["page/b"]);
    assert_eq!(second.is_truncated(), Some(false));

    create_multipart_upload_id(&context, &bucket, "group/a/one").await?;
    create_multipart_upload_id(&context, &bucket, "group/b/two").await?;
    create_multipart_upload_id(&context, &bucket, "group/root").await?;
    let grouped = context
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("group/")
        .delimiter("/")
        .send()
        .await?;
    let grouped_prefixes = grouped
        .common_prefixes()
        .iter()
        .filter_map(|common_prefix| common_prefix.prefix())
        .collect::<Vec<_>>();
    let grouped_uploads = grouped
        .uploads()
        .iter()
        .filter_map(|upload| upload.key())
        .collect::<Vec<_>>();
    assert_eq!(grouped_prefixes, vec!["group/a/", "group/b/"]);
    assert_eq!(grouped_uploads, vec!["group/root"]);

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_upload_part_copy_full_range_conditions_and_missing_upload() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let source_bucket = format!("test-copy-part-src-{}", Uuid::new_v4());
    let target_bucket = format!("test-copy-part-dst-{}", Uuid::new_v4());
    let source = "source.txt";
    create_bucket(&context, &source_bucket).await?;
    create_bucket(&context, &target_bucket).await?;
    put_object(&context, &source_bucket, source, b"0123456789abcdef").await?;
    let source_etag = context
        .head_object()
        .bucket(&source_bucket)
        .key(source)
        .send()
        .await?
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("head object response missing ETag"))?
        .to_string();
    let copy_source = format!("{source_bucket}/{source}");

    let full_key = "full-copy.txt";
    let full_upload = create_multipart_upload_id(&context, &target_bucket, full_key).await?;
    let full_copy = context
        .upload_part_copy()
        .bucket(&target_bucket)
        .key(full_key)
        .upload_id(&full_upload)
        .part_number(1)
        .copy_source(&copy_source)
        .send()
        .await?;
    let full_part = CompletedPart::builder()
        .part_number(1)
        .e_tag(
            full_copy
                .copy_part_result()
                .and_then(|result| result.e_tag())
                .ok_or_else(|| anyhow::anyhow!("upload part copy response missing ETag"))?,
        )
        .build();
    let full_complete = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![full_part]))
        .build();
    context
        .complete_multipart_upload()
        .bucket(&target_bucket)
        .key(full_key)
        .upload_id(&full_upload)
        .multipart_upload(full_complete)
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&context, &target_bucket, full_key).await?,
        b"0123456789abcdef"
    );

    let range_key = "range-copy.txt";
    let range_upload = create_multipart_upload_id(&context, &target_bucket, range_key).await?;
    let range_copy = context
        .upload_part_copy()
        .bucket(&target_bucket)
        .key(range_key)
        .upload_id(&range_upload)
        .part_number(1)
        .copy_source(&copy_source)
        .copy_source_range("bytes=2-5")
        .copy_source_if_match(source_etag.clone())
        .send()
        .await?;
    let range_part = CompletedPart::builder()
        .part_number(1)
        .e_tag(
            range_copy
                .copy_part_result()
                .and_then(|result| result.e_tag())
                .ok_or_else(|| anyhow::anyhow!("upload part copy response missing ETag"))?,
        )
        .build();
    let range_complete = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![range_part]))
        .build();
    context
        .complete_multipart_upload()
        .bucket(&target_bucket)
        .key(range_key)
        .upload_id(&range_upload)
        .multipart_upload(range_complete)
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&context, &target_bucket, range_key).await?,
        b"2345"
    );

    let invalid_range_upload =
        create_multipart_upload_id(&context, &target_bucket, "invalid-range.txt").await?;
    let Err(err) = context
        .upload_part_copy()
        .bucket(&target_bucket)
        .key("invalid-range.txt")
        .upload_id(&invalid_range_upload)
        .part_number(1)
        .copy_source(&copy_source)
        .copy_source_range("bytes=0-99")
        .send()
        .await
    else {
        anyhow::bail!("copy source range beyond the source object should fail");
    };
    assert_error_contains(err, "InvalidRange");

    let blocked_upload =
        create_multipart_upload_id(&context, &target_bucket, "blocked.txt").await?;
    let Err(err) = context
        .upload_part_copy()
        .bucket(&target_bucket)
        .key("blocked.txt")
        .upload_id(&blocked_upload)
        .part_number(1)
        .copy_source(&copy_source)
        .copy_source_if_none_match(source_etag)
        .send()
        .await
    else {
        anyhow::bail!("matching copy source If-None-Match should fail");
    };
    assert_error_contains(err, "PreconditionFailed");

    let Err(err) = context
        .upload_part_copy()
        .bucket(&target_bucket)
        .key("missing-upload.txt")
        .upload_id(Uuid::new_v4().to_string())
        .part_number(1)
        .copy_source(&copy_source)
        .send()
        .await
    else {
        anyhow::bail!("copying into a missing upload should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_get_object_part_number_and_boundary_cleanup() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-get-object-part-{}", Uuid::new_v4());
    let key = "multipart-object.txt";
    create_bucket(&context, &bucket).await?;

    let upload_id = create_multipart_upload_id(&context, &bucket, key).await?;
    let first_part = vec![b'a'; 5 * 1024 * 1024];
    let part1 = upload_part_body(
        &context,
        &bucket,
        key,
        &upload_id,
        1,
        ByteStream::from(first_part.clone()),
    )
    .await?;
    let part2 = upload_part(&context, &bucket, key, &upload_id, 2, b"tail").await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part1, part2]))
        .build();
    context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(upload)
        .send()
        .await?;

    assert_eq!(
        get_object_part_bytes(&context, &bucket, key, 1).await?,
        first_part
    );
    let head_first_part = context
        .head_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(head_first_part.parts_count(), Some(2));
    assert_eq!(
        head_first_part.content_length(),
        Some(i64::try_from(first_part.len())?)
    );
    let get_first_part = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(get_first_part.parts_count(), Some(2));
    assert_eq!(
        get_object_part_bytes(&context, &bucket, key, 2).await?,
        b"tail"
    );
    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(3)
        .send()
        .await
    else {
        anyhow::bail!("missing completed part should fail");
    };
    assert_error_contains(err, "InvalidPart");

    put_object(&context, &bucket, key, b"plain").await?;
    assert_eq!(
        get_object_part_bytes(&context, &bucket, key, 1).await?,
        b"plain"
    );
    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(2)
        .send()
        .await
    else {
        anyhow::bail!("PutObject should clear completed part boundaries");
    };
    assert_error_contains(err, "InvalidPart");

    put_object(&context, &bucket, "copy-source.txt", b"copied").await?;
    context
        .copy_object()
        .bucket(&bucket)
        .key(key)
        .copy_source(format!("{bucket}/copy-source.txt"))
        .send()
        .await?;
    assert_eq!(
        get_object_part_bytes(&context, &bucket, key, 1).await?,
        b"copied"
    );
    let Err(err) = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=0-0")
        .part_number(1)
        .send()
        .await
    else {
        anyhow::bail!("Range and partNumber together should fail");
    };
    assert_error_contains(err, "InvalidRequest");

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_invalid_upload_ids_are_no_such_upload() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-upload-id-errors-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;
    put_object(&context, &bucket, "source.txt", b"source").await?;

    let Err(err) = context
        .upload_part()
        .bucket(&bucket)
        .key("invalid-upload-part.txt")
        .upload_id("not-a-uuid")
        .part_number(1)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await
    else {
        anyhow::bail!("uploading with malformed upload ID should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    let invalid_complete = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag("00000000000000000000000000000000")
                .build(),
        )
        .build();
    let Err(err) = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("invalid-complete.txt")
        .upload_id("not-a-uuid")
        .multipart_upload(invalid_complete)
        .send()
        .await
    else {
        anyhow::bail!("completing with malformed upload ID should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    let Err(err) = context
        .upload_part_copy()
        .bucket(&bucket)
        .key("invalid-copy.txt")
        .upload_id("not-a-uuid")
        .part_number(1)
        .copy_source(format!("{bucket}/source.txt"))
        .send()
        .await
    else {
        anyhow::bail!("copying with malformed upload ID should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_abort_multipart_upload() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await?;

    let bucket = format!("test-multipart-abort-{}", Uuid::new_v4());
    let key = "aborted.txt";
    create_bucket(&context, &bucket).await?;

    let upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?;
    upload_part(&context, &bucket, key, &upload_id, 1, b"unused").await?;

    context
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    let objects = context
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(key)
        .send()
        .await?;
    assert!(objects.contents().is_empty());

    let Err(err) = context
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id("56788")
        .send()
        .await
    else {
        anyhow::bail!("aborting an invalid upload should fail");
    };
    assert_error_contains(err, "NoSuchUpload");

    delete_bucket(&context, &bucket).await?;

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
    .await?;

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

#[tokio::test]
#[tracing::instrument]
async fn test_zero_readers_fails_startup() -> Result<()> {
    let _guard = serial().await;
    let root = PathBuf::from(format!("{FS_ROOT}-zero-readers-{}", Uuid::new_v4()));
    fs::create_dir_all(&root)?;

    let Err(err) = Sqlite::new(&Config {
        root: root.clone(),
        readers: 0,
        ..Default::default()
    })
    .await
    else {
        anyhow::bail!("zero readers should fail startup");
    };
    assert_error_contains(err, "readers must be greater than zero");

    fs::remove_dir_all(root)?;

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_bucket_read_only_blocks_delete_and_upload_part() -> Result<()> {
    let _guard = serial().await;
    let root = PathBuf::from(format!("{FS_ROOT}-readonly-{}", Uuid::new_v4()));
    let bucket = format!("test-read-only-bucket-{}", Uuid::new_v4());
    let key = "sample.txt";
    let content = b"read-only data";

    let upload_id = {
        let context = TestContext::new_with_root(None, root.clone(), false).await?;
        create_bucket(&context, &bucket).await?;
        context
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(content))
            .send()
            .await?;
        context
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart.txt")
            .send()
            .await?
            .upload_id
            .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
    };

    let mut buckets = HashMap::new();
    buckets.insert(
        bucket.clone(),
        Bucket {
            read_only: Some(true),
            sqlite: None,
        },
    );
    let context = TestContext::new_with_root(
        Some(Config {
            buckets,
            ..Default::default()
        }),
        root,
        true,
    )
    .await?;

    let Err(err) = context
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
    else {
        anyhow::bail!("deleting from a read-only bucket should fail");
    };
    assert_error_contains(err, "database is in read-only mode");

    let Err(err) = context
        .upload_part()
        .bucket(&bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
    else {
        anyhow::bail!("uploading a part to a read-only bucket should fail");
    };
    assert_error_contains(err, "database is in read-only mode");

    let object = context.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(object.body.collect().await?.into_bytes().as_ref(), content);

    Ok(())
}
