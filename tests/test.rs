#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

use std::{collections::HashMap, env, fs, ops::Deref, path::PathBuf};

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
use s3ite::{Bucket, Config, Sqlite};
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
    root: PathBuf,
    cleanup: bool,
}

impl TestContext {
    pub async fn new(config: Option<Config>) -> Self {
        Self::new_with_root(config, PathBuf::from(FS_ROOT), true).await
    }

    pub async fn new_with_root(config: Option<Config>, root: PathBuf, cleanup: bool) -> Self {
        Lazy::force(&TRACING);

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        fs::create_dir_all(&root).unwrap();
        let mut config = config.unwrap_or_default();
        config.root = root.clone();

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
        let client = s3s_aws::Client::from(service);

        // Setup aws sdk config
        let config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_client(client)
            .region(Region::new(REGION))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build();

        Self {
            client: Client::new(&config),
            root,
            cleanup,
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
        if self.cleanup {
            fs::remove_dir_all(&self.root).unwrap();
        }
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

async fn put_object(c: &Client, bucket: &str, key: &str, body: &'static [u8]) -> Result<()> {
    c.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    Ok(())
}

async fn upload_part(
    c: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: &'static [u8],
) -> Result<CompletedPart> {
    let part = c
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from_static(body))
        .send()
        .await?;

    Ok(CompletedPart::builder()
        .e_tag(part.e_tag.unwrap())
        .part_number(part_number)
        .build())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

pub fn base64(input: impl AsRef<[u8]>) -> String {
    let base64 = base64_simd::STANDARD;
    base64.encode_to_string(input)
}

fn assert_error_contains(error: impl std::fmt::Debug, text: &str) {
    let error = format!("{error:?}");
    assert!(error.contains(text), "expected {text:?} in {error}");
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
async fn test_get_object_ranges() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await;

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

    let err = context
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=99-100")
        .send()
        .await
        .unwrap_err();
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
async fn test_list_objects_prefix_delimiter_and_pagination() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await;

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
        .continuation_token(first_page.next_continuation_token().unwrap())
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
async fn test_list_object_versions_for_unversioned_bucket() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await;

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
    assert!(first_page
        .versions()
        .iter()
        .all(|version| version.is_latest().unwrap_or(false)));
    assert!(first_page
        .versions()
        .iter()
        .all(|version| version.version_id() == Some("null")));

    let second_page = context
        .list_object_versions()
        .bucket(&bucket)
        .prefix("version/")
        .key_marker(first_page.next_key_marker().unwrap())
        .version_id_marker(first_page.next_version_id_marker().unwrap())
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
async fn test_multipart_completion_validation_and_etag() -> Result<()> {
    let _guard = serial().await;
    let context = TestContext::new(None).await;

    let bucket = format!("test-multipart-validation-{}", Uuid::new_v4());
    create_bucket(&context, &bucket).await?;

    let key = "valid.txt";
    let upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .unwrap();
    let part1 = upload_part(&context, &bucket, key, &upload_id, 1, b"hello ").await?;
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
    assert!(complete.e_tag().unwrap().trim_matches('"').ends_with("-2"));

    let object = context.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(
        object.body.collect().await?.into_bytes().as_ref(),
        b"hello world"
    );

    let missing_key = "missing.txt";
    let missing_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(missing_key)
        .send()
        .await?
        .upload_id
        .unwrap();
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
    let err = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(missing_key)
        .upload_id(&missing_upload_id)
        .multipart_upload(missing_upload)
        .send()
        .await
        .unwrap_err();
    assert_error_contains(err, "InvalidPart");

    let wrong_key = "wrong-etag.txt";
    let wrong_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(wrong_key)
        .send()
        .await?
        .upload_id
        .unwrap();
    let _wrong_part =
        upload_part(&context, &bucket, wrong_key, &wrong_upload_id, 1, b"one").await?;
    let wrong_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![CompletedPart::builder()
            .part_number(1)
            .e_tag("00000000000000000000000000000000")
            .build()]))
        .build();
    let err = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(wrong_key)
        .upload_id(&wrong_upload_id)
        .multipart_upload(wrong_upload)
        .send()
        .await
        .unwrap_err();
    assert_error_contains(err, "InvalidPart");

    let order_key = "out-of-order.txt";
    let order_upload_id = context
        .create_multipart_upload()
        .bucket(&bucket)
        .key(order_key)
        .send()
        .await?
        .upload_id
        .unwrap();
    let order_part1 =
        upload_part(&context, &bucket, order_key, &order_upload_id, 1, b"one").await?;
    let order_part2 =
        upload_part(&context, &bucket, order_key, &order_upload_id, 2, b"two").await?;
    let order_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![order_part2, order_part1]))
        .build();
    let err = context
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(order_key)
        .upload_id(&order_upload_id)
        .multipart_upload(order_upload)
        .send()
        .await
        .unwrap_err();
    assert_error_contains(err, "InvalidPartOrder");

    delete_object(&context, &bucket, key).await?;
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

#[tokio::test]
#[tracing::instrument]
async fn test_zero_readers_fails_startup() -> Result<()> {
    let _guard = serial().await;
    let root = PathBuf::from(format!("{FS_ROOT}-zero-readers-{}", Uuid::new_v4()));
    fs::create_dir_all(&root)?;

    let err = Sqlite::new(&Config {
        root: root.clone(),
        readers: 0,
        ..Default::default()
    })
    .await
    .unwrap_err();
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
        let context = TestContext::new_with_root(None, root.clone(), false).await;
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
            .unwrap()
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
    .await;

    let err = context
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap_err();
    assert_error_contains(err, "database is in read-only mode");

    let err = context
        .upload_part()
        .bucket(&bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"blocked"))
        .send()
        .await
        .unwrap_err();
    assert_error_contains(err, "database is in read-only mode");

    let object = context.get_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(object.body.collect().await?.into_bytes().as_ref(), content);

    Ok(())
}
