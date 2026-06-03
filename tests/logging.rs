//! Real-binary logging side-effect tests.

mod support;

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{
        BucketCannedAcl, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete,
        ObjectAttributes, ObjectCannedAcl, ObjectIdentifier,
    },
};
use reqwest::Method;
use serial_test::serial;
use support::{
    fixtures::{bucket_name, create_bucket, put_bucket_versioning},
    raw_http, sdk,
    server::{TestServer, TestServerOptions},
};

const LOG_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[tokio::test]
#[serial(conformance)]
async fn server_startup_is_logged() -> Result<()> {
    let server = logged_server().await?;
    let log = wait_for_log_contains(&server, "server is running at http://").await?;

    assert!(log.contains(server.endpoint()));
    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn graceful_shutdown_is_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log");

    create_bucket(&client, &bucket).await?;
    let log = wait_for_log_contains(&server, "spawn writer").await?;
    assert!(log.contains("spawn reader"));
    assert!(log.contains("spawn blob reader"));

    let log = server.shutdown_and_read_log()?;
    assert!(log.contains("Gracefully shutdown!"));
    assert!(log.contains("server is stopped"));
    assert!(log.contains("close writer"));
    assert!(log.contains("close reader"));
    assert!(log.contains("close blob reader"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn bucket_creation_and_put_object_are_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log");

    create_bucket(&client, &bucket).await?;
    let log = wait_for_log_contains_all(
        &server,
        &[
            "created bucket",
            &format!("create_bucket{{bucket={bucket}}}: s3ite::s3: close"),
            "time.busy=",
            "time.idle=",
        ],
    )
    .await?;
    assert!(log.contains(&bucket));

    client
        .put_object()
        .bucket(&bucket)
        .key("logged-object")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    let log = wait_for_log_contains_all(
        &server,
        &[
            "write file",
            &format!(
                "put_object{{bucket={bucket} key=logged-object content_length=Some(4)}}: s3ite::s3: close"
            ),
        ],
    )
    .await?;
    assert!(log.contains("logged-object"));
    assert!(log.contains("size=4"));
    assert!(log.contains("841a2d689ad86bd1611447453c22c6fc"));

    client
        .get_object()
        .bucket(&bucket)
        .key("logged-object")
        .send()
        .await?;
    let log = wait_for_log_contains_all(
        &server,
        &[
            &format!("get_object{{bucket={bucket} key=logged-object"),
            "version_id= part_number=None range=None}: s3ite::s3: close",
        ],
    )
    .await?;
    assert!(!log.contains("secret-key-for-tests"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn multipart_completion_is_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log");
    let key = "logged-multipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let part = upload_part(&client, &bucket, key, &upload_id, 1, b"part-body").await?;
    complete_upload(&client, &bucket, key, &upload_id, vec![part]).await?;

    let log = wait_for_log_contains_all(
        &server,
        &[
            "completed multipart upload",
            &format!(
                "complete_multipart_upload{{bucket={bucket} key={key} upload_id={upload_id}}}: s3ite::s3: close"
            ),
        ],
    )
    .await?;
    assert!(log.contains("version_id=null"));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn all_s3_endpoint_trace_closes_are_logged() -> Result<()> {
    let server = logged_server().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-log-all");
    let target_bucket = bucket_name("s3ite-log-target");
    let empty_bucket = bucket_name("s3ite-log-empty");
    let key = "logged-object";
    let copy_key = "logged-copy";
    let delete_key = "logged-delete-one";
    let delete_many_a = "logged-delete-many-a";
    let delete_many_b = "logged-delete-many-b";
    let multipart_key = "logged-multipart";
    let copy_part_key = "logged-copy-part";
    let abort_key = "logged-abort";

    create_bucket(&client, &bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    create_bucket(&client, &empty_bucket).await?;

    client.list_buckets().send().await?;
    client.head_bucket().bucket(&bucket).send().await?;
    client.get_bucket_location().bucket(&bucket).send().await?;
    client.get_bucket_acl().bucket(&bucket).send().await?;
    client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await?;

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await?;
    for key in [delete_key, delete_many_a, delete_many_b] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"delete"))
            .send()
            .await?;
    }

    client
        .copy_object()
        .bucket(&bucket)
        .key(copy_key)
        .copy_source(format!("{bucket}/{key}"))
        .send()
        .await?;
    client.head_object().bucket(&bucket).key(key).send().await?;
    client.get_object().bucket(&bucket).key(key).send().await?;
    client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .max_parts(2)
        .part_number_marker("0")
        .send()
        .await?;
    client
        .get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    client
        .put_object_acl()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::Private)
        .send()
        .await?;
    let _torrent = raw_http::signed_request(
        &server,
        Method::GET,
        &format!("/{bucket}/{key}?torrent"),
        Vec::new(),
    )
    .await?;

    client
        .list_objects()
        .bucket(&bucket)
        .prefix("logged")
        .delimiter("/")
        .max_keys(10)
        .send()
        .await?;
    client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("logged")
        .delimiter("/")
        .start_after("logged-")
        .max_keys(10)
        .send()
        .await?;
    client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("logged")
        .delimiter("/")
        .max_keys(10)
        .send()
        .await?;

    let upload_id = create_upload(&client, &bucket, multipart_key).await?;
    let part = upload_part(&client, &bucket, multipart_key, &upload_id, 1, b"part-body").await?;
    client
        .list_parts()
        .bucket(&bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number_marker("0")
        .max_parts(10)
        .send()
        .await?;
    client
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("logged")
        .delimiter("/")
        .max_uploads(10)
        .send()
        .await?;
    complete_upload(&client, &bucket, multipart_key, &upload_id, vec![part]).await?;

    let copy_upload_id = create_upload(&client, &bucket, copy_part_key).await?;
    let copy_part = upload_part_copy(
        &client,
        &bucket,
        copy_part_key,
        &copy_upload_id,
        1,
        &format!("{bucket}/{key}"),
        Some("bytes=0-3"),
    )
    .await?;
    complete_upload(
        &client,
        &bucket,
        copy_part_key,
        &copy_upload_id,
        vec![copy_part],
    )
    .await?;

    let abort_upload_id = create_upload(&client, &bucket, abort_key).await?;
    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(abort_key)
        .upload_id(&abort_upload_id)
        .send()
        .await?;

    client
        .delete_object()
        .bucket(&bucket)
        .key(delete_key)
        .send()
        .await?;
    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key(delete_many_a).build()?)
        .objects(ObjectIdentifier::builder().key(delete_many_b).build()?)
        .build()?;
    client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    client.delete_bucket().bucket(&empty_bucket).send().await?;

    let expected = vec![
        format!("create_bucket{{bucket={bucket}}}: s3ite::s3: close"),
        format!("create_bucket{{bucket={target_bucket}}}: s3ite::s3: close"),
        format!("create_bucket{{bucket={empty_bucket}}}: s3ite::s3: close"),
        format!("copy_object{{bucket={bucket} key={copy_key}}}: s3ite::s3: close"),
        format!("delete_bucket{{bucket={empty_bucket}}}: s3ite::s3: close"),
        format!("delete_object{{bucket={bucket} key={delete_key} version_id=}}: s3ite::s3: close"),
        format!("delete_objects{{bucket={bucket} key_count=2}}: s3ite::s3: close"),
        format!("get_bucket_location{{bucket={bucket}}}: s3ite::s3: close"),
        format!("get_bucket_acl{{bucket={bucket}}}: s3ite::s3: close"),
        format!("get_bucket_versioning{{bucket={bucket}}}: s3ite::s3: close"),
        format!("put_bucket_versioning{{bucket={bucket}}}: s3ite::s3: close"),
        format!(
            "get_object{{bucket={bucket} key={key} version_id= part_number=None range=None}}: s3ite::s3: close"
        ),
        format!("get_object_torrent{{bucket={bucket} key={key}}}: s3ite::s3: close"),
        format!("head_bucket{{bucket={bucket}}}: s3ite::s3: close"),
        format!(
            "head_object{{bucket={bucket} key={key} version_id= part_number=None}}: s3ite::s3: close"
        ),
        format!(
            "get_object_attributes{{bucket={bucket} key={key} version_id= max_parts=Some(2) part_number_marker=Some(0)}}: s3ite::s3: close"
        ),
        format!("get_object_acl{{bucket={bucket} key={key} version_id=}}: s3ite::s3: close"),
        format!("put_bucket_acl{{bucket={bucket}}}: s3ite::s3: close"),
        "list_buckets{prefix= continuation_token= max_buckets=None}: s3ite::s3: close".to_string(),
        format!(
            "list_objects{{bucket={bucket} prefix=logged delimiter=/ marker= max_keys=Some(10)}}: s3ite::s3: close"
        ),
        format!(
            "list_object_versions{{bucket={bucket} prefix=logged delimiter=/ key_marker= version_id_marker= max_keys=Some(10)}}: s3ite::s3: close"
        ),
        format!(
            "list_objects_v2{{bucket={bucket} prefix=logged delimiter=/ continuation_token= start_after=logged- max_keys=Some(10)}}: s3ite::s3: close"
        ),
        format!(
            "list_multipart_uploads{{bucket={bucket} prefix=logged delimiter=/ key_marker= upload_id_marker= max_uploads=Some(10)}}: s3ite::s3: close"
        ),
        format!("put_object{{bucket={bucket} key={key} content_length=Some(4)}}: s3ite::s3: close"),
        format!("put_object_acl{{bucket={bucket} key={key} version_id=}}: s3ite::s3: close"),
        format!("create_multipart_upload{{bucket={bucket} key={multipart_key}}}: s3ite::s3: close"),
        format!(
            "upload_part{{bucket={bucket} key={multipart_key} upload_id={upload_id} part_number=1 content_length=Some(9)}}: s3ite::s3: close"
        ),
        format!(
            "upload_part_copy{{bucket={bucket} key={copy_part_key} upload_id={copy_upload_id} part_number=1 source_range=bytes=0-3}}: s3ite::s3: close"
        ),
        format!(
            "list_parts{{bucket={bucket} key={multipart_key} upload_id={upload_id} part_number_marker=Some(0) max_parts=Some(10)}}: s3ite::s3: close"
        ),
        format!(
            "abort_multipart_upload{{bucket={bucket} key={abort_key} upload_id={abort_upload_id}}}: s3ite::s3: close"
        ),
        format!(
            "complete_multipart_upload{{bucket={bucket} key={multipart_key} upload_id={upload_id}}}: s3ite::s3: close"
        ),
    ];
    let log = wait_for_log_contains_all(&server, &expected).await?;
    assert!(log.contains("time.busy="));
    assert!(log.contains("time.idle="));
    assert!(!log.contains("secret-key-for-tests"));

    Ok(())
}

async fn logged_server() -> Result<TestServer> {
    TestServer::spawn_with(TestServerOptions {
        rust_log: Some("s3ite=debug,s3s=warn".to_string()),
        ..Default::default()
    })
    .await
}

async fn wait_for_log_contains(server: &TestServer, needle: &str) -> Result<String> {
    wait_for_log_contains_all(server, &[needle]).await
}

async fn wait_for_log_contains_all<S>(server: &TestServer, needles: &[S]) -> Result<String>
where
    S: AsRef<str>,
{
    let deadline = Instant::now() + LOG_TIMEOUT;
    loop {
        let log = tokio::fs::read_to_string(server.log_path())
            .await
            .with_context(|| format!("read {}", server.log_path().display()))?;
        let missing = needles
            .iter()
            .map(AsRef::as_ref)
            .filter(|needle| !log.contains(*needle))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(log);
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for log entries {missing:?}; current log:\n{log}");
        }
        tokio::time::sleep(LOG_POLL_INTERVAL).await;
    }
}

async fn create_upload(client: &Client, bucket: &str, key: &str) -> Result<String> {
    Ok(client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .context("create multipart response missing upload ID")?
        .to_string())
}

async fn upload_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: &'static [u8],
) -> Result<CompletedPart> {
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(
            response
                .e_tag()
                .context("upload part response missing ETag")?,
        )
        .build())
}

async fn complete_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> Result<()> {
    let multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(multipart_upload)
        .send()
        .await?;
    Ok(())
}

async fn upload_part_copy(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    copy_source: &str,
    copy_source_range: Option<&str>,
) -> Result<CompletedPart> {
    let mut request = client
        .upload_part_copy()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .copy_source(copy_source);
    if let Some(copy_source_range) = copy_source_range {
        request = request.copy_source_range(copy_source_range);
    }

    let response = request.send().await?;
    let result = response
        .copy_part_result()
        .context("upload part copy response missing result")?;
    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(
            result
                .e_tag()
                .context("upload part copy response missing ETag")?,
        )
        .build())
}
