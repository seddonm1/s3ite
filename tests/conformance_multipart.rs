//! Real-binary HTTP multipart conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_multipart`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes},
    sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let first = vec![b'a'; 5 * MIB];
    let second = vec![b'b'; MIB];
    let parts = vec![
        upload_part(&client, &bucket, key, &upload_id, 1, first.clone()).await?,
        upload_part(&client, &bucket, key, &upload_id, 2, second.clone()).await?,
    ];

    complete_upload(&client, &bucket, key, &upload_id, parts.clone()).await?;
    complete_upload(&client, &bucket, key, &upload_id, parts).await?;

    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(key)
        .send()
        .await?;
    assert_eq!(response.contents().len(), 1);
    assert_eq!(
        response.contents()[0].size(),
        Some((first.len() + second.len()) as i64)
    );

    let body = get_object_bytes(&client, &bucket, key).await?;
    assert_eq!(body.len(), first.len() + second.len());
    assert_eq!(&body[..first.len()], first.as_slice());
    assert_eq!(&body[first.len()..], second.as_slice());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_list_multipart_upload() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_list_multipart_upload
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");

    create_bucket(&client, &bucket).await?;
    let upload_id1 = create_upload(&client, &bucket, "mymultipart").await?;
    let upload_id2 = create_upload(&client, &bucket, "mymultipart").await?;
    let upload_id3 = create_upload(&client, &bucket, "mymultipart2").await?;

    let response = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await?;
    let upload_ids = response
        .uploads()
        .iter()
        .filter_map(|upload| upload.upload_id())
        .collect::<Vec<_>>();
    assert!(upload_ids.contains(&upload_id1.as_str()));
    assert!(upload_ids.contains(&upload_id2.as_str()));
    assert!(upload_ids.contains(&upload_id3.as_str()));

    abort_upload(&client, &bucket, "mymultipart", &upload_id1).await?;
    abort_upload(&client, &bucket, "mymultipart", &upload_id2).await?;
    abort_upload(&client, &bucket, "mymultipart2", &upload_id3).await?;

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_missing_part() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_missing_part
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let part = upload_part(&client, &bucket, key, &upload_id, 1, vec![0]).await?;
    let missing_part = CompletedPart::builder()
        .e_tag(
            part.e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(9999)
        .build();

    let error = match complete_upload(&client, &bucket, key, &upload_id, vec![missing_part]).await {
        Ok(_) => bail!("completion with missing part should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidPart");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_incorrect_etag() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_incorrect_etag
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    upload_part(&client, &bucket, key, &upload_id, 1, vec![0]).await?;
    let wrong_part = CompletedPart::builder()
        .e_tag("ffffffffffffffffffffffffffffffff")
        .part_number(1)
        .build();

    let error = match complete_upload(&client, &bucket, key, &upload_id, vec![wrong_part]).await {
        Ok(_) => bail!("completion with wrong ETag should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidPart");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_get_part() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_get_part
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let first = vec![b'a'; 5 * MIB];
    let second = vec![b'b'; MIB];
    let parts = vec![
        upload_part(&client, &bucket, key, &upload_id, 1, first.clone()).await?,
        upload_part(&client, &bucket, key, &upload_id, 2, second.clone()).await?,
    ];

    let missing = match client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await
    {
        Ok(_) => bail!("partNumber before complete should fail"),
        Err(error) => error,
    };
    assert_error_contains(&missing, "NoSuchKey");

    let complete = complete_upload(&client, &bucket, key, &upload_id, parts).await?;

    let part1 = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(part1.content_length(), Some(first.len() as i64));
    assert_eq!(part1.e_tag(), complete.e_tag());
    assert_eq!(part1.body.collect().await?.into_bytes().as_ref(), first);

    let part2 = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(2)
        .send()
        .await?;
    assert_eq!(part2.content_length(), Some(second.len() as i64));
    assert_eq!(part2.e_tag(), complete.e_tag());
    assert_eq!(part2.body.collect().await?.into_bytes().as_ref(), second);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_resend_first_finishes_last() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_resend_first_finishes_last
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let stale = upload_part(&client, &bucket, key, &upload_id, 1, b"BBBBBBBB".to_vec()).await?;
    let current = upload_part(&client, &bucket, key, &upload_id, 1, b"AAAAAAAA".to_vec()).await?;

    complete_upload(&client, &bucket, key, &upload_id, vec![stale, current]).await?;

    let body = get_object_bytes(&client, &bucket, key).await?;
    assert_eq!(body, b"AAAAAAAA");

    Ok(())
}

async fn create_upload(client: &Client, bucket: &str, key: &str) -> Result<String> {
    Ok(client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string())
}

async fn upload_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> Result<CompletedPart> {
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from(body))
        .send()
        .await?;

    Ok(CompletedPart::builder()
        .e_tag(
            response
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(part_number)
        .build())
}

async fn complete_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();

    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(multipart_upload)
        .send()
        .await?)
}

async fn abort_upload(client: &Client, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
}
