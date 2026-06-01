//! Real-binary HTTP checksum conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_checksum`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{
        ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
        ObjectAttributes,
    },
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes},
    sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;
const SHA256_1024_A: &str = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";
const CRC64NVME_1024_A: &str = "Qeh8oXvGiSo=";
const CRC32_5_MIB_A: &str = "JRTCyQ==";
const CRC32_5_MIB_B: &str = "QoZTGg==";
const CRC32_5_MIB_C: &str = "YAgjqw==";
const CRC32_15_MIB_ABC: &str = "WgDhBQ==";
const CRC32C_5_MIB_A: &str = "MDaLrw==";
const CRC32C_5_MIB_B: &str = "TH4EZg==";
const CRC32C_5_MIB_C: &str = "Z7mBIQ==";
const CRC32C_15_MIB_ABC: &str = "xU+Krw==";

#[tokio::test]
#[serial(conformance)]
async fn test_object_checksum_sha256() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_checksum_sha256
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("myobj")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(SHA256_1024_A)
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await?;
    assert_eq!(put.checksum_sha256(), Some(SHA256_1024_A));
    assert_eq!(put.checksum_type(), Some(&ChecksumType::FullObject));

    let plain_head = client
        .head_object()
        .bucket(&bucket)
        .key("myobj")
        .send()
        .await?;
    assert!(plain_head.checksum_sha256().is_none());

    let checksum_head = client
        .head_object()
        .bucket(&bucket)
        .key("myobj")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(checksum_head.checksum_sha256(), Some(SHA256_1024_A));

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("myobj")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await
    {
        Ok(_) => bail!("bad SHA256 checksum should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "BadDigest");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_object_checksum_crc64nvme() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_object_checksum_crc64nvme
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("myobj")
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_crc64_nvme(CRC64NVME_1024_A)
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await?;
    assert_eq!(put.checksum_crc64_nvme(), Some(CRC64NVME_1024_A));
    assert_eq!(put.checksum_type(), Some(&ChecksumType::FullObject));

    let plain_head = client
        .head_object()
        .bucket(&bucket)
        .key("myobj")
        .send()
        .await?;
    assert!(plain_head.checksum_crc64_nvme().is_none());

    let checksum_head = client
        .head_object()
        .bucket(&bucket)
        .key("myobj")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(checksum_head.checksum_crc64_nvme(), Some(CRC64NVME_1024_A));

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("myobj")
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_crc64_nvme("AAAAAAAAAAA=")
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await
    {
        Ok(_) => bail!("bad CRC64NVME checksum should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "BadDigest");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_default_crc64nvme_for_put_and_attributes() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("default")
        .body(ByteStream::from_static(b"default checksum"))
        .send()
        .await?;
    let checksum = put
        .checksum_crc64_nvme()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing CRC64NVME"))?;
    assert_eq!(put.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(put.checksum_sha256().is_none());

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("default")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc64_nvme(), Some(checksum));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(&bucket)
        .key("default")
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await?;
    let attrs_checksum = attrs
        .checksum()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing checksum"))?;
    assert_eq!(attrs_checksum.checksum_crc64_nvme(), Some(checksum));
    assert_eq!(
        attrs_checksum.checksum_type(),
        Some(&ChecksumType::FullObject)
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_copy_object_inherits_source_checksum_algorithm() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("source")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(SHA256_1024_A)
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await?;

    let copy = client
        .copy_object()
        .bucket(&bucket)
        .key("target")
        .copy_source(format!("{bucket}/source"))
        .send()
        .await?;
    let copy_result = copy
        .copy_object_result()
        .ok_or_else(|| anyhow::anyhow!("CopyObject response missing result"))?;
    assert_eq!(copy_result.checksum_sha256(), Some(SHA256_1024_A));
    assert!(copy_result.checksum_crc64_nvme().is_none());
    assert_eq!(copy_result.checksum_type(), Some(&ChecksumType::FullObject));

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("target")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_sha256(), Some(SHA256_1024_A));
    assert!(head.checksum_crc64_nvme().is_none());
    assert_eq!(
        get_object_bytes(&client, &bucket, "target").await?,
        vec![b'A'; 1024]
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_use_cksum_helper_crc32() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_use_cksum_helper_crc32
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    let completed = complete_crc32_full_object(&client, &bucket, "mymultipart3").await?;
    assert_eq!(completed.checksum_crc32(), Some(CRC32_15_MIB_ABC));
    assert_eq!(completed.checksum_type(), Some(&ChecksumType::FullObject));

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("mymultipart3")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc32(), Some(CRC32_15_MIB_ABC));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(&bucket)
        .key("mymultipart3")
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await?;
    assert_eq!(
        attrs
            .checksum()
            .and_then(|checksum| checksum.checksum_crc32()),
        Some(CRC32_15_MIB_ABC)
    );

    let part = client
        .get_object()
        .bucket(&bucket)
        .key("mymultipart3")
        .part_number(1)
        .send()
        .await?;
    assert_eq!(part.checksum_crc32(), Some(CRC32_5_MIB_A));
    assert_eq!(part.checksum_type(), Some(&ChecksumType::FullObject));

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_multipart_use_cksum_helper_crc32c() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_use_cksum_helper_crc32c
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    let completed = complete_crc32c_full_object(&client, &bucket, "mymultipart3").await?;
    assert_eq!(completed.checksum_crc32_c(), Some(CRC32C_15_MIB_ABC));
    assert_eq!(completed.checksum_type(), Some(&ChecksumType::FullObject));

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("mymultipart3")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc32_c(), Some(CRC32C_15_MIB_ABC));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(&bucket)
        .key("mymultipart3")
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await?;
    assert_eq!(
        attrs
            .checksum()
            .and_then(|checksum| checksum.checksum_crc32_c()),
        Some(CRC32C_15_MIB_ABC)
    );

    let part = client
        .get_object()
        .bucket(&bucket)
        .key("mymultipart3")
        .part_number(1)
        .send()
        .await?;
    assert_eq!(part.checksum_crc32_c(), Some(CRC32C_5_MIB_A));
    assert_eq!(part.checksum_type(), Some(&ChecksumType::FullObject));

    Ok(())
}

async fn complete_crc32_full_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let upload_id = create_upload(client, bucket, key, ChecksumAlgorithm::Crc32).await?;
    let part1 = upload_crc32_part(client, bucket, key, &upload_id, 1, b'A', CRC32_5_MIB_A).await?;
    let part2 = upload_crc32_part(client, bucket, key, &upload_id, 2, b'B', CRC32_5_MIB_B).await?;
    let part3 = upload_crc32_part(client, bucket, key, &upload_id, 3, b'C', CRC32_5_MIB_C).await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part1, part2, part3]))
        .build();

    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .checksum_crc32(CRC32_15_MIB_ABC)
        .multipart_upload(upload)
        .send()
        .await?)
}

async fn complete_crc32c_full_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let upload_id = create_upload(client, bucket, key, ChecksumAlgorithm::Crc32C).await?;
    let part1 =
        upload_crc32c_part(client, bucket, key, &upload_id, 1, b'A', CRC32C_5_MIB_A).await?;
    let part2 =
        upload_crc32c_part(client, bucket, key, &upload_id, 2, b'B', CRC32C_5_MIB_B).await?;
    let part3 =
        upload_crc32c_part(client, bucket, key, &upload_id, 3, b'C', CRC32C_5_MIB_C).await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part1, part2, part3]))
        .build();

    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .checksum_crc32_c(CRC32C_15_MIB_ABC)
        .multipart_upload(upload)
        .send()
        .await?)
}

async fn create_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    algorithm: ChecksumAlgorithm,
) -> Result<String> {
    Ok(client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .checksum_algorithm(algorithm)
        .checksum_type(ChecksumType::FullObject)
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string())
}

async fn upload_crc32_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    byte: u8,
    checksum: &str,
) -> Result<CompletedPart> {
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_crc32(checksum)
        .body(ByteStream::from(vec![byte; 5 * MIB]))
        .send()
        .await?;

    Ok(CompletedPart::builder()
        .e_tag(
            response
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(part_number)
        .checksum_crc32(checksum)
        .build())
}

async fn upload_crc32c_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    byte: u8,
    checksum: &str,
) -> Result<CompletedPart> {
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .checksum_crc32_c(checksum)
        .body(ByteStream::from(vec![byte; 5 * MIB]))
        .send()
        .await?;

    Ok(CompletedPart::builder()
        .e_tag(
            response
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(part_number)
        .checksum_crc32_c(checksum)
        .build())
}
