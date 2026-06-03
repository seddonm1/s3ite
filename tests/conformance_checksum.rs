//! Real-binary HTTP checksum conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_checksum`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadOutput, get_object::GetObjectOutput,
        head_object::HeadObjectOutput,
    },
    primitives::ByteStream,
    types::{
        Checksum, ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload,
        CompletedPart, ObjectAttributes,
    },
};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
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
const SHA256_5_MIB_A: &str = "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=";
const SHA256_5_MIB_B: &str = "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=";
const SHA256_5_MIB_C: &str = "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=";
const SHA256_15_MIB_ABC_COMPOSITE: &str = "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3";
const SHA256_1024_A_COMPOSITE: &str = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";
const SHA1_5_MIB_A: &str = "iIaTCGbm+vdVjNqIMF2S0T7ibMk=";
const SHA1_5_MIB_B: &str = "LS/TJ32bAVKEwRu+sE3X7awh/lk=";
const SHA1_5_MIB_C: &str = "6DDwovUaHwrKNXDMzOGbuvj9kxI=";
const SHA1_15_MIB_ABC_COMPOSITE: &str = "sizjvY4eud3MrcHdZM3cQ/ol39o=-3";
const CRC64NVME_5_MIB_A: &str = "L/E4WYn8v98=";
const CRC64NVME_5_MIB_B: &str = "xW1l19VobYM=";
const CRC64NVME_5_MIB_C: &str = "cK5MnNaWrW4=";
const CRC64NVME_15_MIB_ABC: &str = "i+6LR0y3eFo=";
const SHA256_MULTIPART_ETAG_ABC: &str = "b2add96cc9702bbf4efb0ccdfc6b7747-3";

/// Verifies explicit SHA256 object checksums are stored, exposed, and validated.
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

/// Verifies explicit SHA256 PutObject checksums suppress default CRC64NVME.
#[tokio::test]
#[serial(conformance)]
async fn test_explicit_sha256_put_suppresses_default_crc64nvme() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("sha256")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(SHA256_1024_A)
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await?;
    assert_eq!(put.checksum_sha256(), Some(SHA256_1024_A));
    assert!(put.checksum_crc64_nvme().is_none());
    assert_eq!(put.checksum_type(), Some(&ChecksumType::FullObject));

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("sha256")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_sha256(), Some(SHA256_1024_A));
    assert!(head.checksum_crc64_nvme().is_none());

    Ok(())
}

/// Verifies explicit CRC64NVME object checksums are stored, exposed, and validated.
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

/// Verifies default CRC64NVME checksums appear through HeadObject and attributes.
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

/// Verifies CopyObject gets a default CRC64NVME checksum when the source has no checksum.
#[tokio::test]
#[serial(conformance)]
async fn test_default_crc64nvme_for_copy_object() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "source", b"copy checksum").await?;

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
    let checksum = copy_result
        .checksum_crc64_nvme()
        .ok_or_else(|| anyhow::anyhow!("CopyObject response missing CRC64NVME"))?;
    assert_eq!(copy_result.checksum_type(), Some(&ChecksumType::FullObject));

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("target")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc64_nvme(), Some(checksum));
    assert_eq!(
        get_object_bytes(&client, &bucket, "target").await?,
        b"copy checksum"
    );

    Ok(())
}

/// Verifies CopyObject inherits the source checksum algorithm when unspecified.
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

/// Verifies default multipart completion exposes a full-object CRC64NVME checksum.
#[tokio::test]
#[serial(conformance)]
async fn test_default_crc64nvme_for_multipart_completion() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");
    let key = "multipart-default";

    create_bucket(&client, &bucket).await?;
    let upload_id = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string();
    let part1 =
        upload_plain_part(&client, &bucket, key, &upload_id, 1, vec![b'A'; 5 * MIB]).await?;
    let part2 = upload_plain_part(&client, &bucket, key, &upload_id, 2, b"tail".to_vec()).await?;
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part1, part2]))
        .build();

    let completed = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(upload)
        .send()
        .await?;
    let checksum = completed
        .checksum_crc64_nvme()
        .ok_or_else(|| anyhow::anyhow!("CompleteMultipartUpload missing CRC64NVME"))?;
    assert_eq!(completed.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(completed.checksum_sha256().is_none());

    let head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc64_nvme(), Some(checksum));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await?;
    assert_eq!(
        attrs
            .checksum()
            .and_then(|value| value.checksum_crc64_nvme()),
        Some(checksum)
    );

    let part = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await?;
    assert!(part.checksum_crc64_nvme().is_none());
    assert!(part.checksum_type().is_none());

    Ok(())
}

/// Verifies multipart CRC32 full-object completion and part checksum exposure.
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

/// Verifies multipart CRC32C full-object completion and part checksum exposure.
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

/// Verifies multipart SHA256 completion rejects bad or missing part checksums.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_checksum_sha256() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_checksum_sha256
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");

    create_bucket(&client, &bucket).await?;

    let upload_id = create_checksum_upload(
        &client,
        &bucket,
        "mymultipart",
        ChecksumAlgorithm::Sha256,
        None,
    )
    .await?;
    let part = upload_checksum_part(
        &client,
        ChecksumPartUpload::new(
            &bucket,
            "mymultipart",
            &upload_id,
            1,
            PartBody::new(b'A', 1024),
            ChecksumAlgorithm::Sha256,
            SHA256_1024_A,
        ),
    )
    .await?;
    let error = match complete_checksum_upload(
        &client,
        &bucket,
        "mymultipart",
        &upload_id,
        ChecksumAlgorithm::Sha256,
        "bad",
        vec![part],
    )
    .await
    {
        Ok(_) => bail!("bad multipart SHA256 checksum should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "BadDigest");

    let upload_id = create_checksum_upload(
        &client,
        &bucket,
        "mymultipart2",
        ChecksumAlgorithm::Sha256,
        None,
    )
    .await?;
    let part = upload_checksum_part(
        &client,
        ChecksumPartUpload::new(
            &bucket,
            "mymultipart2",
            &upload_id,
            1,
            PartBody::new(b'A', 1024),
            ChecksumAlgorithm::Sha256,
            SHA256_1024_A,
        ),
    )
    .await?;
    let missing_checksum_part = CompletedPart::builder()
        .e_tag(
            part.e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(1)
        .build();
    let error = match complete_checksum_upload(
        &client,
        &bucket,
        "mymultipart2",
        &upload_id,
        ChecksumAlgorithm::Sha256,
        "bad",
        vec![missing_checksum_part],
    )
    .await
    {
        Ok(_) => bail!("multipart completion missing part checksum should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "BadDigest");

    let upload_id = create_checksum_upload(
        &client,
        &bucket,
        "mymultipart3",
        ChecksumAlgorithm::Sha256,
        None,
    )
    .await?;
    let part = upload_checksum_part(
        &client,
        ChecksumPartUpload::new(
            &bucket,
            "mymultipart3",
            &upload_id,
            1,
            PartBody::new(b'A', 1024),
            ChecksumAlgorithm::Sha256,
            SHA256_1024_A,
        ),
    )
    .await?;
    let completed = complete_checksum_upload(
        &client,
        &bucket,
        "mymultipart3",
        &upload_id,
        ChecksumAlgorithm::Sha256,
        SHA256_1024_A_COMPOSITE,
        vec![part],
    )
    .await?;
    assert_eq!(completed.checksum_sha256(), Some(SHA256_1024_A_COMPOSITE));

    let head = client
        .head_object()
        .bucket(&bucket)
        .key("mymultipart3")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_sha256(), Some(SHA256_1024_A_COMPOSITE));

    Ok(())
}

/// Verifies idempotent multipart completion preserves ETag and SHA256 checksum.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_reupload_checksum_and_etag() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_reupload_checksum_and_etag
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id =
        create_checksum_upload(&client, &bucket, key, ChecksumAlgorithm::Sha256, None).await?;
    let parts = upload_checksum_abc_parts(
        &client,
        &bucket,
        key,
        &upload_id,
        ChecksumAlgorithm::Sha256,
        [SHA256_5_MIB_A, SHA256_5_MIB_B, SHA256_5_MIB_C],
    )
    .await?;

    let first = complete_checksum_upload(
        &client,
        &bucket,
        key,
        &upload_id,
        ChecksumAlgorithm::Sha256,
        SHA256_15_MIB_ABC_COMPOSITE,
        parts.clone(),
    )
    .await?;
    assert_eq!(
        first.e_tag().map(unquoted_etag),
        Some(SHA256_MULTIPART_ETAG_ABC)
    );
    assert_eq!(first.checksum_sha256(), Some(SHA256_15_MIB_ABC_COMPOSITE));

    let second = complete_checksum_upload(
        &client,
        &bucket,
        key,
        &upload_id,
        ChecksumAlgorithm::Sha256,
        SHA256_15_MIB_ABC_COMPOSITE,
        parts,
    )
    .await?;
    assert_eq!(second.e_tag(), first.e_tag());
    assert_eq!(second.checksum_sha256(), first.checksum_sha256());

    Ok(())
}

/// Verifies multipart SHA256 composite checksum exposure across APIs.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_use_cksum_helper_sha256() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_use_cksum_helper_sha256
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");
    let key = "mymultipart3";

    create_bucket(&client, &bucket).await?;
    let completed = complete_three_part_checksum(
        &client,
        &bucket,
        key,
        ChecksumAlgorithm::Sha256,
        ChecksumType::Composite,
        [SHA256_5_MIB_A, SHA256_5_MIB_B, SHA256_5_MIB_C],
        SHA256_15_MIB_ABC_COMPOSITE,
    )
    .await?;
    assert_eq!(
        completed.checksum_sha256(),
        Some(SHA256_15_MIB_ABC_COMPOSITE)
    );
    assert!(completed.checksum_crc64_nvme().is_none());

    assert_checksum_outputs(
        &client,
        &bucket,
        key,
        ChecksumAlgorithm::Sha256,
        ChecksumType::Composite,
        SHA256_15_MIB_ABC_COMPOSITE,
        [SHA256_5_MIB_A, SHA256_5_MIB_B, SHA256_5_MIB_C],
    )
    .await?;
    let head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert!(head.checksum_crc64_nvme().is_none());

    Ok(())
}

/// Verifies multipart CRC64NVME full-object checksum exposure across APIs.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_use_cksum_helper_crc64nvme() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_use_cksum_helper_crc64nvme
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");
    let key = "mymultipart3";

    create_bucket(&client, &bucket).await?;
    complete_three_part_checksum(
        &client,
        &bucket,
        key,
        ChecksumAlgorithm::Crc64Nvme,
        ChecksumType::FullObject,
        [CRC64NVME_5_MIB_A, CRC64NVME_5_MIB_B, CRC64NVME_5_MIB_C],
        CRC64NVME_15_MIB_ABC,
    )
    .await?;

    assert_checksum_outputs(
        &client,
        &bucket,
        key,
        ChecksumAlgorithm::Crc64Nvme,
        ChecksumType::FullObject,
        CRC64NVME_15_MIB_ABC,
        [CRC64NVME_5_MIB_A, CRC64NVME_5_MIB_B, CRC64NVME_5_MIB_C],
    )
    .await?;

    Ok(())
}

/// Verifies multipart SHA1 composite checksum exposure across APIs.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_use_cksum_helper_sha1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_use_cksum_helper_sha1
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ck");
    let key = "mymultipart3";

    create_bucket(&client, &bucket).await?;
    complete_three_part_checksum(
        &client,
        &bucket,
        key,
        ChecksumAlgorithm::Sha1,
        ChecksumType::Composite,
        [SHA1_5_MIB_A, SHA1_5_MIB_B, SHA1_5_MIB_C],
        SHA1_15_MIB_ABC_COMPOSITE,
    )
    .await?;

    assert_checksum_outputs(
        &client,
        &bucket,
        key,
        ChecksumAlgorithm::Sha1,
        ChecksumType::Composite,
        SHA1_15_MIB_ABC_COMPOSITE,
        [SHA1_5_MIB_A, SHA1_5_MIB_B, SHA1_5_MIB_C],
    )
    .await?;

    Ok(())
}

async fn complete_crc32_full_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<CompleteMultipartUploadOutput> {
    complete_three_part_checksum(
        client,
        bucket,
        key,
        ChecksumAlgorithm::Crc32,
        ChecksumType::FullObject,
        [CRC32_5_MIB_A, CRC32_5_MIB_B, CRC32_5_MIB_C],
        CRC32_15_MIB_ABC,
    )
    .await
}

async fn complete_crc32c_full_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<CompleteMultipartUploadOutput> {
    complete_three_part_checksum(
        client,
        bucket,
        key,
        ChecksumAlgorithm::Crc32C,
        ChecksumType::FullObject,
        [CRC32C_5_MIB_A, CRC32C_5_MIB_B, CRC32C_5_MIB_C],
        CRC32C_15_MIB_ABC,
    )
    .await
}

async fn complete_three_part_checksum(
    client: &Client,
    bucket: &str,
    key: &str,
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
    part_checksums: [&str; 3],
    final_checksum: &str,
) -> Result<CompleteMultipartUploadOutput> {
    let upload_id =
        create_checksum_upload(client, bucket, key, algorithm.clone(), Some(checksum_type)).await?;
    let parts = upload_checksum_abc_parts(
        client,
        bucket,
        key,
        &upload_id,
        algorithm.clone(),
        part_checksums,
    )
    .await?;
    complete_checksum_upload(
        client,
        bucket,
        key,
        &upload_id,
        algorithm,
        final_checksum,
        parts,
    )
    .await
}

async fn create_checksum_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    algorithm: ChecksumAlgorithm,
    checksum_type: Option<ChecksumType>,
) -> Result<String> {
    let mut request = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .checksum_algorithm(algorithm);
    if let Some(checksum_type) = checksum_type {
        request = request.checksum_type(checksum_type);
    }

    Ok(request
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string())
}

async fn upload_checksum_abc_parts(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    algorithm: ChecksumAlgorithm,
    checksums: [&str; 3],
) -> Result<Vec<CompletedPart>> {
    let mut parts = Vec::with_capacity(3);
    for (index, byte) in [b'A', b'B', b'C'].into_iter().enumerate() {
        parts.push(
            upload_checksum_part(
                client,
                ChecksumPartUpload::new(
                    bucket,
                    key,
                    upload_id,
                    i32::try_from(index + 1)?,
                    PartBody::new(byte, 5 * MIB),
                    algorithm.clone(),
                    checksums[index],
                ),
            )
            .await?,
        );
    }
    Ok(parts)
}

#[derive(Clone, Copy)]
struct PartBody {
    byte: u8,
    size: usize,
}

impl PartBody {
    fn new(byte: u8, size: usize) -> Self {
        Self { byte, size }
    }
}

struct ChecksumPartUpload<'a> {
    bucket: &'a str,
    key: &'a str,
    upload_id: &'a str,
    part_number: i32,
    body: PartBody,
    algorithm: ChecksumAlgorithm,
    checksum: &'a str,
}

impl<'a> ChecksumPartUpload<'a> {
    fn new(
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
        part_number: i32,
        body: PartBody,
        algorithm: ChecksumAlgorithm,
        checksum: &'a str,
    ) -> Self {
        Self {
            bucket,
            key,
            upload_id,
            part_number,
            body,
            algorithm,
            checksum,
        }
    }
}

async fn upload_checksum_part(
    client: &Client,
    upload: ChecksumPartUpload<'_>,
) -> Result<CompletedPart> {
    let request = client
        .upload_part()
        .bucket(upload.bucket)
        .key(upload.key)
        .upload_id(upload.upload_id)
        .part_number(upload.part_number)
        .checksum_algorithm(upload.algorithm.clone())
        .body(ByteStream::from(vec![upload.body.byte; upload.body.size]));
    let response = set_upload_checksum(request, &upload.algorithm, upload.checksum)
        .send()
        .await?;

    let builder = CompletedPart::builder()
        .e_tag(
            response
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(upload.part_number);
    completed_part_checksum(builder, &upload.algorithm, upload.checksum)
}

async fn complete_checksum_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    algorithm: ChecksumAlgorithm,
    checksum: &str,
    parts: Vec<CompletedPart>,
) -> Result<CompleteMultipartUploadOutput> {
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let request = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload);

    Ok(set_completion_checksum(request, &algorithm, checksum)
        .send()
        .await?)
}

async fn assert_checksum_outputs(
    client: &Client,
    bucket: &str,
    key: &str,
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
    final_checksum: &str,
    part_checksums: [&str; 3],
) -> Result<()> {
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_type(), Some(&checksum_type));
    assert_eq!(head_checksum(&head, &algorithm), Some(final_checksum));

    let attrs = client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await?;
    let checksum = attrs
        .checksum()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing checksum"))?;
    assert_eq!(checksum.checksum_type(), Some(&checksum_type));
    assert_eq!(
        object_attributes_checksum(checksum, &algorithm),
        Some(final_checksum)
    );

    for (index, checksum) in part_checksums.into_iter().enumerate() {
        let part = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .part_number(i32::try_from(index + 1)?)
            .send()
            .await?;
        assert_eq!(part.checksum_type(), Some(&checksum_type));
        assert_eq!(get_object_checksum(&part, &algorithm), Some(checksum));
    }

    Ok(())
}

fn set_upload_checksum(
    request: aws_sdk_s3::operation::upload_part::builders::UploadPartFluentBuilder,
    algorithm: &ChecksumAlgorithm,
    checksum: &str,
) -> aws_sdk_s3::operation::upload_part::builders::UploadPartFluentBuilder {
    match algorithm {
        ChecksumAlgorithm::Crc32 => request.checksum_crc32(checksum),
        ChecksumAlgorithm::Crc32C => request.checksum_crc32_c(checksum),
        ChecksumAlgorithm::Crc64Nvme => request.checksum_crc64_nvme(checksum),
        ChecksumAlgorithm::Sha1 => request.checksum_sha1(checksum),
        ChecksumAlgorithm::Sha256 => request.checksum_sha256(checksum),
        _ => request,
    }
}

fn set_completion_checksum(
    request: aws_sdk_s3::operation::complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder,
    algorithm: &ChecksumAlgorithm,
    checksum: &str,
) -> aws_sdk_s3::operation::complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder
{
    match algorithm {
        ChecksumAlgorithm::Crc32 => request.checksum_crc32(checksum),
        ChecksumAlgorithm::Crc32C => request.checksum_crc32_c(checksum),
        ChecksumAlgorithm::Crc64Nvme => request.checksum_crc64_nvme(checksum),
        ChecksumAlgorithm::Sha1 => request.checksum_sha1(checksum),
        ChecksumAlgorithm::Sha256 => request.checksum_sha256(checksum),
        _ => request,
    }
}

fn completed_part_checksum(
    builder: aws_sdk_s3::types::builders::CompletedPartBuilder,
    algorithm: &ChecksumAlgorithm,
    checksum: &str,
) -> Result<CompletedPart> {
    let builder = match algorithm {
        ChecksumAlgorithm::Crc32 => builder.checksum_crc32(checksum),
        ChecksumAlgorithm::Crc32C => builder.checksum_crc32_c(checksum),
        ChecksumAlgorithm::Crc64Nvme => builder.checksum_crc64_nvme(checksum),
        ChecksumAlgorithm::Sha1 => builder.checksum_sha1(checksum),
        ChecksumAlgorithm::Sha256 => builder.checksum_sha256(checksum),
        _ => bail!("unsupported checksum algorithm in test helper"),
    };
    Ok(builder.build())
}

fn head_checksum<'a>(
    output: &'a HeadObjectOutput,
    algorithm: &ChecksumAlgorithm,
) -> Option<&'a str> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => output.checksum_crc32(),
        ChecksumAlgorithm::Crc32C => output.checksum_crc32_c(),
        ChecksumAlgorithm::Crc64Nvme => output.checksum_crc64_nvme(),
        ChecksumAlgorithm::Sha1 => output.checksum_sha1(),
        ChecksumAlgorithm::Sha256 => output.checksum_sha256(),
        _ => None,
    }
}

fn object_attributes_checksum<'a>(
    checksum: &'a Checksum,
    algorithm: &ChecksumAlgorithm,
) -> Option<&'a str> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => checksum.checksum_crc32(),
        ChecksumAlgorithm::Crc32C => checksum.checksum_crc32_c(),
        ChecksumAlgorithm::Crc64Nvme => checksum.checksum_crc64_nvme(),
        ChecksumAlgorithm::Sha1 => checksum.checksum_sha1(),
        ChecksumAlgorithm::Sha256 => checksum.checksum_sha256(),
        _ => None,
    }
}

fn get_object_checksum<'a>(
    output: &'a GetObjectOutput,
    algorithm: &ChecksumAlgorithm,
) -> Option<&'a str> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => output.checksum_crc32(),
        ChecksumAlgorithm::Crc32C => output.checksum_crc32_c(),
        ChecksumAlgorithm::Crc64Nvme => output.checksum_crc64_nvme(),
        ChecksumAlgorithm::Sha1 => output.checksum_sha1(),
        ChecksumAlgorithm::Sha256 => output.checksum_sha256(),
        _ => None,
    }
}

fn unquoted_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

async fn upload_plain_part(
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
