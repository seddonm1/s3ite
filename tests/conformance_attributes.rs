//! Real-binary HTTP object attributes conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --test conformance_attributes`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::Result;
use aws_sdk_s3::{
    Client,
    operation::get_object_attributes::GetObjectAttributesOutput,
    primitives::ByteStream,
    types::{
        BucketVersioningStatus, ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart,
        ObjectAttributes, StorageClass,
    },
};
use serial_test::serial;
use support::{
    fixtures::{bucket_name, create_bucket, put_bucket_versioning},
    sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;
const SHA256_5_MIB_A: &str = "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=";
const SHA256_5_MIB_B: &str = "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=";
const SHA256_5_MIB_C: &str = "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=";
const SHA256_15_MIB_ABC_COMPOSITE: &str = "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3";
const SHA256_1024_A: &str = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

/// Verifies GetObjectAttributes returns basic current-object fields.
#[tokio::test]
#[serial(conformance)]
async fn test_get_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await?;
    let etag = put
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?;

    let response = get_all_attributes(&client, &bucket, key).await?;
    assert_eq!(response.delete_marker(), None);
    assert_eq!(response.version_id(), None);
    assert_basic_attributes(&response, 3, etag);
    assert!(response.object_parts().is_none());

    Ok(())
}

/// Verifies GetObjectAttributes returns an explicit object checksum.
#[tokio::test]
#[serial(conformance)]
async fn test_get_checksum_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_checksum_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "myobj";

    create_bucket(&client, &bucket).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(SHA256_1024_A)
        .body(ByteStream::from(vec![b'A'; 1024]))
        .send()
        .await?;
    assert_eq!(put.checksum_sha256(), Some(SHA256_1024_A));
    let etag = put
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?;

    let response = get_all_attributes(&client, &bucket, key).await?;
    assert_basic_attributes(&response, 1024, etag);
    let checksum = response
        .checksum()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing checksum"))?;
    assert_eq!(checksum.checksum_sha256(), Some(SHA256_1024_A));
    assert!(response.object_parts().is_none());

    Ok(())
}

/// Verifies GetObjectAttributes returns exact object-version attributes.
#[tokio::test]
#[serial(conformance)]
async fn test_get_versioned_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_versioned_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "obj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"foo"))
        .send()
        .await?;
    let etag = put
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("PutObject response missing ETag"))?
        .to_string();
    let version = put
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("versioned PutObject response missing VersionId"))?
        .to_string();

    let current = get_all_attributes(&client, &bucket, key).await?;
    assert_eq!(current.delete_marker(), None);
    assert_eq!(current.version_id(), Some(version.as_str()));
    assert_basic_attributes(&current, 3, &etag);
    assert!(current.object_parts().is_none());

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await?;

    let exact = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .version_id(&version)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::Checksum)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::StorageClass)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await?;
    assert_eq!(exact.delete_marker(), None);
    assert_eq!(exact.version_id(), Some(version.as_str()));
    assert_basic_attributes(&exact, 3, &etag);
    assert!(exact.object_parts().is_none());

    Ok(())
}

/// Verifies GetObjectAttributes returns multipart object parts.
#[tokio::test]
#[serial(conformance)]
async fn test_get_multipart_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_multipart_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "multipart";
    let part_count = 6;
    let total_size = i64::from(part_count) * 5 * i64::try_from(MIB)?;

    create_bucket(&client, &bucket).await?;
    let etag = complete_plain_multipart(&client, &bucket, key, part_count).await?;

    let response = get_all_attributes(&client, &bucket, key).await?;
    assert_basic_attributes(&response, total_size, &etag);
    let parts = response
        .object_parts()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing object parts"))?;
    assert_eq!(parts.total_parts_count(), Some(part_count));
    assert_eq!(parts.is_truncated(), Some(false));
    assert_eq!(parts.parts().len(), usize::try_from(part_count)?);
    for (index, part) in parts.parts().iter().enumerate() {
        assert_eq!(part.part_number(), Some(i32::try_from(index + 1)?));
        assert_eq!(part.size(), Some(5 * i64::try_from(MIB)?));
        assert!(part.checksum_sha256().is_none());
    }

    Ok(())
}

/// Verifies GetObjectAttributes paginates multipart object parts.
#[tokio::test]
#[serial(conformance)]
async fn test_get_paginated_multipart_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_paginated_multipart_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "multipart";
    let part_count = 6;
    let total_size = i64::from(part_count) * 5 * i64::try_from(MIB)?;

    create_bucket(&client, &bucket).await?;
    let etag = complete_plain_multipart(&client, &bucket, key, part_count).await?;

    let first_page = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::Checksum)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::StorageClass)
        .object_attributes(ObjectAttributes::ObjectSize)
        .max_parts(1)
        .part_number_marker("3")
        .send()
        .await?;
    assert_basic_attributes(&first_page, total_size, &etag);
    let parts = first_page
        .object_parts()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing object parts"))?;
    assert_eq!(parts.total_parts_count(), Some(part_count));
    assert_eq!(parts.max_parts(), Some(1));
    assert_eq!(parts.part_number_marker(), Some("3"));
    assert_eq!(parts.is_truncated(), Some(true));
    assert_eq!(parts.next_part_number_marker(), Some("4"));
    assert_eq!(parts.parts().len(), 1);
    assert_eq!(parts.parts()[0].part_number(), Some(4));
    assert_eq!(parts.parts()[0].size(), Some(5 * i64::try_from(MIB)?));
    assert!(parts.parts()[0].checksum_sha256().is_none());

    let second_page = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::Checksum)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::StorageClass)
        .object_attributes(ObjectAttributes::ObjectSize)
        .max_parts(10)
        .part_number_marker("4")
        .send()
        .await?;
    assert_basic_attributes(&second_page, total_size, &etag);
    let parts = second_page
        .object_parts()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing object parts"))?;
    assert_eq!(parts.total_parts_count(), Some(part_count));
    assert_eq!(parts.max_parts(), Some(10));
    assert_eq!(parts.part_number_marker(), Some("4"));
    assert_eq!(parts.is_truncated(), Some(false));
    assert_eq!(parts.next_part_number_marker(), None);
    assert_eq!(parts.parts().len(), 2);
    for (index, part) in parts.parts().iter().enumerate() {
        assert_eq!(part.part_number(), Some(i32::try_from(index + 5)?));
        assert_eq!(part.size(), Some(5 * i64::try_from(MIB)?));
        assert!(part.checksum_sha256().is_none());
    }

    Ok(())
}

/// Verifies GetObjectAttributes returns a single multipart object part.
#[tokio::test]
#[serial(conformance)]
async fn test_get_single_multipart_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_single_multipart_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "multipart";

    create_bucket(&client, &bucket).await?;
    let etag = complete_plain_multipart(&client, &bucket, key, 1).await?;

    let response = get_all_attributes(&client, &bucket, key).await?;
    assert_basic_attributes(&response, 5 * i64::try_from(MIB)?, &etag);
    let parts = response
        .object_parts()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing object parts"))?;
    assert_eq!(parts.total_parts_count(), Some(1));
    assert_eq!(parts.parts().len(), 1);
    assert_eq!(parts.parts()[0].part_number(), Some(1));
    assert_eq!(parts.parts()[0].size(), Some(5 * i64::try_from(MIB)?));
    assert!(parts.parts()[0].checksum_sha256().is_none());

    Ok(())
}

/// Verifies GetObjectAttributes returns multipart checksums and part checksums.
#[tokio::test]
#[serial(conformance)]
async fn test_get_multipart_checksum_object_attributes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_multipart_checksum_object_attributes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-attr");
    let key = "multipart_checksum";
    let part_checksums = [SHA256_5_MIB_A, SHA256_5_MIB_B, SHA256_5_MIB_C];

    create_bucket(&client, &bucket).await?;
    let upload_id = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .content_type("text/plain")
        .metadata("foo", "bar")
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing UploadId"))?
        .to_string();
    let mut parts = Vec::with_capacity(part_checksums.len());
    for (index, checksum) in part_checksums.into_iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let byte = [b'A', b'B', b'C'][index];
        let response = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(checksum)
            .body(ByteStream::from(vec![byte; 5 * MIB]))
            .send()
            .await?;
        parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(
                    response
                        .e_tag()
                        .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
                )
                .checksum_sha256(checksum)
                .build(),
        );
    }
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let completed = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await?;
    assert_eq!(
        completed.checksum_sha256(),
        Some(SHA256_15_MIB_ABC_COMPOSITE)
    );

    let response = get_all_attributes(&client, &bucket, key).await?;
    assert_eq!(response.object_size(), Some(15 * i64::try_from(MIB)?));
    let checksum = response
        .checksum()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing checksum"))?;
    assert_eq!(
        checksum.checksum_sha256(),
        Some(SHA256_15_MIB_ABC_COMPOSITE)
    );
    let parts = response
        .object_parts()
        .ok_or_else(|| anyhow::anyhow!("GetObjectAttributes response missing object parts"))?;
    assert_eq!(parts.total_parts_count(), Some(3));
    for (index, part) in parts.parts().iter().enumerate() {
        assert_eq!(part.part_number(), Some(i32::try_from(index + 1)?));
        assert_eq!(part.size(), Some(5 * i64::try_from(MIB)?));
        assert_eq!(part.checksum_sha256(), Some(part_checksums[index]));
    }

    Ok(())
}

async fn get_all_attributes(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<GetObjectAttributesOutput> {
    Ok(client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::Checksum)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::StorageClass)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await?)
}

fn assert_basic_attributes(output: &GetObjectAttributesOutput, size: i64, etag: &str) {
    assert_eq!(output.object_size(), Some(size));
    assert_eq!(output.e_tag(), Some(unquoted_etag(etag)));
    assert_eq!(output.storage_class(), Some(&StorageClass::Standard));
}

async fn complete_plain_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    part_count: i32,
) -> Result<String> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing UploadId"))?
        .to_string();

    let mut parts = Vec::with_capacity(usize::try_from(part_count)?);
    for part_number in 1..=part_count {
        let response = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(vec![
                b'A' + u8::try_from(part_number - 1)?;
                5 * MIB
            ]))
            .send()
            .await?;
        parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(
                    response
                        .e_tag()
                        .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
                )
                .build(),
        );
    }

    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let completed = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await?;
    Ok(completed
        .e_tag()
        .ok_or_else(|| anyhow::anyhow!("complete multipart response missing ETag"))?
        .to_string())
}

fn unquoted_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}
