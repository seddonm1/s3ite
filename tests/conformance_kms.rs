//! Ceph SSE-KMS cases run through the S3 and KMS HTTP APIs.

mod support;

use anyhow::{Context, Result};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{ChecksumMode, CompletedMultipartUpload, CompletedPart, ServerSideEncryption},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::kms::{ALGORITHM, KEY_ID, KmsServer, PRIMARY, SECONDARY, set_default};
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_object},
    sdk,
    sse::{MIB, assert_storage_does_not_contain, copy_source, signed},
};

/// Creates a fresh bucket on an isolated pair of S3 and KMS services.
async fn setup() -> Result<(KmsServer, Client, String)> {
    let kms = KmsServer::start().await?;
    let client = sdk::client(&kms.server);
    let bucket = bucket_name("s3ite-kms");
    create_bucket(&client, &bucket).await?;
    Ok((kms, client, bucket))
}

/// Checks the KMS response headers and bytes on explicit and bucket-default transfers.
async fn transfer(size: usize, default: bool) -> Result<()> {
    let (kms, client, bucket) = setup().await?;
    let body = vec![b'A'; size];
    let mut put = client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .body(ByteStream::from(body.clone()));
    if default {
        set_default(&client, &bucket).await?;
    } else {
        put = put
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(PRIMARY);
    }
    let put = put.send().await?;
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(put.ssekms_key_id(), Some(PRIMARY));
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(
        get.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(get.ssekms_key_id(), Some(PRIMARY));
    assert_eq!(get.body.collect().await?.into_bytes().as_ref(), body);
    if size >= 32 {
        assert_storage_does_not_contain(&kms.server, &bucket, &body[..32])?;
    }
    Ok(())
}

/// Checks invalid writes and confirms the original plaintext object survives.
async fn rejected(headers: &[(&str, &str)]) -> Result<()> {
    let (kms, client, bucket) = setup().await?;
    put_object(&client, &bucket, "object", b"original").await?;
    let response = signed(
        &kms.server,
        Method::PUT,
        &format!("/{bucket}/object"),
        vec![],
        headers,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        response
            .text()
            .await?
            .contains("<Code>InvalidArgument</Code>")
    );
    assert_eq!(
        get_object_bytes(&client, &bucket, "object").await?,
        b"original"
    );
    Ok(())
}

/// Exercises initiation, repeated part headers, completion retries, and cross-part ranges.
async fn multipart(part_key: &str, finish: bool) -> Result<()> {
    let (kms, client, bucket) = setup().await?;
    let create = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("object")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(PRIMARY)
        .content_type("text/plain")
        .metadata("foo", "bar")
        .send()
        .await?;
    let id = create.upload_id().context("missing upload ID")?;
    let path = format!("/{bucket}/object?uploadId={id}");
    let mut xml = String::from("<CompleteMultipartUpload>");
    let mut data = Vec::new();
    for (part, byte) in [(1, b'A'), (2, b'B')] {
        let body = vec![byte; 5 * MIB];
        data.extend_from_slice(&body);
        let result = signed(
            &kms.server,
            Method::PUT,
            &format!("{path}&partNumber={part}"),
            body,
            &[(ALGORITHM, "aws:kms"), (KEY_ID, part_key)],
        )
        .await?;
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.headers()[KEY_ID], PRIMARY);
        let etag = result.headers()["etag"].to_str()?;
        xml.push_str(&format!(
            "<Part><PartNumber>{part}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    if !finish {
        return Ok(());
    }
    xml.push_str("</CompleteMultipartUpload>");
    for _ in 0..2 {
        let result = signed(
            &kms.server,
            Method::POST,
            &path,
            xml.as_bytes().to_vec(),
            &[(ALGORITHM, "aws:kms"), (KEY_ID, PRIMARY)],
        )
        .await?;
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.headers()[KEY_ID], PRIMARY);
        assert!(!result.text().await?.contains("<Error>"));
    }
    let listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("object")
        .send()
        .await?;
    assert_eq!(listing.contents().len(), 1);
    assert_eq!(listing.contents()[0].size(), Some(data.len().try_into()?));
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(get.content_length(), Some(data.len().try_into()?));
    assert_eq!(get.content_type(), Some("text/plain"));
    assert_eq!(
        get.metadata()
            .and_then(|m| m.get("foo"))
            .map(String::as_str),
        Some("bar")
    );
    assert_eq!(get.body.collect().await?.into_bytes().as_ref(), data);
    for (start, end) in [(0, 999_999), (5 * MIB - 1, 5 * MIB + 1), (1, 10_000_000)] {
        let range = client
            .get_object()
            .bucket(&bucket)
            .key("object")
            .range(format!("bytes={start}-{end}"))
            .send()
            .await?;
        assert_eq!(
            range.body.collect().await?.into_bytes().as_ref(),
            &data[start..=end]
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Mode {
    Plain,
    Local,
    Kms,
}

/// Returns the expected algorithm and key on each side of a copy.
fn settings(mode: Mode, key: &str) -> (Option<ServerSideEncryption>, Option<&str>) {
    match mode {
        Mode::Plain => (None, None),
        Mode::Local => (Some(ServerSideEncryption::Aes256), None),
        Mode::Kms => (Some(ServerSideEncryption::AwsKms), Some(key)),
    }
}

/// Checks cross-bucket copies for each supported source/destination encryption pair.
async fn copy(source: Mode, destination: Mode, size: usize, as_part: bool) -> Result<()> {
    let (_kms, client, bucket) = setup().await?;
    let dest = bucket_name("s3ite-kms-copy");
    create_bucket(&client, &dest).await?;
    let (algorithm, key) = settings(source, PRIMARY);
    let mut body = vec![b'A'; size];
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("src")
        .set_server_side_encryption(algorithm.clone())
        .set_ssekms_key_id(key.map(str::to_owned))
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_eq!(
        (put.server_side_encryption(), put.ssekms_key_id()),
        (algorithm.as_ref(), key)
    );
    let (algorithm, key) = settings(destination, SECONDARY);
    let expected = (algorithm.as_ref(), key);
    let src = copy_source(&bucket, "src");
    if as_part {
        let create = client
            .create_multipart_upload()
            .bucket(&dest)
            .key("dst")
            .set_server_side_encryption(algorithm.clone())
            .set_ssekms_key_id(key.map(str::to_owned))
            .send()
            .await?;
        assert_eq!(
            (create.server_side_encryption(), create.ssekms_key_id()),
            expected
        );
        let id = create.upload_id().context("missing upload ID")?;
        let copied = client
            .upload_part_copy()
            .bucket(&dest)
            .key("dst")
            .upload_id(id)
            .part_number(1)
            .copy_source(src)
            .send()
            .await?;
        assert_eq!(
            (copied.server_side_encryption(), copied.ssekms_key_id()),
            expected
        );
        let second = vec![b'B'; size];
        body.extend_from_slice(&second);
        let part = client
            .upload_part()
            .bucket(&dest)
            .key("dst")
            .upload_id(id)
            .part_number(2)
            .body(ByteStream::from(second))
            .send()
            .await?;
        assert_eq!(
            (part.server_side_encryption(), part.ssekms_key_id()),
            expected
        );
        let parts = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(
                        copied
                            .copy_part_result()
                            .and_then(|r| r.e_tag())
                            .context("missing copied ETag")?,
                    )
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .e_tag(part.e_tag().context("missing ETag")?)
                    .build(),
            )
            .build();
        let complete = client
            .complete_multipart_upload()
            .bucket(&dest)
            .key("dst")
            .upload_id(id)
            .multipart_upload(parts)
            .send()
            .await?;
        assert_eq!(
            (complete.server_side_encryption(), complete.ssekms_key_id()),
            expected
        );
    } else {
        let copied = client
            .copy_object()
            .bucket(&dest)
            .key("dst")
            .copy_source(src)
            .set_server_side_encryption(algorithm.clone())
            .set_ssekms_key_id(key.map(str::to_owned))
            .send()
            .await?;
        assert_eq!(
            (copied.server_side_encryption(), copied.ssekms_key_id()),
            expected
        );
    }
    let get = client.get_object().bucket(&dest).key("dst").send().await?;
    assert_eq!(
        (get.server_side_encryption(), get.ssekms_key_id()),
        expected
    );
    assert_eq!(get.body.collect().await?.into_bytes().as_ref(), body);
    Ok(())
}

/// Round-trips a 100-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_present() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_present
    transfer(100, false).await
}

/// Round-trips a 1-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_transfer_1b() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_transfer_1b
    transfer(1, false).await
}

/// Round-trips a 1024-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_transfer_1kb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_transfer_1kb
    transfer(1024, false).await
}

/// Round-trips a 1048576-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_transfer_1mb() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_sse_kms_transfer_1MB: Rust uses a lowercase test name
    transfer(1048576, false).await
}

/// Round-trips a 13-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_transfer_13b() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_transfer_13b
    transfer(13, false).await
}

/// Round-trips a 1-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_default_upload_1b() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_default_upload_1b
    transfer(1, true).await
}

/// Round-trips a 1024-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_default_upload_1kb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_default_upload_1kb
    transfer(1024, true).await
}

/// Round-trips a 1048576-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_default_upload_1mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_default_upload_1mb
    transfer(1048576, true).await
}

/// Round-trips a 8388608-byte encrypted object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_default_upload_8mb() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_default_upload_8mb
    transfer(8388608, true).await
}

/// Rejects inconsistent encryption headers before storing the replacement.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_no_key() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_no_key
    rejected(&[(ALGORITHM, "aws:kms")]).await
}

/// Rejects inconsistent encryption headers before storing the replacement.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_not_declared() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_not_declared
    rejected(&[(KEY_ID, PRIMARY)]).await
}

/// Rejects inconsistent encryption headers before storing the replacement.
#[tokio::test]
#[serial(conformance)]
async fn test_put_obj_enc_conflict_c_kms() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_obj_enc_conflict_c_kms
    rejected(&[
        (ALGORITHM, "aws:kms"),
        (KEY_ID, PRIMARY),
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        (
            "x-amz-server-side-encryption-customer-key",
            "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        ),
        (
            "x-amz-server-side-encryption-customer-key-md5",
            "DWygnHRtgiJ77HCm+1rvHw==",
        ),
    ])
    .await
}

/// Rejects inconsistent encryption headers before storing the replacement.
#[tokio::test]
#[serial(conformance)]
async fn test_put_obj_enc_conflict_s3_kms() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_obj_enc_conflict_s3_kms
    rejected(&[(ALGORITHM, "AES256"), (KEY_ID, PRIMARY)]).await
}

/// Rejects inconsistent encryption headers before storing the replacement.
#[tokio::test]
#[serial(conformance)]
async fn test_put_obj_enc_conflict_bad_enc_kms() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_obj_enc_conflict_bad_enc_kms
    rejected(&[(ALGORITHM, "aes:kms")]).await
}

/// Keeps the initiation key for every part.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_multipart_upload() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_sse_kms_multipart_upload: uses two 5 MiB parts
    multipart(PRIMARY, true).await
}

/// Keeps the initiation key for every part.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_multipart_invalid_chunks_1() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_sse_kms_multipart_invalid_chunks_1: uses two 5 MiB parts
    multipart(SECONDARY, false).await
}

/// Keeps the initiation key for every part.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_multipart_invalid_chunks_2() -> Result<()> {
    // Adapted from s3tests/functional/test_s3.py::test_sse_kms_multipart_invalid_chunks_2: uses two 5 MiB parts
    multipart("unknown", false).await
}

/// Copies 1 bytes from unencrypted to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_unencrypted_sse_kms_standard_standard_1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-1]
    copy(Mode::Plain, Mode::Kms, 1, false).await
}

/// Copies 1024 bytes from unencrypted to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_unencrypted_sse_kms_standard_standard_1024() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-1024]
    copy(Mode::Plain, Mode::Kms, 1024, false).await
}

/// Copies 1048576 bytes from unencrypted to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_unencrypted_sse_kms_standard_standard_1048576() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-1048576]
    copy(Mode::Plain, Mode::Kms, 1048576, false).await
}

/// Copies 8388608 bytes from unencrypted to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_unencrypted_sse_kms_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-8388608]
    copy(Mode::Plain, Mode::Kms, 8388608, false).await
}

/// Copies 8388608 bytes from unencrypted to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_part_enc_unencrypted_sse_kms_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[unencrypted-sse-kms-STANDARD-STANDARD-8388608]
    copy(Mode::Plain, Mode::Kms, 8388608, true).await
}

/// Copies 1 bytes from sse-s3 to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_s3_sse_kms_standard_standard_1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-1]
    copy(Mode::Local, Mode::Kms, 1, false).await
}

/// Copies 1024 bytes from sse-s3 to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_s3_sse_kms_standard_standard_1024() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-1024]
    copy(Mode::Local, Mode::Kms, 1024, false).await
}

/// Copies 1048576 bytes from sse-s3 to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_s3_sse_kms_standard_standard_1048576() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-1048576]
    copy(Mode::Local, Mode::Kms, 1048576, false).await
}

/// Copies 8388608 bytes from sse-s3 to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_s3_sse_kms_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-8388608]
    copy(Mode::Local, Mode::Kms, 8388608, false).await
}

/// Copies 8388608 bytes from sse-s3 to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_part_enc_sse_s3_sse_kms_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[sse-s3-sse-kms-STANDARD-STANDARD-8388608]
    copy(Mode::Local, Mode::Kms, 8388608, true).await
}

/// Copies 1 bytes from sse-kms to unencrypted.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_unencrypted_standard_standard_1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-1]
    copy(Mode::Kms, Mode::Plain, 1, false).await
}

/// Copies 1024 bytes from sse-kms to unencrypted.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_unencrypted_standard_standard_1024() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-1024]
    copy(Mode::Kms, Mode::Plain, 1024, false).await
}

/// Copies 1048576 bytes from sse-kms to unencrypted.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_unencrypted_standard_standard_1048576() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-1048576]
    copy(Mode::Kms, Mode::Plain, 1048576, false).await
}

/// Copies 8388608 bytes from sse-kms to unencrypted.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_unencrypted_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-8388608]
    copy(Mode::Kms, Mode::Plain, 8388608, false).await
}

/// Copies 8388608 bytes from sse-kms to unencrypted.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_part_enc_sse_kms_unencrypted_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[sse-kms-unencrypted-STANDARD-STANDARD-8388608]
    copy(Mode::Kms, Mode::Plain, 8388608, true).await
}

/// Copies 1 bytes from sse-kms to sse-s3.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_s3_standard_standard_1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-1]
    copy(Mode::Kms, Mode::Local, 1, false).await
}

/// Copies 1024 bytes from sse-kms to sse-s3.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_s3_standard_standard_1024() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-1024]
    copy(Mode::Kms, Mode::Local, 1024, false).await
}

/// Copies 1048576 bytes from sse-kms to sse-s3.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_s3_standard_standard_1048576() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-1048576]
    copy(Mode::Kms, Mode::Local, 1048576, false).await
}

/// Copies 8388608 bytes from sse-kms to sse-s3.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_s3_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-8388608]
    copy(Mode::Kms, Mode::Local, 8388608, false).await
}

/// Copies 8388608 bytes from sse-kms to sse-s3.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_part_enc_sse_kms_sse_s3_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[sse-kms-sse-s3-STANDARD-STANDARD-8388608]
    copy(Mode::Kms, Mode::Local, 8388608, true).await
}

/// Copies 1 bytes from sse-kms to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_kms_standard_standard_1() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-1]
    copy(Mode::Kms, Mode::Kms, 1, false).await
}

/// Copies 1024 bytes from sse-kms to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_kms_standard_standard_1024() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-1024]
    copy(Mode::Kms, Mode::Kms, 1024, false).await
}

/// Copies 1048576 bytes from sse-kms to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_kms_standard_standard_1048576() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-1048576]
    copy(Mode::Kms, Mode::Kms, 1048576, false).await
}

/// Copies 8388608 bytes from sse-kms to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_enc_sse_kms_sse_kms_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-8388608]
    copy(Mode::Kms, Mode::Kms, 8388608, false).await
}

/// Copies 8388608 bytes from sse-kms to sse-kms.
#[tokio::test]
#[serial(conformance)]
async fn test_copy_part_enc_sse_kms_sse_kms_standard_standard_8388608() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_copy_part_enc[sse-kms-sse-kms-STANDARD-STANDARD-8388608]
    copy(Mode::Kms, Mode::Kms, 8388608, true).await
}

/// Checks encrypted HEAD responses and rejects encryption declarations on reads.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_method_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_method_head
    let (kms, client, bucket) = setup().await?;
    client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(PRIMARY)
        .body(ByteStream::from(vec![b'A'; 1000]))
        .send()
        .await?;
    let head = client
        .head_object()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(head.ssekms_key_id(), Some(PRIMARY));
    let response = signed(
        &kms.server,
        Method::HEAD,
        &format!("/{bucket}/object"),
        vec![],
        &[(ALGORITHM, "aws:kms"), (KEY_ID, PRIMARY)],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

/// Rejects a KMS declaration on a GET of a plaintext object.
#[tokio::test]
#[serial(conformance)]
async fn test_sse_kms_read_declare() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_sse_kms_read_declare
    let (kms, client, bucket) = setup().await?;
    put_object(&client, &bucket, "object", b"plaintext").await?;
    let response = signed(
        &kms.server,
        Method::GET,
        &format!("/{bucket}/object"),
        vec![],
        &[(ALGORITHM, "aws:kms"), (KEY_ID, PRIMARY)],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

/// Installs a KMS bucket encryption rule.
#[tokio::test]
#[serial(conformance)]
async fn test_put_bucket_encryption_kms() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_put_bucket_encryption_kms
    let (_kms, client, bucket) = setup().await?;
    set_default(&client, &bucket).await
}

/// Verifies the KMS key and algorithm in the bucket encryption response.
#[tokio::test]
#[serial(conformance)]
async fn test_get_bucket_encryption_kms() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_get_bucket_encryption_kms
    let (_kms, client, bucket) = setup().await?;
    let get = client.get_bucket_encryption().bucket(&bucket);
    assert_error_contains(
        &get.clone().send().await.unwrap_err(),
        "ServerSideEncryptionConfigurationNotFoundError",
    );
    set_default(&client, &bucket).await?;
    let result = get.send().await?;
    let default = result
        .server_side_encryption_configuration()
        .and_then(|c| c.rules().first())
        .and_then(|r| r.apply_server_side_encryption_by_default())
        .context("missing default")?;
    assert_eq!(default.sse_algorithm(), &ServerSideEncryption::AwsKms);
    assert_eq!(default.kms_master_key_id(), Some(PRIMARY));
    Ok(())
}

/// Deletes the bucket encryption rule before and after configuring it.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_bucket_encryption_kms() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_bucket_encryption_kms
    let (kms, client, bucket) = setup().await?;
    for configured in [false, true] {
        if configured {
            set_default(&client, &bucket).await?;
        }
        let response = signed(
            &kms.server,
            Method::DELETE,
            &format!("/{bucket}?encryption"),
            vec![],
            &[],
        )
        .await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    assert_error_contains(
        &client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap_err(),
        "ServerSideEncryptionConfigurationNotFoundError",
    );
    Ok(())
}

/// Checks context binding, provider error propagation, and failed overwrite atomicity.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_kms_context_and_provider_failures() -> Result<()> {
    let (kms, client, bucket) = setup().await?;
    let put = || {
        client
            .put_object()
            .bucket(&bucket)
            .key("object")
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(PRIMARY)
    };
    for bad_context in [
        "?".to_owned(),
        STANDARD.encode(b"[]"),
        STANDARD.encode(br#"{"aws:s3:arn":"different-object"}"#),
    ] {
        let error = put()
            .ssekms_encryption_context(bad_context)
            .send()
            .await
            .unwrap_err();
        assert_error_contains(&error, "InvalidArgument");
    }
    put()
        .ssekms_encryption_context(STANDARD.encode(br#"{"tenant":"example"}"#))
        .body(ByteStream::from_static(b"original"))
        .send()
        .await?;
    let db = rusqlite::Connection::open(kms.server.data_root().join(format!("{bucket}.sqlite3")))?;
    let encoded: String = db.query_row("SELECT encryption FROM object_version", [], |row| {
        row.get(0)
    })?;
    let stored: serde_json::Value = serde_json::from_str(&encoded)?;
    assert_eq!(stored["Kms"]["context"]["tenant"], "example");
    assert_eq!(
        stored["Kms"]["context"]["aws:s3:arn"],
        format!("arn:aws:s3:::{bucket}/object")
    );
    assert_error_contains(
        &put().ssekms_key_id("unknown").send().await.unwrap_err(),
        "InvalidArgument",
    );
    for (provider, code) in [
        ("AccessDeniedException", "AccessDenied"),
        ("InvalidCiphertextException", "InvalidRequest"),
        ("DependencyTimeoutException", "ServiceUnavailable"),
    ] {
        kms.fail_with(Some(provider));
        assert_error_contains(&put().send().await.unwrap_err(), code);
        assert_error_contains(
            &client
                .get_object()
                .bucket(&bucket)
                .key("object")
                .send()
                .await
                .unwrap_err(),
            code,
        );
    }
    kms.fail_with(Some("AccessDeniedException"));
    let denied = client
        .head_object()
        .bucket(&bucket)
        .key("object")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        denied
            .raw_response()
            .context("missing HTTP response")?
            .status()
            .as_u16(),
        403
    );
    client
        .head_object()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    kms.fail_with(None);
    assert_eq!(
        get_object_bytes(&client, &bucket, "object").await?,
        b"original"
    );
    Ok(())
}

/// Re-encrypts an object in place and checks the new key and encryption context.
#[tokio::test]
#[serial(conformance)]
async fn s3ite_rotate_object_kms_key() -> Result<()> {
    let (_kms, client, bucket) = setup().await?;
    let context = STANDARD.encode(br#"{"generation":"one"}"#);
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("object")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("alias/primary")
        .ssekms_encryption_context(&context)
        .body(ByteStream::from_static(b"original"))
        .send()
        .await?;
    assert_eq!(put.ssekms_key_id(), Some(PRIMARY));
    assert_eq!(put.ssekms_encryption_context(), Some(context.as_str()));
    let context = STANDARD.encode(br#"{"generation":"two"}"#);
    let copy = client
        .copy_object()
        .bucket(&bucket)
        .key("object")
        .copy_source(copy_source(&bucket, "object"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(SECONDARY)
        .ssekms_encryption_context(&context)
        .send()
        .await?;
    assert_eq!(copy.ssekms_encryption_context(), Some(context.as_str()));
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("object")
        .send()
        .await?;
    assert_eq!(get.ssekms_key_id(), Some(SECONDARY));
    assert_eq!(get.body.collect().await?.into_bytes().as_ref(), b"original");
    Ok(())
}
