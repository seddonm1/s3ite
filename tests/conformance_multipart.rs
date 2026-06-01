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
    types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart},
};
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_bucket_versioning, put_object},
    raw_http, sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;

/// Verifies multipart upload completion is idempotent and materializes bytes.
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

/// Verifies empty multipart completion bodies are rejected as malformed XML.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_empty() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_empty
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let response = raw_http::signed_request(
        &server,
        Method::POST,
        &format!(
            "/{}/{}?uploadId={upload_id}",
            raw_http::path_segment(&bucket),
            raw_http::path_segment(key)
        ),
        Vec::new(),
    )
    .await?;

    assert_status(&response, StatusCode::BAD_REQUEST);
    let body = response.text().await?;
    assert!(
        body.contains("MalformedXML"),
        "expected MalformedXML error, got {body}"
    );

    Ok(())
}

/// Verifies completing an unknown upload ID fails with NoSuchUpload.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_complete_without_create() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_complete_without_create
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let part = CompletedPart::builder()
        .e_tag("1234")
        .part_number(1)
        .build();
    let error = match complete_upload(&client, &bucket, key, "abc1234def", vec![part]).await {
        Ok(_) => bail!("completion without a created upload should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchUpload");

    Ok(())
}

/// Verifies a one-byte single-part multipart upload can complete.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_small() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_small
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let parts = vec![upload_part(&client, &bucket, key, &upload_id, 1, vec![0]).await?];

    complete_upload(&client, &bucket, key, &upload_id, parts.clone()).await?;
    complete_upload(&client, &bucket, key, &upload_id, parts).await?;

    let body = get_object_bytes(&client, &bucket, key).await?;
    assert_eq!(body, vec![0]);

    Ok(())
}

/// Verifies active multipart uploads are listed and aborted uploads disappear.
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

/// Verifies multipart upload listings include the synthetic owner identity.
#[tokio::test]
#[serial(conformance)]
async fn test_list_multipart_upload_owner() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_list_multipart_upload_owner
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, "mymultipart").await?;

    let response = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await?;
    let upload = response
        .uploads()
        .iter()
        .find(|upload| upload.upload_id() == Some(upload_id.as_str()))
        .ok_or_else(|| anyhow::anyhow!("multipart listing missing created upload"))?;
    assert_eq!(upload.owner().and_then(|owner| owner.id()), Some("s3ite"));
    assert_eq!(
        upload.owner().and_then(|owner| owner.display_name()),
        Some("s3ite")
    );
    assert_eq!(
        upload.initiator().and_then(|initiator| initiator.id()),
        Some("s3ite")
    );
    assert_eq!(
        upload
            .initiator()
            .and_then(|initiator| initiator.display_name()),
        Some("s3ite")
    );

    abort_upload(&client, &bucket, "mymultipart", &upload_id).await?;

    Ok(())
}

/// Verifies completing with a missing part number fails with InvalidPart.
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

/// Verifies completing with the wrong part ETag fails with InvalidPart.
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

/// Verifies non-final multipart parts smaller than 5 MiB are rejected.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_size_too_small() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_size_too_small
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let (upload_id, parts, _body) =
        upload_patterned_parts(&client, &bucket, key, 100 * 1024, 10 * 1024).await?;

    let error = match complete_upload(&client, &bucket, key, &upload_id, parts).await {
        Ok(_) => bail!("completion with undersized non-final parts should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "EntityTooSmall");

    Ok(())
}

/// Verifies multipart uploads across several object sizes preserve bytes.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_multiple_sizes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_multiple_sizes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let sizes = [
        5 * MIB,
        5 * MIB + 100 * 1024,
        5 * MIB + 600 * 1024,
        10 * MIB,
        10 * MIB + 100 * 1024,
        10 * MIB + 600 * 1024,
    ];

    create_bucket(&client, &bucket).await?;
    for size in sizes {
        let key = format!("mymultipart-{size}");
        let (upload_id, parts, expected) =
            upload_patterned_parts(&client, &bucket, &key, size, 5 * MIB).await?;
        complete_upload(&client, &bucket, &key, &upload_id, parts).await?;

        let body = get_object_bytes(&client, &bucket, &key).await?;
        assert_eq!(body, expected);
    }

    Ok(())
}

/// Verifies completed multipart object content is the concatenation of parts.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_contents() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_contents
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";
    let chunks = vec![
        vec![b'a'; 5 * MIB],
        vec![b'b'; 5 * MIB],
        vec![b'c'; 5 * MIB],
        vec![b'd'; 3 * MIB],
    ];

    create_bucket(&client, &bucket).await?;
    let (upload_id, parts, expected) = upload_chunks(&client, &bucket, key, chunks).await?;
    complete_upload(&client, &bucket, key, &upload_id, parts).await?;

    let body = get_object_bytes(&client, &bucket, key).await?;
    assert_eq!(body, expected);

    Ok(())
}

/// Verifies completing a multipart upload overwrites an existing object.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_overwrite_existing_object() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_overwrite_existing_object
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";
    let original = vec![b'x'; 5 * MIB];

    create_bucket(&client, &bucket).await?;
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.clone()))
        .send()
        .await?;

    let chunks = vec![original.clone(), original.clone()];
    let (upload_id, parts, expected) = upload_chunks(&client, &bucket, key, chunks).await?;
    complete_upload(&client, &bucket, key, &upload_id, parts).await?;

    let body = get_object_bytes(&client, &bucket, key).await?;
    assert_eq!(body, expected);

    Ok(())
}

/// Verifies multipart copy can copy a one-byte source object.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_small() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_small
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-mpu-src");
    let target_bucket = bucket_name("s3ite-mpu-dst");
    let source_key = "foo";
    let target_key = "mymultipart";

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_object(&client, &source_bucket, source_key, b"x").await?;

    let (upload_id, parts) = multipart_copy(
        &client,
        CopySourceRef::new(&source_bucket, source_key, None),
        &target_bucket,
        target_key,
        1,
    )
    .await?;
    complete_upload(&client, &target_bucket, target_key, &upload_id, parts).await?;

    let body = get_object_bytes(&client, &target_bucket, target_key).await?;
    assert_eq!(body, b"x");

    Ok(())
}

/// Verifies multipart copy rejects unsatisfiable source ranges.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_invalid_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_invalid_range
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "source", b"12345").await?;
    let upload_id = create_upload(&client, &bucket, "dest").await?;
    let error = match upload_part_copy(
        &client,
        &bucket,
        "dest",
        &upload_id,
        1,
        &copy_source(CopySourceRef::new(&bucket, "source", None)),
        Some("bytes=0-21".to_string()),
    )
    .await
    {
        Ok(_) => bail!("multipart copy with invalid range should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidRange");

    Ok(())
}

/// Verifies multipart copy rejects malformed source range headers.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_improper_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_improper_range
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "source", b"12345").await?;

    for copy_source_range in [
        "0-2",
        "bytes=0",
        "bytes=hello-world",
        "bytes=0-bar",
        "bytes=hello-",
        "bytes=0-2,3-5",
    ] {
        let upload_id = create_upload(&client, &bucket, "dest").await?;
        let error = match upload_part_copy(
            &client,
            &bucket,
            "dest",
            &upload_id,
            1,
            &copy_source(CopySourceRef::new(&bucket, "source", None)),
            Some(copy_source_range.to_string()),
        )
        .await
        {
            Ok(_) => bail!("multipart copy with malformed range should fail"),
            Err(error) => error,
        };
        assert_error_contains(&error, "InvalidArgument");
        abort_upload(&client, &bucket, "dest", &upload_id).await?;
    }

    Ok(())
}

/// Verifies multipart copy without a range copies the full source object.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_without_range() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_without_range
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-mpu-src");
    let target_bucket = bucket_name("s3ite-mpu-dst");
    let source_key = "source";
    let target_key = "mymultipartcopy";

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_object(&client, &source_bucket, source_key, b"0123456789").await?;
    let upload_id = create_upload(&client, &target_bucket, target_key).await?;
    let part = upload_part_copy(
        &client,
        &target_bucket,
        target_key,
        &upload_id,
        1,
        &copy_source(CopySourceRef::new(&source_bucket, source_key, None)),
        None,
    )
    .await?;

    complete_upload(&client, &target_bucket, target_key, &upload_id, vec![part]).await?;

    let body = get_object_bytes(&client, &target_bucket, target_key).await?;
    assert_eq!(body, b"0123456789");

    Ok(())
}

/// Verifies multipart copy source keys with special names are encoded correctly.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_special_names() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_special_names
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-mpu-src");
    let target_bucket = bucket_name("s3ite-mpu-dst");
    let target_key = "mymultipart";

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    for (index, source_key) in [" ", "_", "__", "?versionId"].into_iter().enumerate() {
        let body = vec![b'a' + u8::try_from(index)?];
        put_bytes_raw(&server, &source_bucket, source_key, body.clone()).await?;
        let (upload_id, parts) = multipart_copy(
            &client,
            CopySourceRef::new(&source_bucket, source_key, None),
            &target_bucket,
            target_key,
            body.len(),
        )
        .await?;
        complete_upload(&client, &target_bucket, target_key, &upload_id, parts).await?;

        let copied = get_object_bytes(&client, &target_bucket, target_key).await?;
        assert_eq!(copied, body);
    }

    Ok(())
}

/// Verifies UploadPartCopy decodes percent-encoded copy-source keys exactly once.
#[tokio::test]
#[serial(conformance)]
async fn test_upload_part_copy_percent_encoded_key() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_upload_part_copy_percent_encoded_key
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "anyfile.txt";
    let encoded_key = "anyfilename%25.txt";
    let raw_key = "anyfilename%.txt";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, encoded_key, b"foo").await?;
    put_object(&client, &bucket, key, b"foo").await?;

    let upload_id = create_upload(&client, &bucket, key).await?;
    let copy_source = format!("{bucket}/{}", raw_http::path_segment(raw_key));
    let error =
        match upload_part_copy(&client, &bucket, key, &upload_id, 1, &copy_source, None).await {
            Ok(_) => bail!("copy source must decode to the missing raw percent key"),
            Err(error) => error,
        };
    assert_error_contains(&error, "NoSuchKey");
    abort_upload(&client, &bucket, key, &upload_id).await?;

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"foo");

    Ok(())
}

/// Verifies multipart copy can copy exact source object versions.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_versioned() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_versioned
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-mpu-src");
    let target_bucket = bucket_name("s3ite-mpu-dst");
    let source_key = "foo";
    let target_key = "mymultipart";

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_bucket_versioning(&client, &source_bucket, BucketVersioningStatus::Enabled).await?;

    let mut versions = Vec::new();
    for byte in [b'a', b'b', b'c'] {
        let body = vec![byte; 6 * MIB];
        let version_id = put_bytes(&client, &source_bucket, source_key, body.clone())
            .await?
            .ok_or_else(|| anyhow::anyhow!("versioned PutObject missing version ID"))?;
        versions.push((version_id, body));
    }

    for (version_id, expected) in versions {
        let (upload_id, parts) = multipart_copy(
            &client,
            CopySourceRef::new(&source_bucket, source_key, Some(&version_id)),
            &target_bucket,
            target_key,
            expected.len(),
        )
        .await?;
        complete_upload(&client, &target_bucket, target_key, &upload_id, parts).await?;

        let copied = get_object_bytes(&client, &target_bucket, target_key).await?;
        assert_eq!(copied, expected);
    }

    Ok(())
}

/// Verifies multipart copy preserves copied byte prefixes across several sizes.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_copy_multiple_sizes() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_copy_multiple_sizes
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-mpu-src");
    let target_bucket = bucket_name("s3ite-mpu-dst");
    let source_key = "foo";
    let target_key = "mymultipart";
    let source_body = patterned_body(12 * MIB, 5 * MIB);
    let sizes = [
        5 * MIB,
        5 * MIB + 100 * 1024,
        5 * MIB + 600 * 1024,
        10 * MIB,
        10 * MIB + 100 * 1024,
        10 * MIB + 600 * 1024,
    ];

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_bytes(&client, &source_bucket, source_key, source_body.clone()).await?;
    for size in sizes {
        let (upload_id, parts) = multipart_copy(
            &client,
            CopySourceRef::new(&source_bucket, source_key, None),
            &target_bucket,
            target_key,
            size,
        )
        .await?;
        complete_upload(&client, &target_bucket, target_key, &upload_id, parts).await?;

        let copied = get_object_bytes(&client, &target_bucket, target_key).await?;
        assert_eq!(copied.as_slice(), &source_body[..size]);
    }

    Ok(())
}

/// Verifies completed multipart partNumber reads return exact part bytes.
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

/// Verifies single-part multipart partNumber reads and out-of-range errors.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_single_get_part() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_single_get_part
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";
    let body = vec![b'a'; 5 * MIB];

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let parts = vec![upload_part(&client, &bucket, key, &upload_id, 1, body.clone()).await?];

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
    let head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(head.parts_count(), Some(1));
    assert_eq!(head.e_tag(), complete.e_tag());

    let part = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .send()
        .await?;
    assert_eq!(part.parts_count(), Some(1));
    assert_eq!(part.e_tag(), complete.e_tag());
    assert_eq!(part.content_length(), Some(body.len() as i64));
    assert_eq!(
        part.body.collect().await?.into_bytes().as_ref(),
        body.as_slice()
    );

    let invalid = match client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(5)
        .send()
        .await
    {
        Ok(_) => bail!("partNumber beyond multipart object should fail"),
        Err(error) => error,
    };
    assert_error_contains(&invalid, "InvalidPart");

    Ok(())
}

/// Verifies duplicate part entries complete with the final uploaded part.
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

/// Verifies resending uploaded parts leaves completion with the final part data.
#[tokio::test]
#[serial(conformance)]
async fn test_multipart_upload_resend_part() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_multipart_upload_resend_part
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let resend_sets: &[&[usize]] = &[&[0], &[1], &[2], &[1, 2], &[0, 1, 2, 3, 4, 5]];
    let chunks = (0..6)
        .map(|index| Ok(vec![b'a' + u8::try_from(index)?; 5 * MIB]))
        .collect::<Result<Vec<_>>>()?;
    let expected = chunks.concat();

    create_bucket(&client, &bucket).await?;
    for (case_index, resend_parts) in resend_sets.iter().enumerate() {
        let key = format!("mymultipart-{case_index}");
        let upload_id = create_upload(&client, &bucket, &key).await?;
        let mut parts = Vec::with_capacity(chunks.len());

        for (index, chunk) in chunks.iter().enumerate() {
            let part_number = i32::try_from(index + 1)?;
            parts.push(
                upload_part(
                    &client,
                    &bucket,
                    &key,
                    &upload_id,
                    part_number,
                    chunk.clone(),
                )
                .await?,
            );
        }
        for index in resend_parts.iter().copied() {
            let part_number = i32::try_from(index + 1)?;
            parts[index] = upload_part(
                &client,
                &bucket,
                &key,
                &upload_id,
                part_number,
                chunks[index].clone(),
            )
            .await?;
        }

        complete_upload(&client, &bucket, &key, &upload_id, parts).await?;
        assert_eq!(
            get_object_bytes(&client, &bucket, &key).await?,
            expected.as_slice()
        );
    }

    Ok(())
}

/// Verifies aborting a multipart upload leaves no completed object behind.
#[tokio::test]
#[serial(conformance)]
async fn test_abort_multipart_upload() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_abort_multipart_upload
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    upload_part(&client, &bucket, key, &upload_id, 1, vec![b'a'; 5 * MIB]).await?;
    upload_part(&client, &bucket, key, &upload_id, 2, vec![b'b'; MIB]).await?;

    abort_upload(&client, &bucket, key, &upload_id).await?;

    let response = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(key)
        .send()
        .await?;
    assert!(response.contents().is_empty());

    Ok(())
}

/// Verifies aborting an unknown upload ID fails with NoSuchUpload.
#[tokio::test]
#[serial(conformance)]
async fn test_abort_multipart_upload_not_found() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_abort_multipart_upload_not_found
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-mpu");
    let key = "mymultipart";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, key, b"exists").await?;
    let error = match abort_upload(&client, &bucket, key, "56788").await {
        Ok(()) => bail!("abort with unknown upload ID should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchUpload");

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

async fn upload_patterned_parts(
    client: &Client,
    bucket: &str,
    key: &str,
    size: usize,
    part_size: usize,
) -> Result<(String, Vec<CompletedPart>, Vec<u8>)> {
    let mut chunks = Vec::new();
    let mut remaining = size;
    let mut part_index = 0usize;
    while remaining > 0 {
        let size = remaining.min(part_size);
        let byte = b'a' + u8::try_from(part_index % 26)?;
        chunks.push(vec![byte; size]);
        remaining -= size;
        part_index += 1;
    }
    upload_chunks(client, bucket, key, chunks).await
}

async fn upload_chunks(
    client: &Client,
    bucket: &str,
    key: &str,
    chunks: Vec<Vec<u8>>,
) -> Result<(String, Vec<CompletedPart>, Vec<u8>)> {
    let upload_id = create_upload(client, bucket, key).await?;
    let mut parts = Vec::with_capacity(chunks.len());
    let mut body = Vec::new();

    for (index, chunk) in chunks.into_iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        body.extend_from_slice(&chunk);
        parts.push(upload_part(client, bucket, key, &upload_id, part_number, chunk).await?);
    }

    Ok((upload_id, parts, body))
}

#[derive(Clone, Copy)]
struct CopySourceRef<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Option<&'a str>,
}

impl<'a> CopySourceRef<'a> {
    fn new(bucket: &'a str, key: &'a str, version_id: Option<&'a str>) -> Self {
        Self {
            bucket,
            key,
            version_id,
        }
    }
}

async fn multipart_copy(
    client: &Client,
    source: CopySourceRef<'_>,
    target_bucket: &str,
    target_key: &str,
    size: usize,
) -> Result<(String, Vec<CompletedPart>)> {
    let upload_id = create_upload(client, target_bucket, target_key).await?;
    let mut parts = Vec::new();
    let copy_source = copy_source(source);
    let mut offset = 0usize;

    while offset < size {
        let part_number = i32::try_from(parts.len() + 1)?;
        let end = (offset + 5 * MIB).min(size) - 1;
        let range = format!("bytes={offset}-{end}");
        parts.push(
            upload_part_copy(
                client,
                target_bucket,
                target_key,
                &upload_id,
                part_number,
                &copy_source,
                Some(range),
            )
            .await?,
        );
        offset = end + 1;
    }

    Ok((upload_id, parts))
}

async fn upload_part_copy(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    copy_source: &str,
    copy_source_range: Option<String>,
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
        .ok_or_else(|| anyhow::anyhow!("upload part copy response missing result"))?;
    Ok(CompletedPart::builder()
        .e_tag(
            result
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part copy response missing ETag"))?,
        )
        .part_number(part_number)
        .build())
}

fn copy_source(source: CopySourceRef<'_>) -> String {
    let mut copy_source = format!("{}/{}", source.bucket, raw_http::path_segment(source.key));
    if let Some(version_id) = source.version_id {
        copy_source.push_str("?versionId=");
        copy_source.push_str(&raw_http::path_segment(version_id));
    }
    copy_source
}

async fn put_bytes(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<Option<String>> {
    Ok(client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await?
        .version_id()
        .map(ToOwned::to_owned))
}

async fn put_bytes_raw(server: &TestServer, bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
    let response = raw_http::signed_request(
        server,
        Method::PUT,
        &format!(
            "/{}/{}",
            raw_http::path_segment(bucket),
            raw_http::path_segment(key)
        ),
        body,
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    Ok(())
}

fn patterned_body(size: usize, part_size: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(size);
    let mut remaining = size;
    let mut part_index = 0usize;
    while remaining > 0 {
        let size = remaining.min(part_size);
        let byte = b'a' + (part_index % 26) as u8;
        body.extend(std::iter::repeat_n(byte, size));
        remaining -= size;
        part_index += 1;
    }
    body
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
