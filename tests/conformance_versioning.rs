//! Real-binary HTTP versioning conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_versioning`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{primitives::ByteStream, types::BucketVersioningStatus};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_bucket_versioning, put_object},
    sdk,
    server::TestServer,
};

#[tokio::test]
#[serial(conformance)]
async fn test_versioning_bucket_create_suspend() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_bucket_create_suspend
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");

    create_bucket(&client, &bucket).await?;
    let default_state = client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(default_state.status(), None);

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    let suspended = client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(suspended.status(), Some(&BucketVersioningStatus::Suspended));

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let enabled = client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(enabled.status(), Some(&BucketVersioningStatus::Enabled));

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    let suspended_again = client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await?;
    assert_eq!(
        suspended_again.status(),
        Some(&BucketVersioningStatus::Suspended)
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_create_read_remove() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_create_read_remove
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "versioned.txt";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;

    let first = put_versioned_object(&client, &bucket, key, b"first").await?;
    let second = put_versioned_object(&client, &bucket, key, b"second").await?;
    assert_ne!(first, second);
    assert_ceph_shaped_version_id(&first);
    assert_ceph_shaped_version_id(&second);

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"second");
    assert_eq!(
        get_object_version_bytes(&client, &bucket, key, &first).await?,
        b"first"
    );

    let delete_marker = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(delete_marker.delete_marker(), Some(true));
    let delete_marker_version = delete_marker
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("delete marker response missing version ID"))?
        .to_string();
    assert_ceph_shaped_version_id(&delete_marker_version);

    let error = match client.get_object().bucket(&bucket).key(key).send().await {
        Ok(_) => bail!("current delete marker should hide object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    assert_eq!(
        get_object_version_bytes(&client, &bucket, key, &second).await?,
        b"second"
    );

    let versions = client.list_object_versions().bucket(&bucket).send().await?;
    assert_eq!(versions.delete_markers().len(), 1);
    assert_eq!(versions.versions().len(), 2);
    assert_eq!(
        versions
            .delete_markers()
            .first()
            .and_then(|marker| marker.version_id()),
        Some(delete_marker_version.as_str())
    );
    assert!(
        versions
            .delete_markers()
            .first()
            .and_then(|marker| marker.is_latest())
            .unwrap_or(false)
    );

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&delete_marker_version)
        .send()
        .await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"second");

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_plain_null_version_overwrite_suspended() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_plain_null_version_overwrite_suspended
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "suspended.txt";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let generated = put_versioned_object(&client, &bucket, key, b"generated").await?;

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    let null_put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"null-one"))
        .send()
        .await?;
    assert_eq!(null_put.version_id(), None);
    put_object(&client, &bucket, key, b"null-two").await?;
    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"null-two");

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert_eq!(
        listed
            .versions()
            .iter()
            .filter(|version| version.version_id() == Some("null"))
            .count(),
        1
    );
    assert!(listed.versions().iter().any(|version| {
        version.version_id() == Some(generated.as_str()) && !version.is_latest().unwrap_or(true)
    }));

    let delete_marker = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(delete_marker.delete_marker(), Some(true));
    assert_eq!(delete_marker.version_id(), Some("null"));

    let error = match client.get_object().bucket(&bucket).key(key).send().await {
        Ok(_) => bail!("suspended null delete marker should hide object"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    assert_eq!(
        get_object_version_bytes(&client, &bucket, key, &generated).await?,
        b"generated"
    );

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_list_object_versions_for_unversioned_bucket() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_plain_null_version_overwrite
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");

    create_bucket(&client, &bucket).await?;
    for key in ["version/a", "version/b", "version/c", "version/dir/child"] {
        put_object(&client, &bucket, key, b"x").await?;
    }

    let first_page = client
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
        first_page.versions().iter().all(
            |version| version.version_id() == Some("null") && version.is_latest() == Some(true)
        )
    );

    let second_page =
        client
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

    let grouped = client
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

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn test_list_object_versions_deleted_marker_continues_listing() -> Result<()> {
    // Mirrors the s3-tests cleanup pattern that deletes a page before using its version marker.
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    for _ in 0..5 {
        put_versioned_object(&client, &bucket, "version/a", b"x").await?;
    }
    put_versioned_object(&client, &bucket, "version/b", b"x").await?;

    let first_page = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("version/")
        .max_keys(2)
        .send()
        .await?;
    let next_key_marker = first_page
        .next_key_marker()
        .ok_or_else(|| anyhow::anyhow!("first version page missing next key marker"))?
        .to_string();
    let next_version_id_marker = first_page
        .next_version_id_marker()
        .ok_or_else(|| anyhow::anyhow!("first version page missing next version ID marker"))?
        .to_string();
    let first_page_versions = first_page
        .versions()
        .iter()
        .map(|version| {
            let key = version
                .key()
                .ok_or_else(|| anyhow::anyhow!("listed version missing key"))?
                .to_string();
            let version_id = version
                .version_id()
                .ok_or_else(|| anyhow::anyhow!("listed version missing version ID"))?
                .to_string();
            Ok((key, version_id))
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(first_page_versions.len(), 2);

    for (key, version_id) in &first_page_versions {
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;
    }

    let second_page = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("version/")
        .key_marker(next_key_marker)
        .version_id_marker(next_version_id_marker)
        .send()
        .await?;
    assert!(second_page.versions().iter().any(|version| {
        version.key() == Some("version/a") || version.key() == Some("version/b")
    }));

    Ok(())
}

async fn put_versioned_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> Result<String> {
    Ok(client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("enabled PutObject missing version ID"))?
        .to_string())
}

async fn get_object_version_bytes(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Vec<u8>> {
    Ok(client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes()
        .to_vec())
}

fn assert_ceph_shaped_version_id(version_id: &str) {
    assert_eq!(version_id.len(), 31);
    assert!(
        version_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.')),
        "unexpected version ID shape: {version_id}"
    );
}
