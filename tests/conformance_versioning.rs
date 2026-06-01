//! Real-binary HTTP versioning conformance tests.
//!
//! These tests require the `s3ite` binary. Run them with
//! `cargo test --features binary --test conformance_versioning`, or set
//! `S3ITE_TEST_BIN` to an existing binary path.

mod support;

use anyhow::{Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{
        BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    },
};
use futures::future::try_join_all;
use reqwest::{Method, StatusCode};
use serial_test::serial;
use support::{
    assertions::{assert_error_contains, assert_status},
    fixtures::{bucket_name, create_bucket, get_object_bytes, put_bucket_versioning, put_object},
    raw_http, sdk,
    server::TestServer,
};

const MIB: usize = 1024 * 1024;

/// Verifies bucket versioning state transitions between none, suspended, and enabled.
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

/// Verifies enabled versioning creates readable versions and delete markers.
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

/// Verifies removing the latest object version reveals the prior version.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_create_read_remove_head() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_create_read_remove_head
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let versions = create_multiple_versions(&client, &bucket, key, 5).await?;

    let removed = versions
        .last()
        .ok_or_else(|| anyhow::anyhow!("missing latest version"))?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id(&removed.version_id)
        .send()
        .await?;

    assert_eq!(get_object_bytes(&client, &bucket, key).await?, b"content-3");

    let delete_marker = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(delete_marker.delete_marker(), Some(true));
    let delete_marker_version = delete_marker
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("delete marker response missing version ID"))?;

    let response = client.list_object_versions().bucket(&bucket).send().await?;
    assert_eq!(response.versions().len(), 4);
    assert_eq!(response.delete_markers().len(), 1);
    assert_eq!(
        response
            .delete_markers()
            .first()
            .and_then(|marker| marker.version_id()),
        Some(delete_marker_version)
    );

    Ok(())
}

/// Verifies repeated deletes stack multiple delete markers.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_stack_delete_merkers() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_stack_delete_merkers
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "test1/a";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    put_versioned_object(&client, &bucket, key, b"content").await?;
    for _ in 0..3 {
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await?;
    }

    let response = client.list_object_versions().bucket(&bucket).send().await?;
    assert_eq!(response.versions().len(), 1);
    assert_eq!(response.delete_markers().len(), 3);

    Ok(())
}

/// Verifies deleting the null version removes an unversioned object.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_plain_null_version_removal() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_plain_null_version_removal
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobjfoo";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, key, b"fooz").await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .version_id("null")
        .send()
        .await?;

    let error = match client.get_object().bucket(&bucket).key(key).send().await {
        Ok(_) => bail!("deleted null version should not be readable"),
        Err(error) => error,
    };
    assert_error_contains(&error, "NoSuchKey");
    assert!(
        client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await?
            .versions()
            .is_empty()
    );

    Ok(())
}

/// Verifies suspended versioning overwrites the null version and delete marker.
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

/// Verifies suspended versioning can delete and overwrite the null version repeatedly.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_suspend_versions() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_suspend_versions
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let mut versions = create_multiple_versions(&client, &bucket, key, 5).await?;

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    put_object(&client, &bucket, key, b"null content 1").await?;
    put_object(&client, &bucket, key, b"null content 2").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    put_object(&client, &bucket, key, b"null content 3").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    versions.extend(create_multiple_versions(&client, &bucket, key, 3).await?);

    for version in versions {
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(version.version_id)
            .send()
            .await?;
    }
    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert!(listed.versions().is_empty());

    Ok(())
}

/// Verifies copying a suspended null version preserves the visible body.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_suspended_copy() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_suspended_copy
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let other_bucket = bucket_name("s3ite-ver-copy");
    let source = "testobj1";
    let same_bucket_target = "testobj2";

    create_bucket(&client, &bucket).await?;
    create_bucket(&client, &other_bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    put_versioned_object(&client, &bucket, source, b"generated").await?;

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    put_object(&client, &bucket, source, b"null content").await?;

    client
        .copy_object()
        .bucket(&bucket)
        .key(same_bucket_target)
        .copy_source(format!("{bucket}/{source}"))
        .send()
        .await?;
    client
        .copy_object()
        .bucket(&other_bucket)
        .key(source)
        .copy_source(format!("{bucket}/{source}"))
        .send()
        .await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(source)
        .send()
        .await?;

    assert_eq!(
        get_object_bytes(&client, &bucket, same_bucket_target).await?,
        b"null content"
    );
    assert_eq!(
        get_object_bytes(&client, &other_bucket, source).await?,
        b"null content"
    );

    Ok(())
}

/// Verifies unversioned buckets list null object versions with pagination.
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

/// Verifies all object versions can be removed by exact version ID.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_create_versions_remove_all() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_create_versions_remove_all
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let versions = create_multiple_versions(&client, &bucket, key, 10).await?;

    for version in versions {
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(version.version_id)
            .send()
            .await?;
    }

    assert!(
        client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await?
            .versions()
            .is_empty()
    );

    Ok(())
}

/// Verifies multipart overwrites create independently readable object versions.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_create_overwrite_multipart() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_create_overwrite_multipart
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;

    let mut versions = Vec::new();
    for byte in [b'a', b'b', b'c'] {
        let body = vec![byte; 6 * MIB];
        let completed = multipart_upload(&client, &bucket, key, body.clone()).await?;
        let version_id = completed
            .version_id()
            .ok_or_else(|| anyhow::anyhow!("multipart completion missing version ID"))?
            .to_string();
        versions.push(ObjectVersion {
            version_id,
            content: body,
        });
    }

    for version in &versions {
        assert_eq!(
            get_object_version_bytes(&client, &bucket, key, &version.version_id).await?,
            version.content
        );
    }
    assert_eq!(
        get_object_bytes(&client, &bucket, key).await?,
        versions
            .last()
            .ok_or_else(|| anyhow::anyhow!("missing latest multipart version"))?
            .content
    );

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    let listed_version_ids = listed
        .versions()
        .iter()
        .filter_map(|version| version.version_id())
        .collect::<Vec<_>>();
    assert_eq!(
        listed_version_ids,
        versions
            .iter()
            .rev()
            .map(|version| version.version_id.as_str())
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Verifies exact-version removal works for special key names.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_create_versions_remove_special_names() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_create_versions_remove_special_names
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;

    for key in ["_testobj", "_", ":", " "] {
        let mut version_ids = Vec::new();
        for index in 0..10 {
            version_ids.push(
                put_versioned_object_raw(&server, &bucket, key, format!("content-{index}")).await?,
            );
        }
        for version_id in version_ids {
            delete_object_version_raw(&server, &bucket, key, &version_id).await?;
        }
    }

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert!(listed.versions().is_empty());
    assert!(listed.delete_markers().is_empty());

    Ok(())
}

/// Verifies exact-version copy can copy old versions in and across buckets.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_copy_obj_version() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_copy_obj_version
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let other_bucket = bucket_name("s3ite-ver-copy");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    create_bucket(&client, &other_bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let versions = create_multiple_versions(&client, &bucket, key, 3).await?;

    for (index, version) in versions.iter().enumerate() {
        let new_key = format!("key_{index}");
        client
            .copy_object()
            .bucket(&bucket)
            .key(&new_key)
            .copy_source(format!("{bucket}/{key}?versionId={}", version.version_id))
            .send()
            .await?;
        assert_eq!(
            get_object_bytes(&client, &bucket, &new_key).await?,
            version.content
        );
    }

    for (index, version) in versions.iter().enumerate() {
        let new_key = format!("key_{index}");
        client
            .copy_object()
            .bucket(&other_bucket)
            .key(&new_key)
            .copy_source(format!("{bucket}/{key}?versionId={}", version.version_id))
            .send()
            .await?;
        assert_eq!(
            get_object_bytes(&client, &other_bucket, &new_key).await?,
            version.content
        );
    }

    client
        .copy_object()
        .bucket(&other_bucket)
        .key("new_key")
        .copy_source(format!("{bucket}/{key}"))
        .send()
        .await?;
    assert_eq!(
        get_object_bytes(&client, &other_bucket, "new_key").await?,
        versions
            .last()
            .ok_or_else(|| anyhow::anyhow!("missing latest source version"))?
            .content
    );

    Ok(())
}

/// Verifies ListObjectVersions orders versions by key and newest version first.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_obj_list_marker() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_obj_list_marker
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";
    let key2 = "testobj-1";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let versions = create_multiple_versions(&client, &bucket, key, 5).await?;
    let versions2 = create_multiple_versions(&client, &bucket, key2, 5).await?;

    let response = client.list_object_versions().bucket(&bucket).send().await?;
    let listed = response
        .versions()
        .iter()
        .map(|version| {
            (
                version.key().unwrap_or_default().to_string(),
                version.version_id().unwrap_or_default().to_string(),
            )
        })
        .collect::<Vec<_>>();
    let expected = versions
        .iter()
        .rev()
        .map(|version| (key.to_string(), version.version_id.clone()))
        .chain(
            versions2
                .iter()
                .rev()
                .map(|version| (key2.to_string(), version.version_id.clone())),
        )
        .collect::<Vec<_>>();
    assert_eq!(listed, expected);

    Ok(())
}

/// Verifies PutObject returns VersionId only when bucket state creates generated versions.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_bucket_atomic_upload_return_version_id() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_bucket_atomic_upload_return_version_id
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let enabled_bucket = bucket_name("s3ite-ver");
    let unversioned_bucket = bucket_name("s3ite-ver");
    let suspended_bucket = bucket_name("s3ite-ver");

    create_bucket(&client, &enabled_bucket).await?;
    put_bucket_versioning(&client, &enabled_bucket, BucketVersioningStatus::Enabled).await?;
    let enabled = client
        .put_object()
        .bucket(&enabled_bucket)
        .key("bar")
        .body(ByteStream::from_static(b""))
        .send()
        .await?;
    let enabled_version_id = enabled
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("enabled PutObject missing version ID"))?;
    let listed = client
        .list_object_versions()
        .bucket(&enabled_bucket)
        .send()
        .await?;
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(enabled_version_id))
    );

    create_bucket(&client, &unversioned_bucket).await?;
    let unversioned = client
        .put_object()
        .bucket(&unversioned_bucket)
        .key("baz")
        .body(ByteStream::from_static(b""))
        .send()
        .await?;
    assert_eq!(unversioned.version_id(), None);

    create_bucket(&client, &suspended_bucket).await?;
    put_bucket_versioning(
        &client,
        &suspended_bucket,
        BucketVersioningStatus::Suspended,
    )
    .await?;
    let suspended = client
        .put_object()
        .bucket(&suspended_bucket)
        .key("baz")
        .body(ByteStream::from_static(b""))
        .send()
        .await?;
    assert_eq!(suspended.version_id(), None);

    Ok(())
}

/// Verifies multipart completion returns VersionId based on bucket versioning state.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_bucket_multipart_upload_return_version_id() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_bucket_multipart_upload_return_version_id
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let enabled_bucket = bucket_name("s3ite-ver");
    let unversioned_bucket = bucket_name("s3ite-ver");
    let suspended_bucket = bucket_name("s3ite-ver");

    create_bucket(&client, &enabled_bucket).await?;
    put_bucket_versioning(&client, &enabled_bucket, BucketVersioningStatus::Enabled).await?;
    let enabled = multipart_upload(&client, &enabled_bucket, "bar", vec![b'a'; 6 * MIB]).await?;
    let enabled_version_id = enabled
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("enabled multipart completion missing version ID"))?;
    let listed = client
        .list_object_versions()
        .bucket(&enabled_bucket)
        .send()
        .await?;
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(enabled_version_id))
    );

    create_bucket(&client, &unversioned_bucket).await?;
    let unversioned =
        multipart_upload(&client, &unversioned_bucket, "baz", vec![b'b'; 6 * MIB]).await?;
    assert_eq!(unversioned.version_id(), None);

    create_bucket(&client, &suspended_bucket).await?;
    put_bucket_versioning(
        &client,
        &suspended_bucket,
        BucketVersioningStatus::Suspended,
    )
    .await?;
    let suspended =
        multipart_upload(&client, &suspended_bucket, "foo", vec![b'c'; 6 * MIB]).await?;
    assert_eq!(suspended.version_id(), None);

    Ok(())
}

/// Verifies DeleteObjects can remove exact object versions idempotently.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_multi_object_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_multi_object_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "key";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let versions = create_multiple_versions(&client, &bucket, key, 2).await?;

    let delete = delete_for_versions(key, versions.iter().map(|version| &version.version_id))?;
    client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete.clone())
        .send()
        .await?;
    assert!(
        client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await?
            .versions()
            .is_empty()
    );

    client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    assert!(
        client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await?
            .versions()
            .is_empty()
    );

    Ok(())
}

/// Verifies DeleteObjects can remove versions and delete markers idempotently.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_multi_object_delete_with_marker() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_multi_object_delete_with_marker
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "key";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let versions = create_multiple_versions(&client, &bucket, key, 2).await?;
    let delete_marker = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    let marker_version_id = delete_marker
        .version_id()
        .ok_or_else(|| anyhow::anyhow!("delete marker response missing version ID"))?
        .to_string();

    let delete = delete_for_versions(
        key,
        versions
            .iter()
            .map(|version| &version.version_id)
            .chain(std::iter::once(&marker_version_id)),
    )?;
    client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete.clone())
        .send()
        .await?;

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert!(listed.versions().is_empty());
    assert!(listed.delete_markers().is_empty());

    client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert!(listed.versions().is_empty());
    assert!(listed.delete_markers().is_empty());

    Ok(())
}

/// Verifies DeleteObjects without a version creates a delete marker.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_multi_object_delete_with_marker_create() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_multi_object_delete_with_marker_create
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "key";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key(key).build()?)
        .build()?;
    let response = client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    let deleted = response.deleted();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].delete_marker(), Some(true));
    let marker_version_id = deleted[0].delete_marker_version_id().ok_or_else(|| {
        anyhow::anyhow!("DeleteObjects response missing delete marker version ID")
    })?;

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    let markers = listed.delete_markers();
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].version_id(), Some(marker_version_id));
    assert_eq!(markers[0].key(), Some(key));

    Ok(())
}

/// Verifies concurrent multi-delete requests can remove exact versions idempotently.
#[tokio::test]
#[serial(conformance)]
async fn test_versioning_concurrent_multi_object_delete() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioning_concurrent_multi_object_delete
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let keys = ["key-0", "key-1", "key-2", "key-3", "key-4"];

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    let mut objects = Vec::new();
    for key in keys {
        for version in create_multiple_versions(&client, &bucket, key, 3).await? {
            objects.push(
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(version.version_id)
                    .build()?,
            );
        }
    }
    let expected_count = objects.len();
    let delete = Delete::builder().set_objects(Some(objects)).build()?;

    let deletes = (0..5).map(|_| {
        let client = client.clone();
        let bucket = bucket.clone();
        let delete = delete.clone();
        async move {
            let response = client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await?;
            assert_eq!(response.deleted().len(), expected_count);
            assert!(response.errors().is_empty());
            Ok::<_, anyhow::Error>(())
        }
    });
    try_join_all(deletes).await?;

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert!(listed.versions().is_empty());
    assert!(listed.delete_markers().is_empty());

    Ok(())
}

/// Verifies concurrent version creation and exact-version removal leave no residue.
#[tokio::test]
#[serial(conformance)]
async fn test_versioned_concurrent_object_create_concurrent_remove() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioned_concurrent_object_create_concurrent_remove
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    for _ in 0..5 {
        let versions = put_concurrent_versions(&client, &bucket, key, 5).await?;
        assert_eq!(
            client
                .list_object_versions()
                .bucket(&bucket)
                .send()
                .await?
                .versions()
                .len(),
            versions.len()
        );
        delete_concurrent_versions(&client, &bucket, key, versions).await?;
        let listed = client.list_object_versions().bucket(&bucket).send().await?;
        assert!(listed.versions().is_empty());
        assert!(listed.delete_markers().is_empty());
    }

    Ok(())
}

/// Verifies create and remove races on a versioned key can be drained by exact deletes.
#[tokio::test]
#[serial(conformance)]
async fn test_versioned_concurrent_object_create_and_remove() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_versioned_concurrent_object_create_and_remove
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "testobj";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    for _ in 0..3 {
        let created = put_concurrent_versions(&client, &bucket, key, 3);
        let removed = clear_current_versions(&client, &bucket, key);
        let (created, _) = tokio::try_join!(created, removed)?;
        delete_concurrent_versions(&client, &bucket, key, created).await?;
    }
    clear_current_versions(&client, &bucket, key).await?;

    let listed = client.list_object_versions().bucket(&bucket).send().await?;
    assert!(listed.versions().is_empty());
    assert!(listed.delete_markers().is_empty());

    Ok(())
}

/// Verifies nonversioned delete misses expose the delete-marker header as false.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_marker_nonversioned() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_marker_nonversioned
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "frodo.txt";

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, key, b"body").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_delete_marker_header(&server, &bucket, key, "false").await
}

/// Verifies versioned delete markers expose the delete-marker header as true.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_marker_versioned() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_marker_versioned
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "bilbo.txt";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    put_object(&client, &bucket, key, b"body").await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_delete_marker_header(&server, &bucket, key, "true").await
}

/// Verifies suspended delete markers expose the delete-marker header as true.
#[tokio::test]
#[serial(conformance)]
async fn test_delete_marker_suspended() -> Result<()> {
    // Mirrors s3tests/functional/test_s3.py::test_delete_marker_suspended
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-ver");
    let key = "ringo.txt";

    create_bucket(&client, &bucket).await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    put_object(&client, &bucket, key, b"body").await?;
    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;
    assert_delete_marker_header(&server, &bucket, key, "true").await
}

/// Verifies deleted ListObjectVersions markers can still advance pagination.
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

#[derive(Debug)]
struct ObjectVersion {
    version_id: String,
    content: Vec<u8>,
}

async fn create_multiple_versions(
    client: &Client,
    bucket: &str,
    key: &str,
    count: usize,
) -> Result<Vec<ObjectVersion>> {
    let mut versions = Vec::with_capacity(count);
    for index in 0..count {
        let content = format!("content-{index}").into_bytes();
        let version_id = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(content.clone()))
            .send()
            .await?
            .version_id()
            .ok_or_else(|| anyhow::anyhow!("enabled PutObject missing version ID"))?
            .to_string();
        versions.push(ObjectVersion {
            version_id,
            content,
        });
    }
    Ok(versions)
}

fn delete_for_versions<'a>(
    key: &str,
    version_ids: impl IntoIterator<Item = &'a String>,
) -> Result<Delete> {
    let objects = version_ids
        .into_iter()
        .map(|version_id| {
            Ok(ObjectIdentifier::builder()
                .key(key)
                .version_id(version_id)
                .build()?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Delete::builder().set_objects(Some(objects)).build()?)
}

async fn assert_delete_marker_header(
    server: &TestServer,
    bucket: &str,
    key: &str,
    expected: &str,
) -> Result<()> {
    let response = raw_http::signed_request(
        server,
        Method::HEAD,
        &format!("/{bucket}/{}", raw_http::path_segment(key)),
        Vec::new(),
    )
    .await?;
    assert_status(&response, StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get("x-amz-delete-marker")
            .and_then(|value| value.to_str().ok()),
        Some(expected)
    );
    Ok(())
}

async fn put_versioned_object_raw(
    server: &TestServer,
    bucket: &str,
    key: &str,
    body: String,
) -> Result<String> {
    let response = raw_http::signed_request(
        server,
        Method::PUT,
        &format!("/{bucket}/{}", raw_http::path_segment(key)),
        body.into_bytes(),
    )
    .await?;
    assert_status(&response, StatusCode::OK);
    Ok(response
        .headers()
        .get("x-amz-version-id")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("raw versioned PutObject missing version ID"))?
        .to_string())
}

async fn delete_object_version_raw(
    server: &TestServer,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<()> {
    let response = raw_http::signed_request(
        server,
        Method::DELETE,
        &format!(
            "/{bucket}/{}?versionId={version_id}",
            raw_http::path_segment(key)
        ),
        Vec::new(),
    )
    .await?;
    assert_status(&response, StatusCode::NO_CONTENT);
    Ok(())
}

async fn put_versioned_object(
    client: &Client,
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

async fn multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id()
        .ok_or_else(|| anyhow::anyhow!("create multipart response missing upload ID"))?
        .to_string();
    let response = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body))
        .send()
        .await?;
    let part = CompletedPart::builder()
        .e_tag(
            response
                .e_tag()
                .ok_or_else(|| anyhow::anyhow!("upload part response missing ETag"))?,
        )
        .part_number(1)
        .build();
    let upload = CompletedMultipartUpload::builder()
        .set_parts(Some(vec![part]))
        .build();
    Ok(client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(upload)
        .send()
        .await?)
}

async fn put_concurrent_versions(
    client: &Client,
    bucket: &str,
    key: &str,
    count: usize,
) -> Result<Vec<ObjectVersion>> {
    let writes = (0..count).map(|index| {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        async move {
            let content = format!("content-{index}").into_bytes();
            let version_id = client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(content.clone()))
                .send()
                .await?
                .version_id()
                .ok_or_else(|| anyhow::anyhow!("enabled PutObject missing version ID"))?
                .to_string();
            Ok::<_, anyhow::Error>(ObjectVersion {
                version_id,
                content,
            })
        }
    });
    try_join_all(writes).await
}

async fn delete_concurrent_versions(
    client: &Client,
    bucket: &str,
    key: &str,
    versions: Vec<ObjectVersion>,
) -> Result<()> {
    let deletes = versions.into_iter().map(|version| {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        async move {
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .version_id(version.version_id)
                .send()
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });
    try_join_all(deletes).await?;
    Ok(())
}

async fn clear_current_versions(client: &Client, bucket: &str, key: &str) -> Result<()> {
    let listed = client
        .list_object_versions()
        .bucket(bucket)
        .prefix(key)
        .send()
        .await?;
    let versions = listed
        .versions()
        .iter()
        .filter(|version| version.key() == Some(key))
        .map(|version| {
            Ok(ObjectVersion {
                version_id: version
                    .version_id()
                    .ok_or_else(|| anyhow::anyhow!("listed version missing version ID"))?
                    .to_string(),
                content: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    delete_concurrent_versions(client, bucket, key, versions).await
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
