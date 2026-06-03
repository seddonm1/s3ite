//! Real-binary storage transaction tests.
//!
//! These tests drive public S3 requests and then inspect the bucket SQLite
//! database directly to verify durable state after each completed request.

mod support;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{
        BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    },
};
use rusqlite::{Connection, OpenFlags, params};
use serial_test::serial;
use support::{
    assertions::assert_error_contains,
    fixtures::{bucket_name, create_bucket, put_bucket_versioning, put_object},
    sdk,
    server::TestServer,
};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectRow {
    sequence: i64,
    version_id: String,
    is_latest: bool,
    is_delete_marker: bool,
    size: i64,
    md5: Option<String>,
    checksum_algorithm: Option<i64>,
    checksum_type: Option<i64>,
    checksum_value: Option<String>,
    body: Option<Vec<u8>>,
}

#[derive(Debug, Eq, PartialEq)]
struct MultipartPartRow {
    part_number: i32,
    size: i64,
    md5: Option<String>,
    checksum_algorithm: Option<i64>,
    checksum_value: Option<String>,
    body: Vec<u8>,
}

#[tokio::test]
#[serial(conformance)]
async fn bucket_versioning_write_persists_bucket_state() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    let connection = open_bucket_db(&server, &bucket)?;
    assert_eq!(bucket_versioning_state(&connection)?, 0);

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Enabled).await?;
    assert_eq!(bucket_versioning_state(&connection)?, 1);

    put_bucket_versioning(&client, &bucket, BucketVersioningStatus::Suspended).await?;
    assert_eq!(bucket_versioning_state(&connection)?, 2);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn delete_bucket_removes_empty_bucket_database_files() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    let path = bucket_path(&server, &bucket);
    assert!(path.exists());

    client.delete_bucket().bucket(&bucket).send().await?;
    assert!(!path.exists());
    assert!(!PathBuf::from(format!("{}-wal", path.to_string_lossy())).exists());
    assert!(!PathBuf::from(format!("{}-shm", path.to_string_lossy())).exists());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn put_object_persists_metadata_and_body_together() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"foo").await?;

    let connection = open_bucket_db(&server, &bucket)?;
    let row = single_object_row(&connection, "foo")?;
    assert_eq!(row.version_id, "null");
    assert!(row.is_latest);
    assert!(!row.is_delete_marker);
    assert_eq!(row.size, 3);
    assert_eq!(row.body.as_deref(), Some(b"foo".as_slice()));
    assert!(row.md5.as_ref().is_some_and(|md5| !md5.is_empty()));
    assert!(row.checksum_algorithm.is_some());
    assert!(row.checksum_type.is_some());
    assert!(row.checksum_value.is_some());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn failed_conditional_put_leaves_existing_object_unchanged() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"old").await?;
    let connection = open_bucket_db(&server, &bucket)?;
    let before = single_object_row(&connection, "foo")?;

    let error = match client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .if_match("badetag")
        .body(ByteStream::from_static(b"new"))
        .send()
        .await
    {
        Ok(_) => bail!("stale conditional put should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");

    let after = single_object_row(&connection, "foo")?;
    assert_eq!(after, before);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn failed_conditional_delete_leaves_object_then_success_removes_it() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "foo", b"body").await?;
    let connection = open_bucket_db(&server, &bucket)?;
    let before = single_object_row(&connection, "foo")?;

    let error = match client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .if_match("badetag")
        .send()
        .await
    {
        Ok(_) => bail!("stale conditional delete should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "PreconditionFailed");
    assert_eq!(single_object_row(&connection, "foo")?, before);

    client
        .delete_object()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await?;
    assert_eq!(object_version_count(&connection, "foo")?, 0);
    assert_eq!(object_body_count(&connection, "foo")?, 0);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn delete_objects_persists_successes_and_failed_conditions_independently() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "keep", b"keep").await?;
    put_object(&client, &bucket, "delete", b"delete").await?;
    let connection = open_bucket_db(&server, &bucket)?;
    let keep_before = single_object_row(&connection, "keep")?;
    let delete_etag = client
        .head_object()
        .bucket(&bucket)
        .key("delete")
        .send()
        .await?
        .e_tag()
        .context("HeadObject response missing ETag")?
        .to_string();

    let delete = Delete::builder()
        .objects(
            ObjectIdentifier::builder()
                .key("keep")
                .e_tag("\"badetag\"")
                .build()?,
        )
        .objects(
            ObjectIdentifier::builder()
                .key("delete")
                .e_tag(delete_etag)
                .build()?,
        )
        .build()?;

    let output = client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await?;
    assert_eq!(output.deleted().len(), 1);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(single_object_row(&connection, "keep")?, keep_before);
    assert_eq!(object_version_count(&connection, "delete")?, 0);
    assert_eq!(object_body_count(&connection, "delete")?, 0);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn copy_object_creates_destination_without_mutating_source() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");

    create_bucket(&client, &bucket).await?;
    put_object(&client, &bucket, "source", b"source-body").await?;
    let connection = open_bucket_db(&server, &bucket)?;
    let source_before = single_object_row(&connection, "source")?;

    client
        .copy_object()
        .bucket(&bucket)
        .key("dest")
        .copy_source(format!("{bucket}/source"))
        .send()
        .await?;

    assert_eq!(single_object_row(&connection, "source")?, source_before);
    let dest = single_object_row(&connection, "dest")?;
    assert_eq!(dest.body.as_deref(), Some(b"source-body".as_slice()));
    assert!(dest.checksum_value.is_some());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn cross_bucket_copy_creates_destination_without_mutating_source_bucket() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-storage-src");
    let target_bucket = bucket_name("s3ite-storage-dst");

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_object(&client, &source_bucket, "source", b"source-body").await?;
    let source_connection = open_bucket_db(&server, &source_bucket)?;
    let target_connection = open_bucket_db(&server, &target_bucket)?;
    let source_before = single_object_row(&source_connection, "source")?;

    client
        .copy_object()
        .bucket(&target_bucket)
        .key("dest")
        .copy_source(format!("{source_bucket}/source"))
        .send()
        .await?;

    assert_eq!(
        single_object_row(&source_connection, "source")?,
        source_before
    );
    let dest = single_object_row(&target_connection, "dest")?;
    assert_eq!(dest.body.as_deref(), Some(b"source-body".as_slice()));
    assert!(dest.checksum_value.is_some());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn upload_part_copy_persists_same_and_cross_bucket_part_rows() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let source_bucket = bucket_name("s3ite-storage-src");
    let target_bucket = bucket_name("s3ite-storage-dst");

    create_bucket(&client, &source_bucket).await?;
    create_bucket(&client, &target_bucket).await?;
    put_object(&client, &source_bucket, "source", b"copied-part").await?;
    let source_connection = open_bucket_db(&server, &source_bucket)?;
    let target_connection = open_bucket_db(&server, &target_bucket)?;

    let same_upload_id = create_upload(&client, &source_bucket, "same-dest").await?;
    let same_upload_uuid = Uuid::parse_str(&same_upload_id)?;
    upload_part_copy(
        &client,
        &source_bucket,
        "same-dest",
        &same_upload_id,
        1,
        &format!("{source_bucket}/source"),
    )
    .await?;
    assert_eq!(
        multipart_part_row(&source_connection, same_upload_uuid, 1)?.body,
        b"copied-part"
    );

    let cross_upload_id = create_upload(&client, &target_bucket, "cross-dest").await?;
    let cross_upload_uuid = Uuid::parse_str(&cross_upload_id)?;
    upload_part_copy(
        &client,
        &target_bucket,
        "cross-dest",
        &cross_upload_id,
        1,
        &format!("{source_bucket}/source"),
    )
    .await?;
    let cross_part = multipart_part_row(&target_connection, cross_upload_uuid, 1)?;
    assert_eq!(cross_part.body, b"copied-part");
    assert!(cross_part.md5.is_some());

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn abort_multipart_upload_removes_upload_and_part_rows_without_object() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");
    let key = "abort-me";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let upload_uuid = Uuid::parse_str(&upload_id)?;
    upload_part(&client, &bucket, key, &upload_id, 1, b"part-body").await?;
    let connection = open_bucket_db(&server, &bucket)?;
    assert_eq!(active_upload_count(&connection, upload_uuid)?, 1);
    assert_eq!(
        multipart_part_row(&connection, upload_uuid, 1)?.body,
        b"part-body"
    );

    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    assert_eq!(active_upload_count(&connection, upload_uuid)?, 0);
    assert_eq!(active_part_count(&connection, upload_uuid)?, 0);
    assert_eq!(object_version_count(&connection, key)?, 0);
    assert_eq!(completed_upload_count(&connection, upload_uuid)?, 0);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn multipart_completion_moves_upload_state_into_object_state() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");
    let key = "multipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let upload_uuid = Uuid::parse_str(&upload_id)?;
    let part = upload_part(&client, &bucket, key, &upload_id, 1, b"part-body").await?;
    let connection = open_bucket_db(&server, &bucket)?;

    assert_eq!(active_upload_count(&connection, upload_uuid)?, 1);
    assert_eq!(active_part_count(&connection, upload_uuid)?, 1);
    assert_eq!(object_version_count(&connection, key)?, 0);
    assert_eq!(completed_upload_count(&connection, upload_uuid)?, 0);

    complete_upload(&client, &bucket, key, &upload_id, vec![part.clone()]).await?;
    assert_eq!(active_upload_count(&connection, upload_uuid)?, 0);
    assert_eq!(active_part_count(&connection, upload_uuid)?, 0);
    assert_eq!(completed_upload_count(&connection, upload_uuid)?, 1);
    assert_eq!(completed_object_part_count(&connection, key)?, 1);
    assert_eq!(
        single_object_row(&connection, key)?.body.as_deref(),
        Some(b"part-body".as_slice())
    );

    complete_upload(&client, &bucket, key, &upload_id, vec![part]).await?;
    assert_eq!(object_version_count(&connection, key)?, 1);
    assert_eq!(completed_upload_count(&connection, upload_uuid)?, 1);

    Ok(())
}

#[tokio::test]
#[serial(conformance)]
async fn failed_multipart_completion_keeps_upload_parts_without_object() -> Result<()> {
    let server = TestServer::spawn().await?;
    let client = sdk::client(&server);
    let bucket = bucket_name("s3ite-storage");
    let key = "multipart";

    create_bucket(&client, &bucket).await?;
    let upload_id = create_upload(&client, &bucket, key).await?;
    let upload_uuid = Uuid::parse_str(&upload_id)?;
    upload_part(&client, &bucket, key, &upload_id, 1, b"part-body").await?;
    let connection = open_bucket_db(&server, &bucket)?;

    let bad_part = CompletedPart::builder()
        .part_number(1)
        .e_tag("badetag")
        .build();
    let error = match complete_upload(&client, &bucket, key, &upload_id, vec![bad_part]).await {
        Ok(_) => bail!("completion with a stale part ETag should fail"),
        Err(error) => error,
    };
    assert_error_contains(&error, "InvalidPart");

    assert_eq!(active_upload_count(&connection, upload_uuid)?, 1);
    assert_eq!(active_part_count(&connection, upload_uuid)?, 1);
    assert_eq!(object_version_count(&connection, key)?, 0);
    assert_eq!(completed_upload_count(&connection, upload_uuid)?, 0);

    Ok(())
}

fn open_bucket_db(server: &TestServer, bucket: &str) -> Result<Connection> {
    let path = bucket_path(server, bucket);
    Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open bucket database {}", path.display()))
}

fn bucket_path(server: &TestServer, bucket: &str) -> PathBuf {
    server.data_root().join(format!("{bucket}.sqlite3"))
}

fn bucket_versioning_state(connection: &Connection) -> Result<i64> {
    Ok(connection.query_row(
        "SELECT versioning_state FROM bucket_state WHERE id = 1;",
        [],
        |row| row.get(0),
    )?)
}

fn object_rows(connection: &Connection, key: &str) -> Result<Vec<ObjectRow>> {
    let mut stmt = connection.prepare(
        "
        SELECT
            ov.sequence,
            ov.version_id,
            ov.is_latest,
            ov.is_delete_marker,
            ov.size,
            ov.md5,
            ov.checksum_algorithm,
            ov.checksum_type,
            ov.checksum_value,
            ob.body
        FROM object_version AS ov
        LEFT JOIN object_body AS ob ON ob.sequence = ov.sequence
        WHERE ov.key = ?1
        ORDER BY ov.sequence;",
    )?;
    Ok(stmt
        .query_map([key], |row| {
            Ok(ObjectRow {
                sequence: row.get(0)?,
                version_id: row.get(1)?,
                is_latest: row.get::<_, i64>(2)? != 0,
                is_delete_marker: row.get::<_, i64>(3)? != 0,
                size: row.get(4)?,
                md5: row.get(5)?,
                checksum_algorithm: row.get(6)?,
                checksum_type: row.get(7)?,
                checksum_value: row.get(8)?,
                body: row.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn single_object_row(connection: &Connection, key: &str) -> Result<ObjectRow> {
    let mut rows = object_rows(connection, key)?;
    if rows.len() != 1 {
        bail!("expected one object row for {key}, got {}", rows.len());
    }
    Ok(rows.remove(0))
}

fn object_version_count(connection: &Connection, key: &str) -> Result<i64> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM object_version WHERE key = ?1;",
        [key],
        |row| row.get(0),
    )?)
}

fn object_body_count(connection: &Connection, key: &str) -> Result<i64> {
    Ok(connection.query_row(
        "
        SELECT COUNT(*)
        FROM object_body AS ob
        JOIN object_version AS ov ON ov.sequence = ob.sequence
        WHERE ov.key = ?1;",
        [key],
        |row| row.get(0),
    )?)
}

fn active_upload_count(connection: &Connection, upload_id: Uuid) -> Result<i64> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM multipart_upload WHERE upload_id = ?1;",
        params![upload_id],
        |row| row.get(0),
    )?)
}

fn active_part_count(connection: &Connection, upload_id: Uuid) -> Result<i64> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM multipart_upload_part WHERE upload_id = ?1;",
        params![upload_id],
        |row| row.get(0),
    )?)
}

fn completed_upload_count(connection: &Connection, upload_id: Uuid) -> Result<i64> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM completed_multipart_upload WHERE upload_id = ?1;",
        params![upload_id],
        |row| row.get(0),
    )?)
}

fn completed_object_part_count(connection: &Connection, key: &str) -> Result<i64> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM completed_object_part WHERE key = ?1;",
        [key],
        |row| row.get(0),
    )?)
}

fn multipart_part_row(
    connection: &Connection,
    upload_id: Uuid,
    part_number: i32,
) -> Result<MultipartPartRow> {
    Ok(connection.query_row(
        "
        SELECT
            part_number,
            size,
            md5,
            checksum_algorithm,
            checksum_value,
            value
        FROM multipart_upload_part
        WHERE upload_id = ?1 AND part_number = ?2;",
        params![upload_id, part_number],
        |row| {
            Ok(MultipartPartRow {
                part_number: row.get(0)?,
                size: row.get(1)?,
                md5: row.get(2)?,
                checksum_algorithm: row.get(3)?,
                checksum_value: row.get(4)?,
                body: row.get(5)?,
            })
        },
    )?)
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

async fn upload_part_copy(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    copy_source: &str,
) -> Result<CompletedPart> {
    let response = client
        .upload_part_copy()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .copy_source(copy_source)
        .send()
        .await?;
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
