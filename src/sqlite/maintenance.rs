use super::{
    Arc, BucketConnection, Duration, HashMap, Not, OffsetDateTime, RwLock, Sqlite, SqliteResult,
    Transaction,
};

fn expiration_threshold(ttl_seconds: u64) -> Option<OffsetDateTime> {
    if ttl_seconds == 0 {
        return None;
    }

    let seconds = i64::try_from(ttl_seconds).unwrap_or(i64::MAX);
    Some(OffsetDateTime::now_utc().saturating_sub(Duration::seconds(seconds)))
}

impl Sqlite {
    /// Applies writable-bucket startup maintenance before serving requests.
    pub(super) async fn try_prepare_existing_bucket(
        connection: &BucketConnection,
        config: &crate::Config,
        bucket: &str,
    ) -> SqliteResult<()> {
        if config.read_only(Some(bucket)) {
            return Ok(());
        }

        let bucket = bucket.to_string();
        let config = config.clone();
        let multipart_expire_before = expiration_threshold(config.multipart_upload_ttl_seconds);
        let completed_multipart_expire_before =
            expiration_threshold(config.completed_multipart_upload_ttl_seconds);

        connection
            .write(move |connection| {
                connection.execute_batch(&config.to_sql(Some(&bucket)))?;
                connection.execute_batch(
                    "
                    PRAGMA analysis_limit=1000;
                    PRAGMA optimize;
                    ",
                )?;

                let transaction = connection.transaction()?;
                Self::try_create_tables(&transaction)?;
                Self::try_delete_expired_multipart_uploads(
                    &transaction,
                    multipart_expire_before,
                    completed_multipart_expire_before,
                )?;
                Ok(transaction.commit()?)
            })
            .await
    }

    /// Starts periodic database maintenance for writable bucket connections.
    pub(super) fn spawn_maintenance_task(
        buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
        config: crate::Config,
    ) {
        if config.maintenance_interval_seconds == 0 {
            return;
        }

        let maintenance_interval =
            std::time::Duration::from_secs(config.maintenance_interval_seconds);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(maintenance_interval).await;

                let multipart_expire_before =
                    expiration_threshold(config.multipart_upload_ttl_seconds);
                let completed_multipart_expire_before =
                    expiration_threshold(config.completed_multipart_upload_ttl_seconds);
                let connections = buckets
                    .read()
                    .await
                    .iter()
                    .filter(|(bucket, _)| config.read_only(Some(bucket)).not())
                    .map(|(_, connection)| connection.clone())
                    .collect::<Vec<_>>();

                for connection in connections {
                    Self::try_maintain_connection(
                        &connection,
                        multipart_expire_before,
                        completed_multipart_expire_before,
                        config.maintenance_vacuum_pages,
                    )
                    .await
                    .ok();
                }
            }
        });
    }

    /// Runs one maintenance pass for a writable bucket connection.
    pub(super) async fn try_maintain_connection(
        connection: &BucketConnection,
        multipart_expire_before: Option<OffsetDateTime>,
        completed_multipart_expire_before: Option<OffsetDateTime>,
        vacuum_pages: u32,
    ) -> SqliteResult<()> {
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_delete_expired_multipart_uploads(
                    &transaction,
                    multipart_expire_before,
                    completed_multipart_expire_before,
                )?;
                transaction.commit()?;

                connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
                if vacuum_pages > 0 {
                    connection
                        .execute_batch(&format!("PRAGMA incremental_vacuum({vacuum_pages});"))?;
                }

                Ok(())
            })
            .await
    }
    /// Deletes incomplete and completed multipart records older than configured cutoffs.
    pub(crate) fn try_delete_expired_multipart_uploads(
        transaction: &Transaction,
        incomplete_expire_before: Option<OffsetDateTime>,
        completed_expire_before: Option<OffsetDateTime>,
    ) -> SqliteResult<()> {
        if let Some(expire_before) = incomplete_expire_before {
            Self::try_delete_incomplete_multipart_uploads_expired(transaction, expire_before)?;
        }
        if let Some(expire_before) = completed_expire_before {
            Self::try_delete_completed_multipart_uploads_expired(transaction, expire_before)?;
        }

        Ok(())
    }

    /// Deletes incomplete multipart uploads older than the expiration timestamp.
    fn try_delete_incomplete_multipart_uploads_expired(
        transaction: &Transaction,
        expire_before: OffsetDateTime,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM multipart_upload
            WHERE DATETIME(last_modified) < DATETIME(?1);",
        )?;
        Ok(stmt.execute([expire_before])?)
    }

    /// Deletes completed multipart idempotency records older than the expiration timestamp.
    fn try_delete_completed_multipart_uploads_expired(
        transaction: &Transaction,
        expire_before: OffsetDateTime,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM completed_multipart_upload
            WHERE DATETIME(last_modified) < DATETIME(?1);",
        )?;
        Ok(stmt.execute([expire_before])?)
    }
}
#[cfg(test)]
mod tests {
    use time::{Duration, OffsetDateTime};
    use uuid::Uuid;

    use crate::sqlite::{
        ChecksumMetadata, CompletedMultipartUploadRecord, Multipart, MultipartUploadState,
        NULL_VERSION_ID, Sqlite, SqliteResult,
    };
    use rusqlite::Connection;

    fn open_test_connection() -> rusqlite::Result<Connection> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        Ok(connection)
    }

    #[test]
    fn multipart_touch_keeps_active_upload_out_of_cleanup() -> SqliteResult<()> {
        let mut connection = open_test_connection()?;
        let transaction = connection.transaction()?;
        Sqlite::try_create_tables(&transaction)?;

        let upload_id = Uuid::new_v4();
        Sqlite::try_create_multipart_upload(
            &transaction,
            upload_id,
            "bucket",
            "key",
            MultipartUploadState {
                access_key: None,
                metadata: None,
                content_type: None,
                content_encoding: None,
                cache_control: None,
                expires: None,
                checksum: ChecksumMetadata::default(),
            },
        )?;
        transaction.execute(
            "UPDATE multipart_upload SET last_modified = ?2 WHERE upload_id = ?1;",
            (
                upload_id,
                OffsetDateTime::now_utc().saturating_sub(Duration::hours(2)),
            ),
        )?;

        Sqlite::try_touch_multipart_upload(&transaction, upload_id)?;
        Sqlite::try_delete_expired_multipart_uploads(
            &transaction,
            Some(OffsetDateTime::now_utc().saturating_sub(Duration::hours(1))),
            None,
        )?;

        let state =
            Sqlite::try_get_multipart_upload_state(&transaction, upload_id, "bucket", "key")?;
        assert!(state.is_some());

        Ok(())
    }

    #[test]
    fn multipart_cleanup_keeps_completed_ledger_separate() -> SqliteResult<()> {
        let mut connection = open_test_connection()?;
        let transaction = connection.transaction()?;
        Sqlite::try_create_tables(&transaction)?;

        let upload_id = Uuid::new_v4();
        Sqlite::try_create_multipart_upload(
            &transaction,
            upload_id,
            "bucket",
            "key",
            MultipartUploadState {
                access_key: None,
                metadata: None,
                content_type: None,
                content_encoding: None,
                cache_control: None,
                expires: None,
                checksum: ChecksumMetadata::default(),
            },
        )?;
        Sqlite::try_put_multipart(
            &transaction,
            Multipart {
                upload_id,
                part_number: 1,
                last_modified: OffsetDateTime::now_utc(),
                value: b"part".to_vec(),
                size: 4,
                md5: None,
                checksum: ChecksumMetadata::default(),
            },
        )?;
        transaction.execute(
            "UPDATE multipart_upload SET last_modified = ?2 WHERE upload_id = ?1;",
            (
                upload_id,
                OffsetDateTime::now_utc().saturating_sub(Duration::hours(2)),
            ),
        )?;

        let completed_upload_id = Uuid::new_v4();
        Sqlite::try_put_completed_multipart_upload(
            &transaction,
            CompletedMultipartUploadRecord {
                upload_id: completed_upload_id,
                bucket: "bucket",
                key: "completed-key",
                access_key: None,
                md5: "etag",
                version_id: NULL_VERSION_ID,
                returned_version_id: None,
                checksum: &ChecksumMetadata::default(),
            },
        )?;
        transaction.execute(
            "UPDATE completed_multipart_upload SET last_modified = ?2 WHERE upload_id = ?1;",
            (
                completed_upload_id,
                OffsetDateTime::now_utc().saturating_sub(Duration::hours(2)),
            ),
        )?;

        let expire_before = OffsetDateTime::now_utc().saturating_sub(Duration::hours(1));
        Sqlite::try_delete_expired_multipart_uploads(&transaction, Some(expire_before), None)?;

        let upload_state =
            Sqlite::try_get_multipart_upload_state(&transaction, upload_id, "bucket", "key")?;
        assert!(upload_state.is_none());
        assert!(Sqlite::try_get_multiparts(&transaction, upload_id)?.is_empty());

        let completed = Sqlite::try_get_completed_multipart_upload(
            &transaction,
            completed_upload_id,
            "bucket",
            "completed-key",
        )?;
        assert!(completed.is_some());

        Sqlite::try_delete_expired_multipart_uploads(&transaction, None, Some(expire_before))?;
        let completed = Sqlite::try_get_completed_multipart_upload(
            &transaction,
            completed_upload_id,
            "bucket",
            "completed-key",
        )?;
        assert!(completed.is_none());

        Ok(())
    }
}
