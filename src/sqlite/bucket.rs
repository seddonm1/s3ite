use std::{
    collections::HashMap,
    ops::Not,
    path::{Path, PathBuf},
    sync::Arc,
};

use path_absolutize::Absolutize;
use rusqlite::Transaction;
use tokio::fs;

use super::{
    BucketConnection, BucketOpen, BucketVersioningState, EncryptionMode, Sqlite, SqliteError,
    SqliteResult,
};

impl Sqlite {
    /// Opens all existing bucket databases under the configured root.
    pub(super) async fn open_configured_buckets(
        root: &Path,
        config: &crate::Config,
    ) -> SqliteResult<HashMap<String, Arc<BucketConnection>>> {
        let mut buckets = HashMap::new();
        let mut iter = fs::read_dir(root).await?;

        while let Some(entry) = iter.next_entry().await? {
            if entry.file_type().await?.is_file().not() {
                continue;
            }

            let path = entry.path();
            if path
                .extension()
                .is_none_or(|extension| extension != "sqlite3")
            {
                continue;
            }

            let Some(bucket) = path
                .file_stem()
                .and_then(|file_stem| file_stem.to_str())
                .map(str::to_string)
            else {
                continue;
            };

            Self::prepare_bucket_file(path.clone(), config, &bucket, BucketOpen::Startup).await?;
            let connection = BucketConnection::open(path, config, &bucket).await?;
            buckets.insert(bucket, Arc::new(connection));
        }

        Ok(buckets)
    }

    /// Resolves a path relative to the configured virtual root.
    pub(crate) fn resolve_abs_path(&self, path: impl AsRef<Path>) -> SqliteResult<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    /// Resolves the database file path for a bucket.
    pub(crate) fn get_bucket_path(&self, bucket: &str) -> SqliteResult<PathBuf> {
        let dir = PathBuf::from(format!("{bucket}.sqlite3"));
        self.resolve_abs_path(dir)
    }

    /// Returns an open database connection for a bucket.
    pub(crate) async fn get_connection(&self, bucket: &str) -> SqliteResult<Arc<BucketConnection>> {
        Ok(self
            .buckets
            .read()
            .await
            .get(bucket)
            .ok_or(SqliteError::BucketNotFound)?
            .to_owned())
    }

    /// Creates or opens a bucket database and registers its connection.
    ///
    /// A newly initialized database enables `object_lock` in its schema
    /// transaction, before any worker can serve the bucket. The lifecycle lock
    /// keeps deletion from unlinking the database while it is being opened.
    pub(crate) async fn create_bucket(
        &self,
        bucket: &str,
        object_lock: bool,
    ) -> SqliteResult<bool> {
        self.validate_mutable_bucket(bucket)?;
        let _lifecycle = self.bucket_lifecycle.lock().await;

        if self.buckets.read().await.contains_key(bucket) {
            return Ok(false);
        }

        let file_path = self.get_bucket_path(bucket)?;
        let config = self.config.clone();
        Self::prepare_bucket_file(
            file_path.clone(),
            &config,
            bucket,
            BucketOpen::Create { object_lock },
        )
        .await?;

        let connection = BucketConnection::open(file_path, &config, bucket).await?;
        self.buckets
            .write()
            .await
            .insert(bucket.to_string(), Arc::new(connection));

        Ok(true)
    }

    /// Deletes an empty bucket database and its `SQLite` sidecar files.
    ///
    /// Holds the lifecycle lock through connection shutdown and file removal.
    pub(crate) async fn delete_bucket(&self, bucket: &str) -> SqliteResult<()> {
        self.validate_mutable_bucket(bucket)?;
        let _lifecycle = self.bucket_lifecycle.lock().await;

        let connection = self.get_connection(bucket).await?;
        let close_connection = connection.clone();
        let has_objects = connection
            .write(move |transaction| {
                let has_objects = Self::has_objects(transaction)?;
                if !has_objects {
                    close_connection.close_to_writes();
                }
                Ok(has_objects)
            })
            .await?;
        if has_objects {
            return Err(SqliteError::BucketNotEmpty);
        }

        let connection = self
            .buckets
            .write()
            .await
            .remove(bucket)
            .ok_or(SqliteError::BucketNotFound)?;
        connection.close().await?;

        let bucket_path = self.get_bucket_path(bucket)?;
        Self::remove_bucket_files(&bucket_path).await?;

        Ok(())
    }

    /// Removes a bucket database and best-effort sidecar files created by WAL mode.
    async fn remove_bucket_files(bucket_path: &Path) -> SqliteResult<()> {
        fs::remove_file(bucket_path).await?;
        for suffix in ["-wal", "-shm"] {
            let mut path = bucket_path.as_os_str().to_owned();
            path.push(suffix);
            fs::remove_file(PathBuf::from(path)).await.ok();
        }
        Ok(())
    }

    /// Rejects mutations for read-only bucket configuration.
    pub(crate) fn validate_mutable_bucket(&self, bucket: &str) -> SqliteResult<()> {
        if self.config.read_only(Some(bucket)) {
            return Err(SqliteError::BucketReadOnly);
        }
        Ok(())
    }

    /// Returns the current bucket versioning state.
    pub(crate) fn get_bucket_versioning_state(
        transaction: &Transaction,
    ) -> SqliteResult<BucketVersioningState> {
        let mut stmt = transaction
            .prepare_cached("SELECT versioning_state FROM bucket_state WHERE id = 1;")?;
        let state = stmt.query_row((), |row| row.get(0))?;
        Ok(state)
    }

    /// Updates the bucket versioning state.
    pub(crate) fn put_bucket_versioning_state(
        transaction: &Transaction,
        state: BucketVersioningState,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE bucket_state
            SET versioning_state = ?1
            WHERE id = 1;",
        )?;
        Ok(stmt.execute([state])?)
    }

    /// Returns the encryption default applied to new objects in this bucket.
    pub(crate) fn get_bucket_encryption(
        transaction: &Transaction,
    ) -> SqliteResult<Option<EncryptionMode>> {
        let mut stmt = transaction
            .prepare_cached("SELECT default_encryption FROM bucket_state WHERE id = 1;")?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Replaces the default encryption mode atomically, or clears it on deletion.
    pub(crate) fn put_bucket_encryption(
        transaction: &Transaction,
        mode: Option<&EncryptionMode>,
    ) -> SqliteResult<usize> {
        Ok(transaction.execute(
            "UPDATE bucket_state SET default_encryption = ?1 WHERE id = 1;",
            [mode],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use time::OffsetDateTime;
    use tokio::sync::RwLock;

    use crate::{Config, config::Bucket, sqlite::DEFAULT_OBJECT_BLOB_CHUNK_SIZE};

    use super::*;

    /// Builds storage with custom configuration and an empty bucket registry.
    fn sqlite_with_config(config: Config) -> Sqlite {
        Sqlite {
            root: PathBuf::new(),
            config,
            buckets: Arc::new(RwLock::new(HashMap::new())),
            bucket_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            blob_chunk_size: DEFAULT_OBJECT_BLOB_CHUNK_SIZE,
            requests: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    /// Builds storage rooted at a test directory with an empty bucket registry.
    fn sqlite_with_root(root: PathBuf) -> Sqlite {
        Sqlite {
            root,
            config: Config::default(),
            buckets: Arc::new(RwLock::new(HashMap::new())),
            bucket_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            blob_chunk_size: DEFAULT_OBJECT_BLOB_CHUNK_SIZE,
            requests: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    fn temp_root(name: &str) -> SqliteResult<PathBuf> {
        let root = std::env::temp_dir().join(format!("s3ite-{name}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        Ok(root)
    }

    #[test]
    fn service_read_only_blocks_bucket_mutations() {
        let sqlite = sqlite_with_config(Config {
            read_only: true,
            ..Default::default()
        });

        let err = match sqlite.validate_mutable_bucket("bucket") {
            Ok(()) => panic!("service read-only config should block mutations"),
            Err(err) => err,
        };
        assert!(matches!(err, SqliteError::BucketReadOnly));
    }

    #[test]
    fn bucket_read_only_override_blocks_only_that_bucket() {
        let mut config = Config::default();
        config.buckets.insert(
            "locked".to_string(),
            Bucket {
                read_only: Some(true),
                sqlite: None,
            },
        );
        config.buckets.insert(
            "writable".to_string(),
            Bucket {
                read_only: Some(false),
                sqlite: None,
            },
        );
        let sqlite = sqlite_with_config(config);

        let err = match sqlite.validate_mutable_bucket("locked") {
            Ok(()) => panic!("bucket read-only config should block mutations"),
            Err(err) => err,
        };
        assert!(matches!(err, SqliteError::BucketReadOnly));
        assert!(sqlite.validate_mutable_bucket("writable").is_ok());
        assert!(sqlite.validate_mutable_bucket("unconfigured").is_ok());
    }

    #[tokio::test]
    async fn opens_only_sqlite_bucket_files_from_root() -> SqliteResult<()> {
        let root = temp_root("open-buckets")?;
        fs::create_dir(root.join("directory.sqlite3"))?;
        fs::write(root.join("ignored.txt"), b"not a bucket")?;
        fs::File::create(root.join("bucket.sqlite3"))?;

        let buckets = Sqlite::open_configured_buckets(&root, &Config::default()).await?;
        assert_eq!(buckets.len(), 1);
        assert!(buckets.contains_key("bucket"));
        for connection in buckets.values() {
            connection.close().await?;
        }

        fs::remove_dir_all(root).ok();
        Ok(())
    }

    /// Ensures deleting an empty bucket removes its file and registry entry.
    #[tokio::test]
    async fn delete_bucket_removes_empty_bucket_file_and_connection() -> SqliteResult<()> {
        let root = temp_root("delete-empty-bucket")?;
        let sqlite = sqlite_with_root(root.clone());

        assert!(sqlite.create_bucket("bucket", false).await?);
        let path = sqlite.get_bucket_path("bucket")?;
        assert!(path.exists());

        sqlite.delete_bucket("bucket").await?;

        assert!(!path.exists());
        assert!(!sqlite.buckets.read().await.contains_key("bucket"));

        fs::remove_dir_all(root).ok();
        Ok(())
    }

    /// Commits a delete marker in the bucket.
    async fn write_delete_marker(connection: &BucketConnection) -> SqliteResult<()> {
        connection
            .write(|transaction| {
                transaction.execute(
                    "
                    INSERT INTO object_version (
                        key,
                        version_id,
                        is_latest,
                        is_delete_marker,
                        size,
                        last_modified,
                        md5
                    )
                    VALUES ('key', 'null', 1, 1, 0, ?1, NULL);",
                    [OffsetDateTime::now_utc()],
                )?;
                Ok(())
            })
            .await
    }

    /// Ensures deleting a nonempty bucket preserves its file and registry entry.
    #[tokio::test]
    async fn delete_bucket_refuses_nonempty_bucket() -> SqliteResult<()> {
        let root = temp_root("delete-nonempty-bucket")?;
        let sqlite = sqlite_with_root(root.clone());

        assert!(sqlite.create_bucket("bucket", false).await?);
        let path = sqlite.get_bucket_path("bucket")?;
        let connection = sqlite.get_connection("bucket").await?;
        write_delete_marker(&connection).await?;

        let err = sqlite.delete_bucket("bucket").await.err();
        assert!(matches!(err, Some(SqliteError::BucketNotEmpty)));
        assert!(path.exists());
        assert!(sqlite.buckets.read().await.contains_key("bucket"));

        connection.close().await?;
        fs::remove_dir_all(root).ok();
        Ok(())
    }

    /// Queues a create behind a delete that has started but cannot read the registry.
    #[tokio::test]
    async fn create_bucket_waits_for_in_progress_delete() -> SqliteResult<()> {
        let root = temp_root("create-during-delete")?;
        let sqlite = sqlite_with_root(root.clone());
        assert!(sqlite.create_bucket("bucket", false).await?);

        // Stop the delete at its first registry lookup after it takes the lifecycle lock.
        let buckets = sqlite.buckets.write().await;
        let delete = sqlite.delete_bucket("bucket");
        tokio::pin!(delete);
        tokio::select! {
            biased;
            result = &mut delete => panic!("delete finished with the registry locked: {result:?}"),
            () = std::future::ready(()) => {}
        }
        assert!(sqlite.bucket_lifecycle.try_lock().is_err());
        drop(buckets);

        let create = sqlite.create_bucket("bucket", false);
        tokio::pin!(create);
        tokio::select! {
            biased;
            result = &mut create => panic!("create finished while delete held the lock: {result:?}"),
            () = std::future::ready(()) => {}
        }

        let (deleted, created) = tokio::join!(delete, create);
        deleted?;
        assert!(created?);
        sqlite.delete_bucket("bucket").await?;
        fs::remove_dir_all(root).ok();
        Ok(())
    }

    /// Verifies that reopening a recreated bucket loads its committed delete marker.
    #[tokio::test]
    async fn recreated_bucket_keeps_committed_write_after_reopen() -> SqliteResult<()> {
        let root = temp_root("recreate-bucket")?;
        let sqlite = sqlite_with_root(root.clone());
        assert!(sqlite.create_bucket("bucket", false).await?);
        sqlite.delete_bucket("bucket").await?;
        assert!(sqlite.create_bucket("bucket", false).await?);

        let connection = sqlite.get_connection("bucket").await?;
        write_delete_marker(&connection).await?;
        connection.close().await?;
        drop(connection);
        drop(sqlite);

        let buckets = Sqlite::open_configured_buckets(&root, &Config::default()).await?;
        let connection = buckets.get("bucket").ok_or(SqliteError::BucketNotFound)?;
        let is_delete_marker: bool = connection
            .read(|transaction| {
                Ok(transaction.query_row(
                    "SELECT is_delete_marker FROM object_version WHERE key = 'key';",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await?;
        assert!(is_delete_marker);

        connection.close().await?;
        fs::remove_dir_all(root).ok();
        Ok(())
    }
}
