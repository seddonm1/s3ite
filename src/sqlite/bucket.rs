use std::{
    collections::HashMap,
    ops::Not,
    path::{Path, PathBuf},
    sync::Arc,
};

use path_absolutize::Absolutize;
use rusqlite::Transaction;
use tokio::fs;
use tracing::debug;

use super::{BucketConnection, BucketVersioningState, Sqlite, SqliteError, SqliteResult};

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

            let connection = BucketConnection::open(path, config, &bucket).await?;
            Self::prepare_existing_bucket(&connection, config, &bucket).await?;
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
    pub(crate) async fn create_bucket(&self, bucket: &str) -> SqliteResult<()> {
        self.validate_mutable_bucket(bucket)?;

        if self.buckets.read().await.contains_key(bucket) {
            return Ok(());
        }

        let file_path = self.get_bucket_path(bucket)?;
        let config = self.config.clone();

        let connection = BucketConnection::open(file_path, &config, bucket).await?;

        connection
            .write(move |connection| {
                connection.execute_batch(&config.to_sql(None))?;

                let transaction = connection.transaction()?;
                Self::create_tables(&transaction)?;
                Ok(transaction.commit()?)
            })
            .await?;

        let mut buckets = self.buckets.write().await;
        if buckets.contains_key(bucket).not() {
            buckets.insert(bucket.to_string(), Arc::new(connection));
            debug!(bucket = %bucket, "created bucket");
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
        let state = stmt.query_row((), |row| {
            let value = row.get::<_, String>(0)?;
            BucketVersioningState::from_sql(&value)
        })?;
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
        Ok(stmt.execute([state.as_sql()])?)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tokio::sync::RwLock;

    use crate::{Config, config::Bucket};

    use super::*;

    fn sqlite_with_config(config: Config) -> Sqlite {
        Sqlite {
            root: PathBuf::new(),
            config,
            buckets: Arc::new(RwLock::new(HashMap::new())),
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
}
