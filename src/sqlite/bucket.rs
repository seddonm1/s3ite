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
    pub(super) async fn try_open_configured_buckets(
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
            Self::try_prepare_existing_bucket(&connection, config, &bucket).await?;
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
    pub(crate) async fn try_get_connection(
        &self,
        bucket: &str,
    ) -> SqliteResult<Arc<BucketConnection>> {
        Ok(self
            .buckets
            .read()
            .await
            .get(bucket)
            .ok_or(SqliteError::BucketNotFound)?
            .to_owned())
    }

    /// Creates or opens a bucket database and registers its connection.
    pub(crate) async fn try_create_bucket(&self, bucket: &str) -> SqliteResult<()> {
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
                Self::try_create_tables(&transaction)?;
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
    pub(crate) fn try_get_bucket_versioning_state(
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
    pub(crate) fn try_put_bucket_versioning_state(
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
