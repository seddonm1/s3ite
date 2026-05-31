use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Not,
    path::{Path, PathBuf},
    sync::Arc,
};

use path_absolutize::Absolutize;
use rusqlite::{OptionalExtension, Transaction};
use time::{Duration, OffsetDateTime};
use tokio::{fs, sync::RwLock};
use tracing::debug;

use crate::database::BucketConnection;

mod checksum;
mod error;
mod list;
mod maintenance;
mod multipart;
mod object;
mod schema;
mod types;
mod versioning;

pub use self::error::{SqliteError, SqliteResult};
pub(crate) use self::{
    checksum::ChecksumMetadata,
    types::{
        BucketVersioningState, CompletedMultipartUploadOutcome, CompletedMultipartUploadRecord,
        CompletedMultipartUploadState, CompletedObjectPart, KeyMetadata, KeyValue, Multipart,
        MultipartCompletionPart, MultipartCompletionRequest, MultipartMetadata,
        MultipartUploadState, MultipartUploadSummary, ObjectDeleteOutcome, ObjectSummary,
        ObjectTagCondition, ObjectWriteConditions, ObjectWriteOutcome,
    },
    versioning::NULL_VERSION_ID,
};
use self::{
    checksum::{
        CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT, checksum_value, decode_base64,
        decode_md5_hex, encode_hex, row_checksum,
    },
    types::{row_bool, row_size, sql_limit, sql_size},
};

/// SQLite-backed storage state and open bucket connections.
#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
}

impl Sqlite {
    /// Opens configured bucket databases and starts background maintenance.
    ///
    /// # Errors
    ///
    /// Returns an error if the root path, configured bucket databases, or bucket
    /// configuration cannot be loaded.
    pub async fn new(config: &crate::Config) -> SqliteResult<Self> {
        if config.readers == 0 {
            Err(SqliteError::Configuration(
                "readers must be greater than zero".to_string(),
            ))?;
        }

        let root = env::current_dir()?.join(&config.root).canonicalize()?;
        let buckets = Self::try_open_configured_buckets(&root, config).await?;

        // validate that any specified bucket configurations have existing bucket
        let difference = config
            .buckets
            .keys()
            .collect::<HashSet<_>>()
            .difference(&buckets.keys().collect::<HashSet<_>>())
            .copied()
            .collect::<Vec<_>>();
        if difference.is_empty().not() {
            return Err(SqliteError::Configuration(format!(
                "found configurations for buckets: {difference:?} that do not exist"
            )));
        }

        let buckets = Arc::new(RwLock::new(buckets));
        Self::spawn_maintenance_task(buckets.clone(), config.clone());

        Ok(Self {
            root,
            config: config.clone(),
            buckets,
        })
    }

    /// Opens all existing bucket databases under the configured root.
    async fn try_open_configured_buckets(
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
}
