use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Not,
    path::{Path, PathBuf},
    sync::Arc,
};

use md5::{Digest, Md5};
use path_absolutize::Absolutize;
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction};
use time::{Duration, OffsetDateTime};
use tokio::{fs, sync::RwLock};
use tracing::debug;
use uuid::Uuid;

use crate::database::BucketConnection;

/// Result type returned by `SQLite` storage operations.
pub type SqliteResult<T> = std::result::Result<T, SqliteError>;

/// Storage-domain errors returned by the `SQLite` layer.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// An `SQLite` connection or query failed.
    #[error("Rusqlite {0}")]
    Rusqlite(#[from] rusqlite::Error),
    /// A filesystem operation failed.
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    /// A database worker queue has reached its bounded capacity.
    #[error("database queue is full")]
    DatabaseQueueFull,
    /// A database worker queue is closed.
    #[error("database queue is closed")]
    DatabaseQueueClosed,
    /// A requested bucket database is not open.
    #[error("bucket does not exist")]
    BucketNotFound,
    /// A requested bucket is configured as read-only.
    #[error("database is in read-only mode")]
    BucketReadOnly,
    /// A requested object does not exist.
    #[error("object does not exist")]
    ObjectNotFound,
    /// Storage configuration is invalid.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// A database worker thread panicked or could not be joined.
    #[error("database worker thread panicked")]
    ThreadJoin,
    /// A requested multipart upload does not exist.
    #[error("multipart upload does not exist")]
    MultipartUploadNotFound,
    /// Multipart upload credentials do not match the stored upload owner.
    #[error("multipart upload access denied")]
    MultipartAccessDenied,
    /// A requested multipart part is missing or does not match stored data.
    #[error("invalid multipart part")]
    InvalidMultipartPart,
    /// A non-final multipart part is smaller than the minimum part size.
    #[error("multipart part is too small")]
    MultipartPartTooSmall,
    /// A completed multipart object is too large to store.
    #[error("multipart object is too large")]
    MultipartObjectTooLarge,
}

#[derive(Debug, thiserror::Error)]
#[error("object size is negative")]
struct NegativeObjectSize;

#[derive(Debug, thiserror::Error)]
#[error("SQL limit is negative")]
struct NegativeSqlLimit;

fn row_size(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<i64> {
    let value = row.get::<_, i64>(idx)?;
    if value < 0 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Integer,
            Box::new(NegativeObjectSize),
        ));
    }
    Ok(value)
}

fn sql_size(value: i64) -> rusqlite::Result<i64> {
    if value < 0 {
        return Err(ToSqlConversionFailure(Box::new(NegativeObjectSize)));
    }
    Ok(value)
}

fn sql_limit(value: i64) -> rusqlite::Result<i64> {
    if value < 0 {
        return Err(ToSqlConversionFailure(Box::new(NegativeSqlLimit)));
    }
    Ok(value)
}

fn expiration_threshold(ttl_seconds: u64) -> Option<OffsetDateTime> {
    if ttl_seconds == 0 {
        return None;
    }

    let seconds = i64::try_from(ttl_seconds).unwrap_or(i64::MAX);
    Some(OffsetDateTime::now_utc().saturating_sub(Duration::seconds(seconds)))
}

fn repeat_vars(count: usize) -> String {
    assert_ne!(count, 0);
    let mut vars = "?,".repeat(count);
    vars.pop();
    vars
}

fn encode_hex(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let input = input.as_ref();
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_md5_hex(value: &str) -> SqliteResult<[u8; 16]> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    if bytes.len() != 32 {
        return Err(SqliteError::InvalidMultipartPart);
    }

    let mut decoded = [0; 16];
    for (idx, pair) in bytes.chunks_exact(2).enumerate() {
        let upper = nibble(pair[0]).ok_or(SqliteError::InvalidMultipartPart)?;
        let lower = nibble(pair[1]).ok_or(SqliteError::InvalidMultipartPart)?;
        decoded[idx] = (upper << 4) | lower;
    }
    Ok(decoded)
}

/// SQLite-backed storage state and open bucket connections.
#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
}

/// Object body and metadata loaded from the bucket database.
#[derive(Debug)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) key_metadata: KeyMetadata,
}

/// Minimal object metadata returned by list queries.
#[derive(Debug)]
pub(crate) struct ObjectSummary {
    pub(crate) key: String,
    pub(crate) size: i64,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

/// Object metadata loaded without the object body.
#[derive(Debug)]
pub(crate) struct KeyMetadata {
    pub(crate) size: i64,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) content_type: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

/// Stored multipart upload part with its body and checksum metadata.
#[derive(Debug)]
pub(crate) struct Multipart {
    pub(crate) upload_id: Uuid,
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) value: Vec<u8>,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
}

/// Multipart upload part metadata returned by list-parts queries.
#[derive(Debug)]
pub(crate) struct MultipartMetadata {
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) size: i64,
}

/// Client-requested multipart part checksum used during completion.
#[derive(Debug)]
pub(crate) struct MultipartCompletionPart {
    pub(crate) part_number: i32,
    pub(crate) md5: String,
}

/// Initiated multipart upload state shared by all uploaded parts.
#[derive(Debug)]
pub(crate) struct MultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) content_type: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
}

/// Completion ledger entry used to make multipart completion idempotent.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) md5: String,
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

    /// Applies writable-bucket startup maintenance before serving requests.
    async fn try_prepare_existing_bucket(
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
    fn spawn_maintenance_task(
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
    async fn try_maintain_connection(
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

    /// Completes a multipart upload and records the completed object.
    pub(crate) async fn try_complete_multipart_upload(
        &self,
        bucket: String,
        key: String,
        upload_id: Uuid,
        requested_parts: Vec<MultipartCompletionPart>,
        access_key: Option<String>,
    ) -> SqliteResult<String> {
        self.validate_mutable_bucket(&bucket)?;

        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let md5 = Self::try_complete_multipart_upload_transaction(
                    &transaction,
                    &bucket,
                    &key,
                    upload_id,
                    &requested_parts,
                    access_key,
                )?;
                transaction.commit()?;
                Ok(md5)
            })
            .await
    }

    /// Validates and materializes multipart completion inside one transaction.
    fn try_complete_multipart_upload_transaction(
        transaction: &Transaction,
        bucket: &str,
        key: &str,
        upload_id: Uuid,
        requested_parts: &[MultipartCompletionPart],
        access_key: Option<String>,
    ) -> SqliteResult<String> {
        if requested_parts.is_empty() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let Some(upload_state) =
            Self::try_get_multipart_upload_state(transaction, upload_id, bucket, key)?
        else {
            let completed =
                Self::try_get_completed_multipart_upload(transaction, upload_id, bucket, key)?
                    .ok_or(SqliteError::MultipartUploadNotFound)?;
            Self::try_validate_multipart_access(
                completed.access_key.as_ref(),
                access_key.as_deref(),
            )?;
            return Ok(completed.md5);
        };
        Self::try_validate_multipart_access(
            upload_state.access_key.as_ref(),
            access_key.as_deref(),
        )?;

        let parts = Self::try_get_multiparts(transaction, upload_id)?;
        if parts.len() != requested_parts.len() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let total_size = parts.iter().try_fold(0usize, |total, part| {
            let part_size =
                usize::try_from(part.size).map_err(|_| SqliteError::InvalidMultipartPart)?;
            total
                .checked_add(part_size)
                .ok_or(SqliteError::MultipartObjectTooLarge)
        })?;

        let mut value = Vec::with_capacity(total_size);
        let mut part_md5s = Vec::with_capacity(parts.len() * 16);
        let final_part_number = requested_parts.len();
        for (index, (requested_part, part)) in requested_parts.iter().zip(parts).enumerate() {
            if part.part_number != requested_part.part_number {
                return Err(SqliteError::InvalidMultipartPart);
            }

            if index + 1 != final_part_number && part.size < 5 * 1024 * 1024 {
                return Err(SqliteError::MultipartPartTooSmall);
            }

            let Some(stored_md5) = part.md5.as_deref() else {
                return Err(SqliteError::InvalidMultipartPart);
            };
            if requested_part.md5 != stored_md5 {
                return Err(SqliteError::InvalidMultipartPart);
            }

            part_md5s.extend_from_slice(&decode_md5_hex(stored_md5)?);
            value.extend_from_slice(&part.value);
        }

        let mut md5_hash = Md5::new();
        md5_hash.update(&part_md5s);
        let md5 = format!(
            "{}-{}",
            encode_hex(md5_hash.finalize()),
            requested_parts.len()
        );
        let size = i64::try_from(value.len()).map_err(|_| SqliteError::MultipartObjectTooLarge)?;

        Self::try_put_object(
            transaction,
            KeyValue {
                key: key.to_string(),
                value: Some(value),
                key_metadata: KeyMetadata {
                    size,
                    metadata: upload_state.metadata,
                    content_type: upload_state.content_type,
                    cache_control: upload_state.cache_control,
                    expires: upload_state.expires,
                    last_modified: OffsetDateTime::now_utc(),
                    md5: Some(md5.clone()),
                },
            },
        )?;

        Self::try_delete_multipart(transaction, upload_id)?;
        Self::try_put_completed_multipart_upload(
            transaction,
            upload_id,
            bucket,
            key,
            access_key,
            &md5,
        )?;

        Ok(md5)
    }

    /// Ensures the bucket database schema exists and applies additive migrations.
    pub(crate) fn try_create_tables(transaction: &Transaction) -> SqliteResult<()> {
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS data (
                    key TEXT PRIMARY KEY,
                    value BLOB
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    metadata TEXT,
                    content_type TEXT,
                    cache_control TEXT,
                    expires TEXT,
                    last_modified TEXT NOT NULL,
                    md5 TEXT,
                    FOREIGN KEY (key) REFERENCES data (key) ON DELETE CASCADE
                ) WITHOUT ROWID;",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS multipart_upload (
                    upload_id BLOB NOT NULL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    access_key TEXT,
                    metadata TEXT,
                    content_type TEXT,
                    cache_control TEXT,
                    expires TEXT,
                    UNIQUE(upload_id, bucket, key)
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS completed_multipart_upload (
                    upload_id BLOB NOT NULL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    access_key TEXT,
                    md5 TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    UNIQUE(upload_id, bucket, key)
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS multipart_upload_part (
                    upload_id BLOB NOT NULL,
                    last_modified TEXT NOT NULL,
                    part_number INTEGER NOT NULL,
                    value BLOB NOT NULL,
                    size INTEGER NOT NULL,
                    md5 TEXT,
                    PRIMARY KEY (upload_id, part_number),
                    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE
                );",
            (),
        )?;

        // s3ite 0.7.0 started persisting HTTP metadata on objects and multipart
        // uploads. `CREATE TABLE IF NOT EXISTS` leaves pre-0.7.0 bucket databases
        // unchanged, so keep these additive compatibility changes here until a
        // formal migration system exists.
        for (table, column, definition) in [
            ("metadata", "content_type", "content_type TEXT"),
            ("metadata", "cache_control", "cache_control TEXT"),
            ("metadata", "expires", "expires TEXT"),
            ("multipart_upload", "metadata", "metadata TEXT"),
            ("multipart_upload", "content_type", "content_type TEXT"),
            ("multipart_upload", "cache_control", "cache_control TEXT"),
            ("multipart_upload", "expires", "expires TEXT"),
        ] {
            if Self::try_has_column(transaction, table, column)?.not() {
                transaction
                    .execute(&format!("ALTER TABLE {table} ADD COLUMN {definition};"), ())?;
            }
        }

        Ok(())
    }

    /// Returns whether a table already contains a column.
    fn try_has_column(transaction: &Transaction, table: &str, column: &str) -> SqliteResult<bool> {
        let mut stmt = transaction.prepare(&format!("PRAGMA table_info({table});"))?;
        let columns = stmt
            .query_map((), |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(columns.iter().any(|name| name == column))
    }

    /// Lists object metadata ordered by key for keyset pagination.
    pub(crate) fn try_list_objects(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        limit: i64,
    ) -> SqliteResult<Vec<ObjectSummary>> {
        let limit = sql_limit(limit)?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let map_summary = |row: &rusqlite::Row<'_>| {
            Ok(ObjectSummary {
                key: row.get(0)?,
                size: row_size(row, 1)?,
                last_modified: row.get(2)?,
                md5: row.get(3)?,
            })
        };

        let objects = match (prefix, after_key) {
            (Some(prefix), Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata WHERE key >= ?1 AND key > ?2 ORDER BY key LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata WHERE key >= ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((prefix, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata WHERE key > ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata ORDER BY key LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(objects)
    }

    /// Returns whether the bucket contains any objects.
    pub(crate) fn try_has_objects(transaction: &Transaction) -> SqliteResult<bool> {
        let mut stmt = transaction.prepare_cached("SELECT EXISTS(SELECT 1 FROM metadata);")?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Fetches an object body and metadata by key.
    pub(crate) fn try_get_object(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyValue>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                metadata.key,
                data.value,
                metadata.size,
                metadata.metadata,
                metadata.content_type,
                metadata.cache_control,
                metadata.expires,
                metadata.last_modified,
                metadata.md5
            FROM metadata
            INNER JOIN data ON metadata.key = data.key
            WHERE metadata.key = ?;",
        )?;

        Ok(stmt
            .query_row([key], |row| {
                Ok(KeyValue {
                    key: row.get(0)?,
                    value: row.get(1)?,
                    key_metadata: KeyMetadata {
                        size: row_size(row, 2)?,
                        metadata: row
                            .get::<_, Option<String>>(3)?
                            .map(|metadata| serde_json::from_str(&metadata))
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(err),
                                )
                            })?,
                        content_type: row.get(4)?,
                        cache_control: row.get(5)?,
                        expires: row.get(6)?,
                        last_modified: row.get(7)?,
                        md5: row.get(8)?,
                    },
                })
            })
            .optional()?)
    }

    /// Fetches object metadata by key without reading the body.
    pub(crate) fn try_get_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5
            FROM metadata
            WHERE key = ?;",
        )?;

        Ok(stmt
            .query_row([key], |row| {
                Ok(KeyMetadata {
                    size: row_size(row, 0)?,
                    metadata: row
                        .get::<_, Option<String>>(1)?
                        .map(|metadata| serde_json::from_str(&metadata))
                        .transpose()
                        .map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    content_type: row.get(2)?,
                    cache_control: row.get(3)?,
                    expires: row.get(4)?,
                    last_modified: row.get(5)?,
                    md5: row.get(6)?,
                })
            })
            .optional()?)
    }

    /// Inserts or replaces an object body and metadata.
    pub(crate) fn try_put_object(transaction: &Transaction, kv: KeyValue) -> SqliteResult<usize> {
        let KeyValue {
            key,
            value,
            key_metadata,
        } = kv;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO data (key, value)
            VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE
            SET value=excluded.value;",
        )?;

        stmt.execute((&key, value))?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO metadata (key, size, metadata, content_type, cache_control, expires, last_modified, md5)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(key) DO UPDATE
            SET size=excluded.size, metadata=excluded.metadata, content_type=excluded.content_type, cache_control=excluded.cache_control, expires=excluded.expires, last_modified=excluded.last_modified, md5=excluded.md5;",
        )?;

        let size = sql_size(key_metadata.size)?;
        Ok(stmt.execute((
            key,
            size,
            key_metadata
                .metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            key_metadata.content_type,
            key_metadata.cache_control,
            key_metadata.expires,
            key_metadata.last_modified,
            key_metadata.md5,
        ))?)
    }

    /// Deletes an object by key.
    pub(crate) fn try_delete_object(transaction: &Transaction, key: &str) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM data
            WHERE key = ?1;",
        )?;
        Ok(stmt.execute([key])?)
    }

    /// Deletes multiple objects and returns the keys that existed.
    pub(crate) fn try_delete_objects(
        transaction: &Transaction,
        keys: &[String],
    ) -> SqliteResult<Vec<String>> {
        let vars = repeat_vars(keys.len());

        let mut stmt = transaction.prepare(&format!(
            "
            DELETE FROM data
            WHERE key IN ({vars})
            RETURNING key;"
        ))?;

        let keys = stmt
            .query_map(rusqlite::params_from_iter(keys), |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(keys)
    }

    /// Creates multipart upload state for an upload ID.
    pub(crate) fn try_create_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        state: MultipartUploadState,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload (upload_id, last_modified, bucket, key, access_key, metadata, content_type, cache_control, expires)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
        )?;

        let now = OffsetDateTime::now_utc();
        Ok(stmt.execute((
            upload_id,
            now,
            bucket,
            key,
            state.access_key,
            state
                .metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            state.content_type,
            state.cache_control,
            state.expires,
        ))?)
    }

    /// Fetches multipart upload state for validation and completion.
    pub(crate) fn try_get_multipart_upload_state(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
    ) -> SqliteResult<Option<MultipartUploadState>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT access_key, metadata, content_type, cache_control, expires
            FROM multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        Ok(stmt
            .query_row((upload_id, bucket, key), |row| {
                Ok(MultipartUploadState {
                    access_key: row.get(0)?,
                    metadata: row
                        .get::<_, Option<String>>(1)?
                        .map(|metadata| serde_json::from_str(&metadata))
                        .transpose()
                        .map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    content_type: row.get(2)?,
                    cache_control: row.get(3)?,
                    expires: row.get(4)?,
                })
            })
            .optional()?)
    }

    /// Refreshes multipart upload activity after a successful part upload.
    pub(crate) fn try_touch_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE multipart_upload
            SET last_modified = ?2
            WHERE upload_id = ?1;",
        )?;
        Ok(stmt.execute((upload_id, OffsetDateTime::now_utc()))?)
    }

    /// Records a completed multipart upload for idempotent retries.
    pub(crate) fn try_put_completed_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        access_key: Option<String>,
        md5: &str,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO completed_multipart_upload (upload_id, bucket, key, access_key, md5, last_modified)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(upload_id) DO UPDATE
            SET bucket=excluded.bucket, key=excluded.key, access_key=excluded.access_key, md5=excluded.md5, last_modified=excluded.last_modified;",
        )?;

        Ok(stmt.execute((
            upload_id,
            bucket,
            key,
            access_key,
            md5,
            OffsetDateTime::now_utc(),
        ))?)
    }

    /// Fetches completed multipart upload state by upload ID.
    pub(crate) fn try_get_completed_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
    ) -> SqliteResult<Option<CompletedMultipartUploadState>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT access_key, md5
            FROM completed_multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        Ok(stmt
            .query_row((upload_id, bucket, key), |row| {
                Ok(CompletedMultipartUploadState {
                    access_key: row.get(0)?,
                    md5: row.get(1)?,
                })
            })
            .optional()?)
    }

    /// Inserts or replaces a multipart upload part.
    pub(crate) fn try_put_multipart(
        transaction: &Transaction,
        multipart: Multipart,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload_part (upload_id, last_modified, part_number, value, size, md5)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(upload_id, part_number) DO UPDATE
            SET last_modified=excluded.last_modified, value=excluded.value, size=excluded.size, md5=excluded.md5;",
        )?;

        let size = sql_size(multipart.size)?;
        Ok(stmt.execute((
            multipart.upload_id,
            multipart.last_modified,
            multipart.part_number,
            multipart.value,
            size,
            multipart.md5,
        ))?)
    }

    /// Lists multipart upload part metadata ordered by part number.
    pub(crate) fn try_list_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<Vec<MultipartMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                size
            FROM multipart_upload_part
            WHERE upload_id = ?1
            ORDER BY part_number;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map([upload_id], |row| {
                Ok(MultipartMetadata {
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    size: row_size(row, 2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Fetches multipart upload parts with bodies ordered by part number.
    pub(crate) fn try_get_multiparts(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<Vec<Multipart>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                value,
                size,
                md5
            FROM multipart_upload_part
            WHERE upload_id = ?1
            ORDER BY part_number;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map([upload_id], |row| {
                Ok(Multipart {
                    upload_id,
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    value: row.get(2)?,
                    size: row_size(row, 3)?,
                    md5: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Deletes multipart upload state and cascading parts.
    pub(crate) fn try_delete_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM multipart_upload
            WHERE upload_id = ?1;",
        )?;
        Ok(stmt.execute([upload_id])?)
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

    /// Rejects mutations for read-only bucket configuration.
    pub(crate) fn validate_mutable_bucket(&self, bucket: &str) -> SqliteResult<()> {
        if self.config.read_only(Some(bucket)) {
            return Err(SqliteError::BucketReadOnly);
        }
        Ok(())
    }

    /// Validates that stored multipart state belongs to a request access key.
    pub(crate) fn try_validate_multipart_access(
        stored_access_key: Option<&String>,
        request_access_key: Option<&str>,
    ) -> SqliteResult<()> {
        if stored_access_key.map(String::as_str) == request_access_key {
            Ok(())
        } else {
            Err(SqliteError::MultipartAccessDenied)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                cache_control: None,
                expires: None,
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
                cache_control: None,
                expires: None,
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
            completed_upload_id,
            "bucket",
            "completed-key",
            None,
            "etag",
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
