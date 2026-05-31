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

const BUCKET_SCHEMA_VERSION: i32 = 1;
const NULL_VERSION_ID: &str = "null";
const VERSION_ID_LEN: usize = 31;
const VERSION_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";
const BUCKET_SCHEMA_SQL: &str = "
CREATE TABLE bucket_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    versioning_state TEXT NOT NULL CHECK(versioning_state IN ('never_configured', 'enabled', 'suspended'))
) STRICT;

CREATE TABLE object_version (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    is_latest INTEGER NOT NULL CHECK(is_latest IN (0, 1)),
    is_delete_marker INTEGER NOT NULL CHECK(is_delete_marker IN (0, 1)),
    body BLOB,
    size INTEGER NOT NULL CHECK(size >= 0),
    metadata TEXT,
    content_type TEXT,
    cache_control TEXT,
    expires TEXT,
    last_modified TEXT NOT NULL,
    md5 TEXT,
    UNIQUE(key, version_id),
    CHECK (
        (
            is_delete_marker = 0
            AND body IS NOT NULL
            AND md5 IS NOT NULL
        )
        OR
        (
            is_delete_marker = 1
            AND body IS NULL
            AND size = 0
            AND metadata IS NULL
            AND content_type IS NULL
            AND cache_control IS NULL
            AND expires IS NULL
            AND md5 IS NULL
        )
    )
) STRICT;

CREATE UNIQUE INDEX object_version_latest_one
    ON object_version(key)
    WHERE is_latest = 1;
CREATE INDEX object_version_current_list
    ON object_version(key)
    WHERE is_latest = 1 AND is_delete_marker = 0;
CREATE INDEX object_version_list
    ON object_version(key, sequence DESC);

CREATE TABLE completed_object_part (
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    start INTEGER NOT NULL CHECK(start >= 0),
    size INTEGER NOT NULL CHECK(size >= 0),
    md5 TEXT,
    PRIMARY KEY (key, version_id, part_number),
    FOREIGN KEY (key, version_id) REFERENCES object_version (key, version_id) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

CREATE TABLE multipart_upload (
    upload_id BLOB NOT NULL PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    initiated TEXT NOT NULL,
    last_modified TEXT NOT NULL,
    access_key TEXT,
    metadata TEXT,
    content_type TEXT,
    cache_control TEXT,
    expires TEXT,
    UNIQUE(upload_id, bucket, key)
) STRICT;

CREATE TABLE multipart_upload_part (
    upload_id BLOB NOT NULL,
    last_modified TEXT NOT NULL,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    value BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size >= 0),
    md5 TEXT,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

CREATE TABLE completed_multipart_upload (
    upload_id BLOB NOT NULL PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    access_key TEXT,
    md5 TEXT NOT NULL,
    version_id TEXT NOT NULL,
    returned_version_id TEXT,
    last_modified TEXT NOT NULL,
    UNIQUE(upload_id, bucket, key)
) STRICT;

INSERT INTO bucket_state (id, versioning_state)
VALUES (1, 'never_configured');

PRAGMA user_version = 1;
";

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
    /// A requested object version does not exist.
    #[error("object version does not exist")]
    ObjectVersionNotFound,
    /// A requested object byte range is not satisfiable.
    #[error("object range is invalid")]
    InvalidObjectRange,
    /// Object write preconditions did not match current stored metadata.
    #[error("object precondition failed")]
    PreconditionFailed,
    /// Storage configuration is invalid.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// A bucket database schema is not supported by this binary.
    #[error("unsupported bucket schema version {found}; expected {expected}")]
    UnsupportedSchemaVersion {
        /// Version found in `PRAGMA user_version`.
        found: i32,
        /// Version expected by this binary.
        expected: i32,
    },
    /// Random bytes could not be generated for an opaque version ID.
    #[error("random version id generation failed: {0}")]
    Random(String),
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

fn row_bool(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<bool> {
    Ok(row.get::<_, i32>(idx)? != 0)
}

fn generate_version_id() -> SqliteResult<String> {
    let mut bytes = [0; VERSION_ID_LEN];
    getrandom::fill(&mut bytes).map_err(|err| SqliteError::Random(err.to_string()))?;

    let mut version_id = String::with_capacity(VERSION_ID_LEN);
    for byte in bytes {
        version_id.push(char::from(VERSION_ID_ALPHABET[usize::from(byte & 0x3f)]));
    }

    if version_id == NULL_VERSION_ID {
        generate_version_id()
    } else {
        Ok(version_id)
    }
}

/// SQLite-backed storage state and open bucket connections.
#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
}

/// Bucket-level S3 object versioning state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BucketVersioningState {
    /// Versioning has never been configured on this bucket.
    NeverConfigured,
    /// New object writes create generated version IDs.
    Enabled,
    /// New object writes replace the special `null` version.
    Suspended,
}

impl BucketVersioningState {
    /// Returns the serialized database representation.
    #[must_use]
    fn as_sql(self) -> &'static str {
        match self {
            Self::NeverConfigured => "never_configured",
            Self::Enabled => "enabled",
            Self::Suspended => "suspended",
        }
    }

    /// Parses a database representation.
    fn from_sql(value: &str) -> rusqlite::Result<Self> {
        match value {
            "never_configured" => Ok(Self::NeverConfigured),
            "enabled" => Ok(Self::Enabled),
            "suspended" => Ok(Self::Suspended),
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }

    /// Returns whether write responses expose the stored version ID.
    #[must_use]
    fn returns_write_version_id(self) -> bool {
        self == Self::Enabled
    }
}

/// Object body and metadata loaded from the bucket database.
#[derive(Debug)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) version_id: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) key_metadata: KeyMetadata,
}

/// Minimal object metadata returned by list queries.
#[derive(Debug)]
pub(crate) struct ObjectSummary {
    pub(crate) key: String,
    pub(crate) version_id: String,
    pub(crate) size: i64,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
    pub(crate) is_latest: bool,
    pub(crate) is_delete_marker: bool,
}

/// Object metadata loaded without the object body.
#[derive(Debug)]
pub(crate) struct KeyMetadata {
    pub(crate) version_id: String,
    pub(crate) size: i64,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) content_type: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
    pub(crate) is_latest: bool,
    pub(crate) is_delete_marker: bool,
}

/// Result of storing an object version.
#[derive(Debug)]
pub(crate) struct ObjectWriteOutcome {
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) md5: Option<String>,
    pub(crate) last_modified: OffsetDateTime,
}

/// Result of deleting an object or object version.
#[derive(Debug)]
pub(crate) struct ObjectDeleteOutcome {
    pub(crate) version_id: Option<String>,
    pub(crate) is_delete_marker: bool,
    pub(crate) delete_marker_version_id: Option<String>,
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
    pub(crate) md5: Option<String>,
}

/// Stored part boundary for a completed multipart object.
#[derive(Debug)]
pub(crate) struct CompletedObjectPart {
    pub(crate) part_number: i32,
    pub(crate) start: i64,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
}

/// Client-requested multipart part checksum used during completion.
#[derive(Debug)]
pub(crate) struct MultipartCompletionPart {
    pub(crate) part_number: i32,
    pub(crate) md5: String,
}

/// ETag-like object identity condition evaluated by the storage layer.
#[derive(Clone, Debug)]
pub(crate) enum ObjectTagCondition {
    Any,
    Md5(String),
    Never,
}

/// Preconditions that must hold before replacing an object.
#[derive(Clone, Debug, Default)]
pub(crate) struct ObjectWriteConditions {
    pub(crate) if_match: Option<ObjectTagCondition>,
    pub(crate) if_none_match: Option<ObjectTagCondition>,
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

/// Active multipart upload row returned by multipart upload listing.
#[derive(Debug)]
pub(crate) struct MultipartUploadSummary {
    pub(crate) key: String,
    pub(crate) upload_id: Uuid,
    pub(crate) initiated: OffsetDateTime,
}

/// Completion ledger entry used to make multipart completion idempotent.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) md5: String,
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
}

/// Values inserted into the multipart completion ledger.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadRecord<'a> {
    pub(crate) upload_id: Uuid,
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) access_key: Option<String>,
    pub(crate) md5: &'a str,
    pub(crate) version_id: &'a str,
    pub(crate) returned_version_id: Option<&'a str>,
}

/// Result of materializing multipart upload parts into an object version.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadOutcome {
    pub(crate) md5: String,
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
}

/// Buffered completed multipart object and its persisted part boundaries.
#[derive(Debug)]
struct CompletedMultipartObject {
    value: Vec<u8>,
    size: i64,
    md5: String,
    parts: Vec<CompletedObjectPart>,
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
        write_conditions: ObjectWriteConditions,
    ) -> SqliteResult<CompletedMultipartUploadOutcome> {
        self.validate_mutable_bucket(&bucket)?;

        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let outcome = Self::try_complete_multipart_upload_transaction(
                    &transaction,
                    &bucket,
                    &key,
                    upload_id,
                    &requested_parts,
                    access_key,
                    &write_conditions,
                )?;
                transaction.commit()?;
                Ok(outcome)
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
        write_conditions: &ObjectWriteConditions,
    ) -> SqliteResult<CompletedMultipartUploadOutcome> {
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
            return Ok(CompletedMultipartUploadOutcome {
                md5: completed.md5,
                version_id: completed.version_id,
                returned_version_id: completed.returned_version_id,
            });
        };
        Self::try_validate_multipart_access(
            upload_state.access_key.as_ref(),
            access_key.as_deref(),
        )?;

        let completed_object =
            Self::try_materialize_multipart_object(transaction, upload_id, requested_parts)?;

        Self::try_check_object_write_conditions(transaction, key, write_conditions)?;
        let write_outcome = Self::try_put_object(
            transaction,
            KeyValue {
                key: key.to_string(),
                version_id: String::new(),
                value: Some(completed_object.value),
                key_metadata: KeyMetadata {
                    version_id: String::new(),
                    size: completed_object.size,
                    metadata: upload_state.metadata,
                    content_type: upload_state.content_type,
                    cache_control: upload_state.cache_control,
                    expires: upload_state.expires,
                    last_modified: OffsetDateTime::now_utc(),
                    md5: Some(completed_object.md5.clone()),
                    is_latest: true,
                    is_delete_marker: false,
                },
            },
        )?;
        Self::try_put_completed_object_parts(
            transaction,
            key,
            &write_outcome.version_id,
            &completed_object.parts,
        )?;

        Self::try_delete_multipart(transaction, upload_id)?;
        Self::try_put_completed_multipart_upload(
            transaction,
            CompletedMultipartUploadRecord {
                upload_id,
                bucket,
                key,
                access_key,
                md5: &completed_object.md5,
                version_id: &write_outcome.version_id,
                returned_version_id: write_outcome.returned_version_id.as_deref(),
            },
        )?;

        Ok(CompletedMultipartUploadOutcome {
            md5: completed_object.md5,
            version_id: write_outcome.version_id,
            returned_version_id: write_outcome.returned_version_id,
        })
    }

    /// Validates requested multipart parts and concatenates their bytes.
    fn try_materialize_multipart_object(
        transaction: &Transaction,
        upload_id: Uuid,
        requested_parts: &[MultipartCompletionPart],
    ) -> SqliteResult<CompletedMultipartObject> {
        let parts = Self::try_get_multiparts(transaction, upload_id)?;
        if parts.len() != requested_parts.len() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let mut value = Vec::with_capacity(Self::try_total_multipart_size(&parts)?);
        let mut completed_parts = Vec::with_capacity(parts.len());
        let mut part_md5s = Vec::with_capacity(parts.len() * 16);
        let final_part_number = requested_parts.len();
        let mut offset = 0;
        for (index, (requested_part, part)) in requested_parts.iter().zip(parts.iter()).enumerate()
        {
            Self::try_validate_completion_part(index, final_part_number, requested_part, part)?;
            let stored_md5 = part
                .md5
                .as_deref()
                .ok_or(SqliteError::InvalidMultipartPart)?;
            part_md5s.extend_from_slice(&decode_md5_hex(stored_md5)?);
            completed_parts.push(CompletedObjectPart {
                part_number: part.part_number,
                start: offset,
                size: part.size,
                md5: Some(stored_md5.to_string()),
            });
            offset = offset
                .checked_add(part.size)
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
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
        Ok(CompletedMultipartObject {
            value,
            size,
            md5,
            parts: completed_parts,
        })
    }

    /// Returns the total body size for uploaded multipart parts.
    fn try_total_multipart_size(parts: &[Multipart]) -> SqliteResult<usize> {
        parts.iter().try_fold(0usize, |total, part| {
            let part_size =
                usize::try_from(part.size).map_err(|_| SqliteError::InvalidMultipartPart)?;
            total
                .checked_add(part_size)
                .ok_or(SqliteError::MultipartObjectTooLarge)
        })
    }

    /// Validates one requested completion part against the stored part row.
    fn try_validate_completion_part(
        index: usize,
        final_part_number: usize,
        requested_part: &MultipartCompletionPart,
        part: &Multipart,
    ) -> SqliteResult<()> {
        if part.part_number != requested_part.part_number {
            return Err(SqliteError::InvalidMultipartPart);
        }
        if index + 1 != final_part_number && part.size < 5 * 1024 * 1024 {
            return Err(SqliteError::MultipartPartTooSmall);
        }
        if part.md5.as_deref() != Some(requested_part.md5.as_str()) {
            return Err(SqliteError::InvalidMultipartPart);
        }
        Ok(())
    }

    /// Checks object write preconditions against current metadata in the transaction.
    pub(crate) fn try_check_object_write_conditions(
        transaction: &Transaction,
        key: &str,
        conditions: &ObjectWriteConditions,
    ) -> SqliteResult<()> {
        if conditions.if_match.is_none() && conditions.if_none_match.is_none() {
            return Ok(());
        }

        let current = Self::try_get_metadata(transaction, key)?;
        // S3 reports a missing object before evaluating any If-Match tag.
        if current.is_none() && conditions.if_match.is_some() {
            return Err(SqliteError::ObjectNotFound);
        }

        let current_md5 = current
            .as_ref()
            .and_then(|metadata| metadata.md5.as_deref());

        if let Some(condition) = conditions.if_match.as_ref()
            && Self::object_tag_condition_matches(condition, current_md5).not()
        {
            return Err(SqliteError::PreconditionFailed);
        }

        if let Some(condition) = conditions.if_none_match.as_ref()
            && Self::object_tag_condition_matches(condition, current_md5)
        {
            return Err(SqliteError::PreconditionFailed);
        }

        Ok(())
    }

    /// Returns whether stored object metadata satisfies an object tag condition.
    #[must_use]
    fn object_tag_condition_matches(
        condition: &ObjectTagCondition,
        current_md5: Option<&str>,
    ) -> bool {
        match condition {
            ObjectTagCondition::Any => current_md5.is_some(),
            ObjectTagCondition::Md5(md5) => current_md5 == Some(md5.as_str()),
            ObjectTagCondition::Never => false,
        }
    }

    /// Reads the bucket schema version from `PRAGMA user_version`.
    fn try_schema_version(transaction: &Transaction) -> SqliteResult<i32> {
        let mut stmt = transaction.prepare_cached("PRAGMA user_version;")?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Counts user-created schema objects in a bucket database.
    fn try_user_table_count(transaction: &Transaction) -> SqliteResult<i64> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT COUNT(*)
            FROM sqlite_schema
            WHERE type = 'table'
                AND name NOT LIKE 'sqlite_%';",
        )?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Ensures the bucket database schema exists at the supported version.
    pub(crate) fn try_create_tables(transaction: &Transaction) -> SqliteResult<()> {
        let version = Self::try_schema_version(transaction)?;
        if version == BUCKET_SCHEMA_VERSION {
            return Ok(());
        }

        if version != 0 || Self::try_user_table_count(transaction)? > 0 {
            return Err(SqliteError::UnsupportedSchemaVersion {
                found: version,
                expected: BUCKET_SCHEMA_VERSION,
            });
        }

        transaction.execute_batch(BUCKET_SCHEMA_SQL)?;

        Ok(())
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
                version_id: row.get(1)?,
                size: row_size(row, 2)?,
                last_modified: row.get(3)?,
                md5: row.get(4)?,
                is_latest: true,
                is_delete_marker: false,
            })
        };

        let objects = match (prefix, after_key) {
            (Some(prefix), Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 AND key >= ?1 AND key > ?2 ORDER BY key LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 AND key >= ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((prefix, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 AND key > ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 ORDER BY key LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(objects)
    }

    /// Lists object versions ordered by key and newest version first.
    pub(crate) fn try_list_object_versions(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        after_version_id: Option<&str>,
        limit: i64,
    ) -> SqliteResult<Vec<ObjectSummary>> {
        let limit = sql_limit(limit)?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let after_sequence = match (after_key, after_version_id) {
            (Some(key), Some(version_id)) => Some(Self::try_get_version_sequence(
                transaction,
                key,
                version_id,
            )?),
            _ => None,
        };
        let map_summary = |row: &rusqlite::Row<'_>| {
            Ok(ObjectSummary {
                key: row.get(0)?,
                version_id: row.get(1)?,
                size: row_size(row, 2)?,
                last_modified: row.get(3)?,
                md5: row.get(4)?,
                is_latest: row_bool(row, 5)?,
                is_delete_marker: row_bool(row, 6)?,
            })
        };

        let versions = match (prefix, after_key, after_sequence) {
            (Some(prefix), Some(after_key), Some(after_sequence)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 AND (key > ?2 OR (key = ?2 AND sequence < ?3)) ORDER BY key, sequence DESC LIMIT ?4;",
                )?;
                stmt.query_map((prefix, after_key, after_sequence, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 AND key > ?2 ORDER BY key, sequence DESC LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 ORDER BY key, sequence DESC LIMIT ?2;",
                )?;
                stmt.query_map((prefix, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), Some(after_sequence)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key > ?1 OR (key = ?1 AND sequence < ?2) ORDER BY key, sequence DESC LIMIT ?3;",
                )?;
                stmt.query_map((after_key, after_sequence, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key > ?1 ORDER BY key, sequence DESC LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version ORDER BY key, sequence DESC LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(versions)
    }

    /// Returns whether the bucket contains any objects.
    pub(crate) fn try_has_objects(transaction: &Transaction) -> SqliteResult<bool> {
        let mut stmt =
            transaction.prepare_cached("SELECT EXISTS(SELECT 1 FROM object_version);")?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Fetches the current visible object body and metadata by key.
    pub(crate) fn try_get_object(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyValue>> {
        Self::try_get_object_internal(transaction, key, None, false)
    }

    /// Fetches an exact visible object body and metadata by key and version ID.
    pub(crate) fn try_get_object_version(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<KeyValue>> {
        Self::try_get_object_internal(transaction, key, Some(version_id), false)
    }

    /// Fetches an object body and metadata by current or exact version selector.
    fn try_get_object_internal(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
        include_delete_markers: bool,
    ) -> SqliteResult<Option<KeyValue>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                key,
                version_id,
                body,
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5,
                is_latest,
                is_delete_marker
            FROM object_version
            WHERE key = ?1
                AND (?2 IS NULL OR version_id = ?2)
                AND (?2 IS NOT NULL OR is_latest = 1)
                AND (?3 OR is_delete_marker = 0);",
        )?;

        Ok(stmt
            .query_row((key, version_id, include_delete_markers), |row| {
                Ok(KeyValue {
                    key: row.get(0)?,
                    version_id: row.get(1)?,
                    value: row.get(2)?,
                    key_metadata: KeyMetadata {
                        version_id: row.get(1)?,
                        size: row_size(row, 3)?,
                        metadata: row
                            .get::<_, Option<String>>(4)?
                            .map(|metadata| serde_json::from_str(&metadata))
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    4,
                                    rusqlite::types::Type::Text,
                                    Box::new(err),
                                )
                            })?,
                        content_type: row.get(5)?,
                        cache_control: row.get(6)?,
                        expires: row.get(7)?,
                        last_modified: row.get(8)?,
                        md5: row.get(9)?,
                        is_latest: row_bool(row, 10)?,
                        is_delete_marker: row_bool(row, 11)?,
                    },
                })
            })
            .optional()?)
    }

    /// Fetches current visible object metadata by key without reading the body.
    pub(crate) fn try_get_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        Self::try_get_metadata_internal(transaction, key, None, false)
    }

    /// Fetches the newest non-delete-marker metadata for conditional deletes.
    pub(crate) fn try_get_latest_object_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                version_id,
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5,
                is_latest,
                is_delete_marker
            FROM object_version
            WHERE key = ?1 AND is_delete_marker = 0
            ORDER BY sequence DESC
            LIMIT 1;",
        )?;

        Ok(stmt
            .query_row([key], |row| {
                Ok(KeyMetadata {
                    version_id: row.get(0)?,
                    size: row_size(row, 1)?,
                    metadata: row
                        .get::<_, Option<String>>(2)?
                        .map(|metadata| serde_json::from_str(&metadata))
                        .transpose()
                        .map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    content_type: row.get(3)?,
                    cache_control: row.get(4)?,
                    expires: row.get(5)?,
                    last_modified: row.get(6)?,
                    md5: row.get(7)?,
                    is_latest: row_bool(row, 8)?,
                    is_delete_marker: row_bool(row, 9)?,
                })
            })
            .optional()?)
    }

    /// Fetches exact object metadata, including delete markers, by key and version ID.
    pub(crate) fn try_get_version_metadata(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        Self::try_get_metadata_internal(transaction, key, Some(version_id), true)
    }

    /// Fetches metadata by current or exact version selector.
    fn try_get_metadata_internal(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
        include_delete_markers: bool,
    ) -> SqliteResult<Option<KeyMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                version_id,
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5,
                is_latest,
                is_delete_marker
            FROM object_version
            WHERE key = ?1
                AND (?2 IS NULL OR version_id = ?2)
                AND (?2 IS NOT NULL OR is_latest = 1)
                AND (?3 OR is_delete_marker = 0);",
        )?;

        Ok(stmt
            .query_row((key, version_id, include_delete_markers), |row| {
                Ok(KeyMetadata {
                    version_id: row.get(0)?,
                    size: row_size(row, 1)?,
                    metadata: row
                        .get::<_, Option<String>>(2)?
                        .map(|metadata| serde_json::from_str(&metadata))
                        .transpose()
                        .map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    content_type: row.get(3)?,
                    cache_control: row.get(4)?,
                    expires: row.get(5)?,
                    last_modified: row.get(6)?,
                    md5: row.get(7)?,
                    is_latest: row_bool(row, 8)?,
                    is_delete_marker: row_bool(row, 9)?,
                })
            })
            .optional()?)
    }

    /// Inserts an object version or replaces the `null` version.
    pub(crate) fn try_put_object(
        transaction: &Transaction,
        kv: KeyValue,
    ) -> SqliteResult<ObjectWriteOutcome> {
        let KeyValue {
            key,
            value,
            key_metadata,
            ..
        } = kv;
        let state = Self::try_get_bucket_versioning_state(transaction)?;
        let version_id = match state {
            BucketVersioningState::Enabled => {
                Self::try_generate_unique_version_id(transaction, &key)?
            }
            BucketVersioningState::NeverConfigured | BucketVersioningState::Suspended => {
                NULL_VERSION_ID.to_string()
            }
        };
        let returned_version_id = state.returns_write_version_id().then(|| version_id.clone());

        if state.returns_write_version_id().not() {
            Self::try_delete_version_row(transaction, &key, NULL_VERSION_ID)?;
        }
        Self::try_clear_latest(transaction, &key)?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_version (
                key,
                version_id,
                is_latest,
                is_delete_marker,
                body,
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5
            )
            VALUES (?1, ?2, 1, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);",
        )?;

        let size = sql_size(key_metadata.size)?;
        stmt.execute((
            &key,
            &version_id,
            value,
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
            key_metadata.md5.as_deref(),
        ))?;

        Ok(ObjectWriteOutcome {
            version_id,
            returned_version_id,
            md5: key_metadata.md5,
            last_modified: key_metadata.last_modified,
        })
    }

    /// Deletes an object by key or permanently deletes an exact version.
    pub(crate) fn try_delete_object(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
    ) -> SqliteResult<ObjectDeleteOutcome> {
        if let Some(version_id) = version_id {
            let deleted = Self::try_get_version_metadata(transaction, key, version_id)?;
            let is_latest = deleted.as_ref().is_some_and(|metadata| metadata.is_latest);
            let is_delete_marker = deleted
                .as_ref()
                .is_some_and(|metadata| metadata.is_delete_marker);
            Self::try_delete_version_row(transaction, key, version_id)?;
            if is_latest {
                Self::try_promote_latest_version(transaction, key)?;
            }
            return Ok(ObjectDeleteOutcome {
                version_id: Some(version_id.to_string()),
                is_delete_marker,
                delete_marker_version_id: is_delete_marker.then(|| version_id.to_string()),
            });
        }

        let state = Self::try_get_bucket_versioning_state(transaction)?;
        match state {
            BucketVersioningState::NeverConfigured => {
                Self::try_delete_version_row(transaction, key, NULL_VERSION_ID)?;
                Ok(ObjectDeleteOutcome {
                    version_id: None,
                    is_delete_marker: false,
                    delete_marker_version_id: None,
                })
            }
            BucketVersioningState::Enabled => {
                let version_id = Self::try_generate_unique_version_id(transaction, key)?;
                Self::try_insert_delete_marker(transaction, key, &version_id)?;
                Ok(ObjectDeleteOutcome {
                    version_id: Some(version_id.clone()),
                    is_delete_marker: true,
                    delete_marker_version_id: Some(version_id),
                })
            }
            BucketVersioningState::Suspended => {
                Self::try_delete_version_row(transaction, key, NULL_VERSION_ID)?;
                Self::try_insert_delete_marker(transaction, key, NULL_VERSION_ID)?;
                Ok(ObjectDeleteOutcome {
                    version_id: Some(NULL_VERSION_ID.to_string()),
                    is_delete_marker: true,
                    delete_marker_version_id: Some(NULL_VERSION_ID.to_string()),
                })
            }
        }
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

    /// Returns the storage sequence for an exact object version.
    fn try_get_version_sequence(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<i64> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT sequence
            FROM object_version
            WHERE key = ?1 AND version_id = ?2;",
        )?;
        stmt.query_row((key, version_id), |row| row.get(0))
            .optional()?
            .ok_or(SqliteError::ObjectVersionNotFound)
    }

    /// Generates an unused opaque version ID for a key.
    fn try_generate_unique_version_id(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<String> {
        loop {
            let version_id = generate_version_id()?;
            if Self::try_version_exists(transaction, key, &version_id)?.not() {
                return Ok(version_id);
            }
        }
    }

    /// Returns whether an exact object version row exists.
    fn try_version_exists(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<bool> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT EXISTS(
                SELECT 1
                FROM object_version
                WHERE key = ?1 AND version_id = ?2
            );",
        )?;
        Ok(stmt.query_row((key, version_id), |row| row.get(0))?)
    }

    /// Clears the latest pointer for all versions of a key.
    fn try_clear_latest(transaction: &Transaction, key: &str) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE object_version
            SET is_latest = 0
            WHERE key = ?1;",
        )?;
        Ok(stmt.execute([key])?)
    }

    /// Deletes one exact object version row.
    fn try_delete_version_row(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM object_version
            WHERE key = ?1 AND version_id = ?2;",
        )?;
        Ok(stmt.execute((key, version_id))?)
    }

    /// Promotes the newest remaining version for a key to latest.
    fn try_promote_latest_version(transaction: &Transaction, key: &str) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE object_version
            SET is_latest = 1
            WHERE sequence = (
                SELECT sequence
                FROM object_version
                WHERE key = ?1
                ORDER BY sequence DESC
                LIMIT 1
            );",
        )?;
        Ok(stmt.execute([key])?)
    }

    /// Creates a delete marker as the current latest version.
    fn try_insert_delete_marker(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<usize> {
        Self::try_clear_latest(transaction, key)?;
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_version (
                key,
                version_id,
                is_latest,
                is_delete_marker,
                body,
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5
            )
            VALUES (?1, ?2, 1, 1, NULL, 0, NULL, NULL, NULL, NULL, ?3, NULL);",
        )?;
        Ok(stmt.execute((key, version_id, OffsetDateTime::now_utc()))?)
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
            INSERT INTO multipart_upload (upload_id, initiated, last_modified, bucket, key, access_key, metadata, content_type, cache_control, expires)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);",
        )?;

        let now = OffsetDateTime::now_utc();
        Ok(stmt.execute((
            upload_id,
            now,
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
        record: CompletedMultipartUploadRecord<'_>,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO completed_multipart_upload (upload_id, bucket, key, access_key, md5, version_id, returned_version_id, last_modified)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(upload_id) DO UPDATE
            SET bucket=excluded.bucket, key=excluded.key, access_key=excluded.access_key, md5=excluded.md5, version_id=excluded.version_id, returned_version_id=excluded.returned_version_id, last_modified=excluded.last_modified;",
        )?;

        Ok(stmt.execute((
            record.upload_id,
            record.bucket,
            record.key,
            record.access_key,
            record.md5,
            record.version_id,
            record.returned_version_id,
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
            SELECT access_key, md5, version_id, returned_version_id
            FROM completed_multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        Ok(stmt
            .query_row((upload_id, bucket, key), |row| {
                Ok(CompletedMultipartUploadState {
                    access_key: row.get(0)?,
                    md5: row.get(1)?,
                    version_id: row.get(2)?,
                    returned_version_id: row.get(3)?,
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
        after_part_number: i32,
        limit: i64,
    ) -> SqliteResult<Vec<MultipartMetadata>> {
        let limit = sql_limit(limit)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                size,
                md5
            FROM multipart_upload_part
            WHERE upload_id = ?1
                AND part_number > ?2
            ORDER BY part_number
            LIMIT ?3;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map((upload_id, after_part_number, limit), |row| {
                Ok(MultipartMetadata {
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    size: row_size(row, 2)?,
                    md5: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Lists active multipart uploads ordered by key and upload ID.
    pub(crate) fn try_list_multipart_uploads(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        after_upload_id: Option<Uuid>,
        limit: i64,
    ) -> SqliteResult<Vec<MultipartUploadSummary>> {
        let limit = sql_limit(limit)?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let map_upload = |row: &rusqlite::Row<'_>| {
            Ok(MultipartUploadSummary {
                key: row.get(0)?,
                upload_id: row.get(1)?,
                initiated: row.get(2)?,
            })
        };

        let uploads = match (prefix, after_key, after_upload_id) {
            (Some(prefix), Some(after_key), Some(after_upload_id)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated FROM multipart_upload WHERE key >= ?1 AND (key > ?2 OR (key = ?2 AND upload_id > ?3)) ORDER BY key, upload_id LIMIT ?4;",
                )?;
                stmt.query_map(
                    rusqlite::params![prefix, after_key, after_upload_id, limit],
                    map_upload,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated FROM multipart_upload WHERE key >= ?1 AND key > ?2 ORDER BY key, upload_id LIMIT ?3;",
                )?;
                stmt.query_map(rusqlite::params![prefix, after_key, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated FROM multipart_upload WHERE key >= ?1 ORDER BY key, upload_id LIMIT ?2;",
                )?;
                stmt.query_map(rusqlite::params![prefix, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), Some(after_upload_id)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated FROM multipart_upload WHERE key > ?1 OR (key = ?1 AND upload_id > ?2) ORDER BY key, upload_id LIMIT ?3;",
                )?;
                stmt.query_map(
                    rusqlite::params![after_key, after_upload_id, limit],
                    map_upload,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated FROM multipart_upload WHERE key > ?1 ORDER BY key, upload_id LIMIT ?2;",
                )?;
                stmt.query_map(rusqlite::params![after_key, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated FROM multipart_upload ORDER BY key, upload_id LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(uploads)
    }

    /// Fetches one completed multipart object part boundary.
    pub(crate) fn try_get_completed_object_part(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        part_number: i32,
    ) -> SqliteResult<Option<CompletedObjectPart>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT part_number, start, size, md5
            FROM completed_object_part
            WHERE key = ?1 AND version_id = ?2 AND part_number = ?3;",
        )?;

        Ok(stmt
            .query_row((key, version_id, part_number), |row| {
                Ok(CompletedObjectPart {
                    part_number: row.get(0)?,
                    start: row_size(row, 1)?,
                    size: row_size(row, 2)?,
                    md5: row.get(3)?,
                })
            })
            .optional()?)
    }

    /// Counts recorded completed multipart object part boundaries.
    pub(crate) fn try_count_completed_object_parts(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<i32> {
        let mut stmt = transaction.prepare_cached(
            "SELECT COUNT(*) FROM completed_object_part WHERE key = ?1 AND version_id = ?2;",
        )?;
        let count = stmt.query_row((key, version_id), |row| row_size(row, 0))?;
        i32::try_from(count).map_err(|_| SqliteError::MultipartObjectTooLarge)
    }

    /// Inserts completed multipart object part boundaries.
    fn try_put_completed_object_parts(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO completed_object_part (key, version_id, part_number, start, size, md5)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
        )?;

        for part in parts {
            stmt.execute((
                key,
                version_id,
                part.part_number,
                sql_size(part.start)?,
                sql_size(part.size)?,
                &part.md5,
            ))?;
        }

        Ok(())
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
            CompletedMultipartUploadRecord {
                upload_id: completed_upload_id,
                bucket: "bucket",
                key: "completed-key",
                access_key: None,
                md5: "etag",
                version_id: NULL_VERSION_ID,
                returned_version_id: None,
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
