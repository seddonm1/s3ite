use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Not,
    path::PathBuf,
    sync::Arc,
};

use clap::ValueEnum;
use rusqlite::{
    Error::ToSqlConversionFailure,
    ToSql, Transaction,
    limits::Limit,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use time::{Duration, OffsetDateTime};
use tokio::sync::RwLock;
use uuid::Uuid;

pub(crate) use crate::checksum::{
    MultipartChecksumConfig, MultipartCompletionChecksum, PartChecksum, StoredChecksum,
};
use crate::database::BucketConnection;
use crate::sse::{ActiveSse, SSE_S3_ALGORITHM_CODE, StoredSse};

mod bucket;
mod checksum;
mod delete;
mod error;
mod get;
mod list;
mod multipart;
mod put;

use self::checksum::{
    decode_base64, decode_md5_hex, encode_hex, optional_multipart_checksum_config,
    optional_part_checksum, optional_stored_checksum, required_stored_checksum,
};
use self::error::{NegativeObjectSize, NegativeSqlLimit};
pub use self::error::{SqliteError, SqliteResult};

pub(crate) const NULL_VERSION_ID: &str = "null";

const BUCKET_SCHEMA_VERSION: i32 = 9;
pub(crate) const DEFAULT_OBJECT_BLOB_CHUNK_SIZE: usize = 128 * 1024 * 1024;
const BUCKET_SCHEMA_SQL: &str = "
-- Singleton bucket metadata, currently the object versioning state.
CREATE TABLE bucket_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    versioning_state INTEGER NOT NULL CHECK(versioning_state IN (0, 1, 2)),
    default_sse_algorithm INTEGER CHECK(default_sse_algorithm IN (1))
) STRICT;

-- Object metadata for every live version and delete marker.
CREATE TABLE object_version (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    is_latest INTEGER NOT NULL CHECK(is_latest IN (0, 1)),
    is_delete_marker INTEGER NOT NULL CHECK(is_delete_marker IN (0, 1)),
    size INTEGER NOT NULL CHECK(size >= 0),
    metadata TEXT,
    content_type TEXT,
    content_encoding TEXT,
    cache_control TEXT,
    expires TEXT,
    last_modified TEXT NOT NULL,
    md5 TEXT,
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    sse_algorithm INTEGER CHECK(sse_algorithm IN (1)),
    sse_key_nonce BLOB,
    sse_key_ciphertext BLOB,
    sse_key_tag BLOB,
    UNIQUE(key, version_id),
    CHECK (
        (
            sse_algorithm IS NULL
            AND sse_key_nonce IS NULL
            AND sse_key_ciphertext IS NULL
            AND sse_key_tag IS NULL
        )
        OR
        (
            sse_algorithm = 1
            AND length(sse_key_nonce) = 12
            AND length(sse_key_ciphertext) = 32
            AND length(sse_key_tag) = 16
        )
    ),
    CHECK (
        (
            is_delete_marker = 0
            AND md5 IS NOT NULL
            AND checksum_algorithm IS NOT NULL
            AND checksum_type IS NOT NULL
            AND checksum_value IS NOT NULL
        )
        OR
        (
            is_delete_marker = 1
            AND size = 0
            AND metadata IS NULL
            AND content_type IS NULL
            AND content_encoding IS NULL
            AND cache_control IS NULL
            AND expires IS NULL
            AND md5 IS NULL
            AND checksum_algorithm IS NULL
            AND checksum_type IS NULL
            AND checksum_value IS NULL
            AND sse_algorithm IS NULL
            AND sse_key_nonce IS NULL
            AND sse_key_ciphertext IS NULL
            AND sse_key_tag IS NULL
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

-- Active multipart upload metadata, ownership, and checksum configuration.
CREATE TABLE multipart_upload (
    upload_id BLOB NOT NULL PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    initiated TEXT NOT NULL,
    last_modified TEXT NOT NULL,
    access_key TEXT,
    metadata TEXT,
    content_type TEXT,
    content_encoding TEXT,
    cache_control TEXT,
    expires TEXT,
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    sse_algorithm INTEGER CHECK(sse_algorithm IN (1)),
    sse_key_nonce BLOB,
    sse_key_ciphertext BLOB,
    sse_key_tag BLOB,
    UNIQUE(upload_id, bucket, key),
    CHECK (
        (
            sse_algorithm IS NULL
            AND sse_key_nonce IS NULL
            AND sse_key_ciphertext IS NULL
            AND sse_key_tag IS NULL
        )
        OR
        (
            sse_algorithm = 1
            AND length(sse_key_nonce) = 12
            AND length(sse_key_ciphertext) = 32
            AND length(sse_key_tag) = 16
        )
    ),
    CHECK (
        (
            checksum_algorithm IS NULL
            AND checksum_type IS NULL
            AND checksum_value IS NULL
        )
        OR
        (
            checksum_algorithm IS NOT NULL
            AND checksum_type IS NOT NULL
            AND checksum_value IS NULL
        )
    )
) STRICT;

CREATE INDEX multipart_upload_list
    ON multipart_upload(key, upload_id);
CREATE INDEX multipart_upload_expiry
    ON multipart_upload(DATETIME(last_modified));

-- Logical storage part metadata for object bytes and S3 multipart boundaries.
CREATE TABLE object_part (
    object_sequence INTEGER,
    upload_id BLOB,
    is_multipart_part INTEGER NOT NULL CHECK(is_multipart_part IN (0, 1)),
    last_modified TEXT NOT NULL,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    start INTEGER CHECK(start >= 0),
    size INTEGER NOT NULL CHECK(size >= 0),
    md5 TEXT,
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    FOREIGN KEY (object_sequence) REFERENCES object_version (sequence) ON DELETE CASCADE,
    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE,
    CHECK (
        (
            object_sequence IS NOT NULL
            AND upload_id IS NULL
            AND start IS NOT NULL
        )
        OR
        (
            object_sequence IS NULL
            AND upload_id IS NOT NULL
            AND start IS NULL
        )
    ),
    CHECK (
        is_multipart_part = 1
        OR
        (
            object_sequence IS NOT NULL
            AND upload_id IS NULL
            AND part_number = 1
            AND start = 0
            AND md5 IS NULL
            AND checksum_algorithm IS NULL
            AND checksum_type IS NULL
            AND checksum_value IS NULL
        )
    ),
    CHECK (
        (
            checksum_algorithm IS NULL
            AND checksum_type IS NULL
            AND checksum_value IS NULL
        )
        OR
        (
            checksum_algorithm IS NOT NULL
            AND checksum_type IS NULL
            AND checksum_value IS NOT NULL
        )
    )
) STRICT;

CREATE UNIQUE INDEX object_part_object_part
    ON object_part(object_sequence, part_number);
CREATE UNIQUE INDEX object_part_upload_part
    ON object_part(upload_id, part_number);
CREATE INDEX object_part_object_start
    ON object_part(object_sequence, start)
    WHERE object_sequence IS NOT NULL;
CREATE INDEX object_part_completed_list
    ON object_part(object_sequence, is_multipart_part, part_number);

-- Physical object bytes split into bounded SQLite BLOB chunks.
CREATE TABLE object_blob_chunk (
    chunk_id INTEGER PRIMARY KEY,
    object_sequence INTEGER,
    upload_id BLOB,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    chunk_number INTEGER NOT NULL CHECK(chunk_number >= 1),
    offset INTEGER NOT NULL CHECK(offset >= 0),
    size INTEGER NOT NULL CHECK(size >= 0),
    body BLOB NOT NULL,
    sse_nonce BLOB,
    sse_tag BLOB,
    FOREIGN KEY (object_sequence) REFERENCES object_version (sequence) ON DELETE CASCADE,
    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE,
    FOREIGN KEY (object_sequence, part_number)
        REFERENCES object_part (object_sequence, part_number)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (upload_id, part_number)
        REFERENCES object_part (upload_id, part_number)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (
            object_sequence IS NOT NULL
            AND upload_id IS NULL
        )
        OR
        (
            object_sequence IS NULL
            AND upload_id IS NOT NULL
        )
    ),
    CHECK (
        (
            sse_nonce IS NULL
            AND sse_tag IS NULL
        )
        OR
        (
            length(sse_nonce) = 12
            AND length(sse_tag) = 16
        )
    ),
    CHECK (size = length(body))
) STRICT;

CREATE UNIQUE INDEX object_blob_chunk_object_order
    ON object_blob_chunk(object_sequence, part_number, chunk_number)
    WHERE object_sequence IS NOT NULL;
CREATE UNIQUE INDEX object_blob_chunk_upload_order
    ON object_blob_chunk(upload_id, part_number, chunk_number)
    WHERE upload_id IS NOT NULL;
CREATE INDEX object_blob_chunk_object_offset
    ON object_blob_chunk(object_sequence, offset)
    WHERE object_sequence IS NOT NULL;

-- Idempotency ledger for completed multipart upload retries.
CREATE TABLE completed_multipart_upload (
    upload_id BLOB NOT NULL PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    access_key TEXT,
    md5 TEXT NOT NULL,
    version_id TEXT NOT NULL,
    returned_version_id TEXT,
    checksum_algorithm INTEGER NOT NULL CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER NOT NULL CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT NOT NULL,
    last_modified TEXT NOT NULL,
    UNIQUE(upload_id, bucket, key)
) STRICT;

CREATE INDEX completed_multipart_upload_expiry
    ON completed_multipart_upload(DATETIME(last_modified));

INSERT INTO bucket_state (id, versioning_state)
VALUES (1, 0);

PRAGMA user_version = 9;
";

/// Reads a nonnegative object size column from a row.
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

/// Validates an object size before binding it to SQL.
fn sql_size(value: i64) -> rusqlite::Result<i64> {
    if value < 0 {
        return Err(ToSqlConversionFailure(Box::new(NegativeObjectSize)));
    }
    Ok(value)
}

/// Validates a SQL query limit before binding it.
fn sql_limit(value: i64) -> rusqlite::Result<i64> {
    if value < 0 {
        return Err(ToSqlConversionFailure(Box::new(NegativeSqlLimit)));
    }
    Ok(value)
}

/// Reads an integer row column as a `SQLite` boolean.
fn row_bool(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<bool> {
    Ok(row.get::<_, i32>(idx)? != 0)
}

/// Reads nullable SSE-S3 envelope metadata from contiguous row columns.
fn optional_stored_sse(
    algorithm: Option<i64>,
    key_nonce: Option<Vec<u8>>,
    key_ciphertext: Option<Vec<u8>>,
    key_tag: Option<Vec<u8>>,
) -> rusqlite::Result<Option<StoredSse>> {
    match (algorithm, key_nonce, key_ciphertext, key_tag) {
        (None, None, None, None) => Ok(None),
        (Some(algorithm), Some(key_nonce), Some(key_ciphertext), Some(key_tag))
            if algorithm == SSE_S3_ALGORITHM_CODE =>
        {
            Ok(Some(StoredSse {
                key_nonce,
                key_ciphertext,
                key_tag,
            }))
        }
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

/// Converts a configured TTL into an expiration cutoff timestamp.
fn expiration_threshold(ttl_seconds: u64) -> Option<OffsetDateTime> {
    if ttl_seconds == 0 {
        return None;
    }

    let seconds = i64::try_from(ttl_seconds).unwrap_or(i64::MAX);
    Some(OffsetDateTime::now_utc().saturating_sub(Duration::seconds(seconds)))
}

/// Renders the `SQLite` PRAGMA statements for a writable bucket connection.
///
/// `SQLite` interprets negative `cache_size` values as kibibytes, so the
/// configured positive KiB value is emitted as `PRAGMA cache_size=-N`.
pub(crate) fn writer_connection_pragmas(config: &crate::Config, bucket: Option<&str>) -> String {
    format!(
        "
        PRAGMA journal_mode={};
        PRAGMA synchronous={};
        PRAGMA temp_store={};
        PRAGMA cache_size=-{};
        PRAGMA busy_timeout={};
        PRAGMA journal_size_limit={};
        PRAGMA query_only={};
        PRAGMA foreign_keys=true;
        PRAGMA trusted_schema=false;
        PRAGMA auto_vacuum=INCREMENTAL;
        ",
        pragma_value(&config.journal_mode(bucket)),
        pragma_value(&config.synchronous(bucket)),
        pragma_value(&config.temp_store(bucket)),
        config.cache_size(bucket),
        config.busy_timeout_milliseconds(bucket),
        config.journal_size_limit(bucket),
        config.read_only(bucket),
    )
}

/// Renders the `SQLite` PRAGMA statements for read-only bucket connections.
///
/// These pragmas are connection-local and safe for read-only metadata and BLOB
/// readers. Write-oriented settings such as `journal_mode` are applied by the
/// writer connection only.
pub(crate) fn reader_connection_pragmas(config: &crate::Config, bucket: Option<&str>) -> String {
    format!(
        "
        PRAGMA temp_store={};
        PRAGMA cache_size=-{};
        PRAGMA busy_timeout={};
        PRAGMA foreign_keys=true;
        PRAGMA trusted_schema=false;
        ",
        pragma_value(&config.temp_store(bucket)),
        config.cache_size(bucket),
        config.busy_timeout_milliseconds(bucket),
    )
}

/// Returns the configured command/config spelling for a `ValueEnum` value.
fn pragma_value<T: ValueEnum>(value: &T) -> String {
    let Some(possible) = value.to_possible_value() else {
        unreachable!("configuration enum variants must have clap values");
    };
    possible.get_name().to_string()
}

/// Bucket-level S3 object versioning state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub(crate) enum BucketVersioningState {
    /// Versioning has never been configured on this bucket.
    NeverConfigured = 0,
    /// New object writes create generated version IDs.
    Enabled = 1,
    /// New object writes replace the special `null` version.
    Suspended = 2,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid bucket versioning state: {0}")]
struct InvalidBucketVersioningState(i64);

impl BucketVersioningState {
    /// Returns the serialized `SQLite` representation.
    #[must_use]
    fn sql_code(self) -> i64 {
        self as i64
    }

    /// Parses a `SQLite` representation.
    fn from_sql_code(value: i64) -> FromSqlResult<Self> {
        match value {
            value if value == Self::NeverConfigured.sql_code() => Ok(Self::NeverConfigured),
            value if value == Self::Enabled.sql_code() => Ok(Self::Enabled),
            value if value == Self::Suspended.sql_code() => Ok(Self::Suspended),
            _ => Err(FromSqlError::Other(Box::new(InvalidBucketVersioningState(
                value,
            )))),
        }
    }

    /// Returns whether write responses expose the stored version ID.
    #[must_use]
    fn returns_write_version_id(self) -> bool {
        self == Self::Enabled
    }
}

impl ToSql for BucketVersioningState {
    /// Serializes a bucket versioning state as its stable database code.
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.sql_code()))
    }
}

impl FromSql for BucketVersioningState {
    /// Deserializes a bucket versioning state from its database code.
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let ValueRef::Integer(value) = value else {
            return Err(FromSqlError::InvalidType);
        };
        Self::from_sql_code(value)
    }
}

/// Object body and metadata loaded from the bucket database.
#[derive(Debug)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) key_metadata: KeyMetadata,
    pub(crate) encryption: Option<ActiveSse>,
}

/// Pre-encoded object blob chunk ready to insert into storage.
#[derive(Debug)]
pub(crate) struct EncodedBlobChunk {
    pub(crate) chunk_number: i32,
    pub(crate) offset: i64,
    pub(crate) size: i64,
    pub(crate) body: Vec<u8>,
    pub(crate) sse_nonce: Option<Vec<u8>>,
    pub(crate) sse_tag: Option<Vec<u8>>,
}

/// Metadata plus rowid needed to stream an object body from `SQLite`.
#[derive(Debug)]
pub(crate) struct ObjectBodyReference {
    pub(crate) version_id: String,
    pub(crate) sequence: i64,
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
    pub(crate) content_encoding: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) checksum: Option<MultipartChecksumConfig>,
    pub(crate) encryption: Option<StoredSse>,
}

/// Active multipart upload row returned by multipart upload listing.
#[derive(Debug)]
pub(crate) struct MultipartUploadSummary {
    pub(crate) key: String,
    pub(crate) upload_id: Uuid,
    pub(crate) initiated: OffsetDateTime,
    pub(crate) checksum: Option<MultipartChecksumConfig>,
}

/// Completion ledger entry used to make multipart completion idempotent.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) md5: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) checksum: StoredChecksum,
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
    pub(crate) checksum: &'a StoredChecksum,
}

/// Result of materializing multipart upload parts into an object version.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadOutcome {
    pub(crate) md5: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) checksum: StoredChecksum,
    pub(crate) encryption: Option<StoredSse>,
}

/// Read-side multipart completion state.
#[derive(Debug)]
pub(crate) enum MultipartCompletion {
    AlreadyCompleted(CompletedMultipartUploadOutcome),
    Prepared(PreparedMultipartUpload),
}

/// Buffered completed multipart object and its persisted part boundaries.
#[derive(Debug)]
pub(crate) struct PreparedMultipartUpload {
    size: i64,
    md5: String,
    checksum: StoredChecksum,
    upload_checksum: Option<MultipartChecksumConfig>,
    encryption: Option<StoredSse>,
    parts: Vec<CompletedObjectPart>,
}

/// Object metadata loaded without the object body.
#[derive(Debug)]
pub(crate) struct KeyMetadata {
    pub(crate) version_id: String,
    pub(crate) size: i64,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) content_type: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: Option<StoredChecksum>,
    pub(crate) encryption: Option<StoredSse>,
    pub(crate) is_latest: bool,
    pub(crate) is_delete_marker: bool,
}

/// Result of storing an object version.
#[derive(Debug)]
pub(crate) struct ObjectWriteOutcome {
    pub(crate) sequence: i64,
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
    pub(crate) checksum: Option<PartChecksum>,
    pub(crate) encryption: Option<ActiveSse>,
}

/// Metadata for a multipart part whose chunks are already encoded.
#[derive(Debug)]
pub(crate) struct MultipartPartWrite {
    pub(crate) upload_id: Uuid,
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: Option<PartChecksum>,
}

/// Multipart upload part metadata returned by list-parts queries.
#[derive(Debug)]
pub(crate) struct MultipartMetadata {
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: Option<PartChecksum>,
}

/// Stored part boundary for a completed multipart object.
#[derive(Debug)]
pub(crate) struct CompletedObjectPart {
    pub(crate) part_number: i32,
    pub(crate) start: i64,
    pub(crate) size: i64,
    pub(crate) checksum: Option<PartChecksum>,
}

/// Client-requested multipart part checksum used during completion.
#[derive(Debug)]
pub(crate) struct MultipartCompletionPart {
    pub(crate) part_number: i32,
    pub(crate) md5: String,
    pub(crate) checksum: Option<PartChecksum>,
}

/// Request data needed to complete a multipart upload transactionally.
#[derive(Debug)]
pub(crate) struct MultipartCompletionRequest {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) upload_id: Uuid,
    pub(crate) requested_parts: Vec<MultipartCompletionPart>,
    pub(crate) requested_checksum: MultipartCompletionChecksum,
    pub(crate) default_checksum: MultipartChecksumConfig,
    pub(crate) access_key: Option<String>,
    pub(crate) write_conditions: ObjectWriteConditions,
    pub(crate) sse_s3_master_key: Option<[u8; 32]>,
}

/// SQLite-backed storage state and open bucket connections.
#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
    pub(crate) blob_chunk_size: usize,
}

impl Sqlite {
    /// Reads `SQLite`'s runtime limit for a single string, BLOB, or row value.
    fn sqlite_length_limit() -> SqliteResult<usize> {
        let connection = rusqlite::Connection::open_in_memory()?;
        let limit = connection.limit(Limit::SQLITE_LIMIT_LENGTH)?;
        usize::try_from(limit).map_err(|_| {
            SqliteError::Configuration("SQLite length limit does not fit usize".to_string())
        })
    }

    /// Returns the storage chunk size used for bounded `SQLite` BLOB rows.
    pub(crate) fn blob_chunk_size(&self) -> usize {
        self.blob_chunk_size
    }

    /// Chooses a safe storage chunk size for the current `SQLite` build.
    fn default_blob_chunk_size() -> SqliteResult<usize> {
        let sqlite_limit = Self::sqlite_length_limit()?;
        let chunk_size = DEFAULT_OBJECT_BLOB_CHUNK_SIZE.min(sqlite_limit / 2);
        if chunk_size == 0 {
            return Err(SqliteError::Configuration(
                "SQLite length limit is too small for object storage".to_string(),
            ));
        }
        Ok(chunk_size)
    }

    /// Reads the bucket schema version from `PRAGMA user_version`.
    fn schema_version(transaction: &Transaction) -> SqliteResult<i32> {
        let mut stmt = transaction.prepare_cached("PRAGMA user_version;")?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Counts user-created schema objects in a bucket database.
    fn user_table_count(transaction: &Transaction) -> SqliteResult<i64> {
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
    pub(crate) fn create_tables(transaction: &Transaction) -> SqliteResult<()> {
        let version = Self::schema_version(transaction)?;
        if version == BUCKET_SCHEMA_VERSION {
            return Ok(());
        }

        if version != 0 || Self::user_table_count(transaction)? > 0 {
            return Err(SqliteError::UnsupportedSchemaVersion {
                found: version,
                expected: BUCKET_SCHEMA_VERSION,
            });
        }

        transaction.execute_batch(BUCKET_SCHEMA_SQL)?;

        Ok(())
    }

    /// Prepares a writable bucket database before long-lived worker pools open it.
    ///
    /// This creates or validates schema, removes expired multipart state, and
    /// refreshes planner statistics before read-only worker connections cache a
    /// schema view.
    pub(super) async fn prepare_bucket_file(
        path: PathBuf,
        config: &crate::Config,
        bucket: &str,
    ) -> SqliteResult<()> {
        if config.read_only(Some(bucket)) {
            return Ok(());
        }

        let config = config.clone();
        let bucket = bucket.to_string();
        tokio::task::spawn_blocking(move || {
            let mut connection = rusqlite::Connection::open(path)?;
            connection.set_transaction_behavior(rusqlite::TransactionBehavior::Immediate);
            Self::prepare_writable_bucket_connection(&mut connection, &config, &bucket)
        })
        .await
        .map_err(|_| SqliteError::ThreadJoin)?
    }

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
        if config.blob_readers == 0 {
            Err(SqliteError::Configuration(
                "blob_readers must be greater than zero".to_string(),
            ))?;
        }

        let root = env::current_dir()?.join(&config.root).canonicalize()?;
        let blob_chunk_size = Self::default_blob_chunk_size()?;
        let buckets = Self::open_configured_buckets(&root, config).await?;

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
            blob_chunk_size,
        })
    }

    /// Prepares an already opened writable bucket connection.
    fn prepare_writable_bucket_connection(
        connection: &mut rusqlite::Connection,
        config: &crate::Config,
        bucket: &str,
    ) -> SqliteResult<()> {
        let multipart_expire_before = expiration_threshold(config.multipart_upload_ttl_seconds);
        let completed_multipart_expire_before =
            expiration_threshold(config.completed_multipart_upload_ttl_seconds);

        connection.execute_batch(&writer_connection_pragmas(config, Some(bucket)))?;

        let transaction = connection.transaction()?;
        Self::create_tables(&transaction)?;
        Self::delete_expired_multipart_uploads(
            &transaction,
            multipart_expire_before,
            completed_multipart_expire_before,
        )?;
        transaction.commit()?;
        connection.execute_batch("PRAGMA optimize=0x10002;")?;
        Ok(())
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
                    Self::maintain_connection(
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
    pub(super) async fn maintain_connection(
        connection: &BucketConnection,
        multipart_expire_before: Option<OffsetDateTime>,
        completed_multipart_expire_before: Option<OffsetDateTime>,
        vacuum_pages: u32,
    ) -> SqliteResult<()> {
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::delete_expired_multipart_uploads(
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
                connection.execute_batch("PRAGMA optimize;")?;

                Ok(())
            })
            .await
    }

    /// Deletes incomplete and completed multipart records older than configured cutoffs.
    pub(crate) fn delete_expired_multipart_uploads(
        transaction: &Transaction,
        incomplete_expire_before: Option<OffsetDateTime>,
        completed_expire_before: Option<OffsetDateTime>,
    ) -> SqliteResult<()> {
        if let Some(expire_before) = incomplete_expire_before {
            Self::delete_incomplete_multipart_uploads_expired(transaction, expire_before)?;
        }
        if let Some(expire_before) = completed_expire_before {
            Self::delete_completed_multipart_uploads_expired(transaction, expire_before)?;
        }

        Ok(())
    }

    /// Deletes incomplete multipart uploads older than the expiration timestamp.
    fn delete_incomplete_multipart_uploads_expired(
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
    fn delete_completed_multipart_uploads_expired(
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
    use std::{
        collections::{HashMap, HashSet},
        fs,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use rusqlite::Connection;
    use tokio::sync::RwLock;

    use crate::{
        Config,
        config::{Bucket, BucketPragmas, JournalMode, Pragmas, Synchronous, TempStore},
        database::BucketConnection,
    };

    use super::{
        BucketVersioningState, Duration, MultipartUploadState, OffsetDateTime, Sqlite, SqliteError,
        SqliteResult, Uuid, expiration_threshold, reader_connection_pragmas, row_size, sql_limit,
        sql_size, writer_connection_pragmas,
    };

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    fn temp_root(name: &str) -> SqliteResult<PathBuf> {
        let root = std::env::temp_dir().join(format!("s3ite-{name}-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        Ok(root)
    }

    fn remove_root(root: &Path) {
        fs::remove_dir_all(root).ok();
    }

    fn index_columns(connection: &Connection, index: &str) -> SqliteResult<Vec<String>> {
        let mut stmt = connection.prepare(&format!("PRAGMA index_info('{index}');"))?;
        Ok(stmt
            .query_map((), |row| row.get(2))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn index_sql(connection: &Connection, index: &str) -> SqliteResult<String> {
        Ok(connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?1;",
            [index],
            |row| row.get(0),
        )?)
    }

    fn query_plan_uses_index(connection: &Connection, sql: &str, index: &str) -> SqliteResult<()> {
        let mut stmt = connection.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let details = stmt
            .query_map((), |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(
            details.iter().any(|detail| detail.contains(index)),
            "expected query plan to use {index}, got {details:?}"
        );
        Ok(())
    }

    #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct ForeignKeyColumn {
        id: i64,
        seq: i64,
        table: String,
        from: String,
        to: String,
        on_delete: String,
    }

    fn foreign_key_columns(
        connection: &Connection,
        table: &str,
    ) -> SqliteResult<Vec<ForeignKeyColumn>> {
        let mut stmt = connection.prepare(&format!("PRAGMA foreign_key_list('{table}');"))?;
        let mut columns = stmt
            .query_map((), |row| {
                Ok(ForeignKeyColumn {
                    id: row.get(0)?,
                    seq: row.get(1)?,
                    table: row.get(2)?,
                    from: row.get(3)?,
                    to: row.get(4)?,
                    on_delete: row.get(6)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        columns.sort();
        Ok(columns)
    }

    #[test]
    fn size_and_limit_helpers_reject_negative_values() -> SqliteResult<()> {
        let connection = Connection::open_in_memory()?;

        let result = connection.query_row("SELECT -1;", (), |row| row_size(row, 0));
        assert!(result.is_err());
        assert!(sql_size(-1).is_err());
        assert!(sql_limit(-1).is_err());
        assert_eq!(sql_size(0)?, 0);
        assert_eq!(sql_limit(1)?, 1);

        Ok(())
    }

    #[test]
    fn connection_pragmas_render_configured_sqlite_settings() {
        let mut config = Config {
            read_only: true,
            sqlite: Pragmas {
                journal_mode: JournalMode::Delete,
                synchronous: Synchronous::Full,
                temp_store: TempStore::File,
                cache_size: 1024,
                busy_timeout_milliseconds: 250,
                journal_size_limit: 4096,
            },
            ..Config::default()
        };

        let service_sql = writer_connection_pragmas(&config, None);
        assert!(service_sql.contains("PRAGMA journal_mode=delete;"));
        assert!(service_sql.contains("PRAGMA synchronous=full;"));
        assert!(service_sql.contains("PRAGMA temp_store=file;"));
        assert!(service_sql.contains("PRAGMA cache_size=-1024;"));
        assert!(service_sql.contains("PRAGMA busy_timeout=250;"));
        assert!(service_sql.contains("PRAGMA journal_size_limit=4096;"));
        assert!(service_sql.contains("PRAGMA query_only=true;"));
        assert!(service_sql.contains("PRAGMA foreign_keys=true;"));
        assert!(service_sql.contains("PRAGMA trusted_schema=false;"));
        assert!(service_sql.contains("PRAGMA auto_vacuum=INCREMENTAL;"));

        config.buckets.insert(
            "bucket".to_string(),
            Bucket {
                read_only: Some(false),
                sqlite: Some(BucketPragmas {
                    journal_mode: Some(JournalMode::Wal),
                    synchronous: Some(Synchronous::Normal),
                    temp_store: Some(TempStore::Memory),
                    cache_size: Some(512),
                    busy_timeout_milliseconds: Some(100),
                    journal_size_limit: Some(2048),
                }),
            },
        );

        let bucket_sql = writer_connection_pragmas(&config, Some("bucket"));
        assert!(bucket_sql.contains("PRAGMA journal_mode=wal;"));
        assert!(bucket_sql.contains("PRAGMA synchronous=normal;"));
        assert!(bucket_sql.contains("PRAGMA temp_store=memory;"));
        assert!(bucket_sql.contains("PRAGMA cache_size=-512;"));
        assert!(bucket_sql.contains("PRAGMA busy_timeout=100;"));
        assert!(bucket_sql.contains("PRAGMA journal_size_limit=2048;"));
        assert!(bucket_sql.contains("PRAGMA query_only=false;"));

        let reader_sql = reader_connection_pragmas(&config, Some("bucket"));
        assert!(reader_sql.contains("PRAGMA temp_store=memory;"));
        assert!(reader_sql.contains("PRAGMA cache_size=-512;"));
        assert!(reader_sql.contains("PRAGMA busy_timeout=100;"));
        assert!(reader_sql.contains("PRAGMA foreign_keys=true;"));
        assert!(reader_sql.contains("PRAGMA trusted_schema=false;"));
        assert!(!reader_sql.contains("PRAGMA journal_mode"));
        assert!(!reader_sql.contains("PRAGMA journal_size_limit"));
        assert!(!reader_sql.contains("PRAGMA query_only"));
    }

    #[test]
    fn versioning_state_sql_roundtrips_and_rejects_unknown_values() -> SqliteResult<()> {
        let connection = Connection::open_in_memory()?;
        for state in [
            BucketVersioningState::NeverConfigured,
            BucketVersioningState::Enabled,
            BucketVersioningState::Suspended,
        ] {
            let expected_sql = state.sql_code();
            let encoded =
                connection.query_row("SELECT ?1;", [state], |row| row.get::<_, i64>(0))?;
            let decoded = connection.query_row("SELECT ?1;", [expected_sql], |row| {
                row.get::<_, BucketVersioningState>(0)
            })?;
            assert_eq!(encoded, expected_sql);
            assert_eq!(decoded, state);
        }
        let invalid_state = connection.query_row("SELECT 99;", (), |row| {
            row.get::<_, BucketVersioningState>(0)
        });
        let invalid_type = connection.query_row("SELECT 'enabled';", (), |row| {
            row.get::<_, BucketVersioningState>(0)
        });
        assert!(invalid_state.is_err());
        assert!(invalid_type.is_err());
        assert!(!BucketVersioningState::NeverConfigured.returns_write_version_id());
        assert!(BucketVersioningState::Enabled.returns_write_version_id());
        assert!(!BucketVersioningState::Suspended.returns_write_version_id());
        Ok(())
    }

    #[test]
    fn expiration_threshold_handles_disabled_and_active_ttls() {
        assert!(expiration_threshold(0).is_none());
        assert!(
            expiration_threshold(60)
                .is_some_and(|threshold| threshold <= OffsetDateTime::now_utc())
        );
    }

    #[test]
    fn schema_creation_is_idempotent_for_current_version() -> SqliteResult<()> {
        let mut connection = Connection::open_in_memory()?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;

        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;

        let version: i32 = connection.query_row("PRAGMA user_version;", (), |row| row.get(0))?;
        assert_eq!(version, super::BUCKET_SCHEMA_VERSION);
        Ok(())
    }

    #[test]
    fn schema_tables_are_strict_rowid_tables() -> SqliteResult<()> {
        let connection = open_schema()?;
        let mut stmt = connection.prepare("PRAGMA table_list;")?;
        let mut seen = HashSet::new();
        let rows = stmt.query_map((), |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i32>(4)?,
                row.get::<_, i32>(5)?,
            ))
        })?;

        for row in rows {
            let (name, table_type, without_rowid, strict) = row?;
            if table_type != "table" || name.starts_with("sqlite_") {
                continue;
            }

            assert_eq!(without_rowid, 0, "{name} must keep a rowid");
            assert_eq!(strict, 1, "{name} must be STRICT");
            seen.insert(name);
        }

        for table in [
            "bucket_state",
            "object_version",
            "multipart_upload",
            "object_part",
            "object_blob_chunk",
            "completed_multipart_upload",
        ] {
            assert!(seen.contains(table), "missing schema table {table}");
        }

        Ok(())
    }

    #[test]
    fn schema_foreign_keys_cover_dependent_rows() -> SqliteResult<()> {
        let connection = open_schema()?;

        for table in ["object_part", "object_blob_chunk"] {
            let columns = foreign_key_columns(&connection, table)?;
            assert!(
                columns.contains(&ForeignKeyColumn {
                    id: 0,
                    seq: 0,
                    table: "multipart_upload".to_string(),
                    from: "upload_id".to_string(),
                    to: "upload_id".to_string(),
                    on_delete: "CASCADE".to_string(),
                }) || columns.iter().any(|column| {
                    column.table == "multipart_upload"
                        && column.from == "upload_id"
                        && column.to == "upload_id"
                        && column.on_delete == "CASCADE"
                })
            );
            assert!(columns.iter().any(|column| {
                column.table == "object_version"
                    && column.from == "object_sequence"
                    && column.to == "sequence"
                    && column.on_delete == "CASCADE"
            }));
        }

        let blob_chunk_columns = foreign_key_columns(&connection, "object_blob_chunk")?;
        assert!(blob_chunk_columns.iter().any(|column| {
            column.table == "object_part"
                && column.from == "object_sequence"
                && column.to == "object_sequence"
                && column.on_delete == "CASCADE"
        }));
        assert!(blob_chunk_columns.iter().any(|column| {
            column.table == "object_part"
                && column.from == "part_number"
                && column.to == "part_number"
                && column.on_delete == "CASCADE"
        }));
        assert!(blob_chunk_columns.iter().any(|column| {
            column.table == "object_part"
                && column.from == "upload_id"
                && column.to == "upload_id"
                && column.on_delete == "CASCADE"
        }));

        Ok(())
    }

    #[test]
    fn schema_indexes_cover_list_and_cleanup_queries() -> SqliteResult<()> {
        let connection = open_schema()?;

        assert_eq!(
            index_columns(&connection, "object_version_latest_one")?,
            ["key"]
        );
        assert_eq!(
            index_columns(&connection, "object_version_current_list")?,
            ["key"]
        );
        assert_eq!(
            index_columns(&connection, "object_version_list")?,
            ["key", "sequence"]
        );
        assert_eq!(
            index_columns(&connection, "multipart_upload_list")?,
            ["key", "upload_id"]
        );
        assert_eq!(
            index_columns(&connection, "object_part_upload_part")?,
            ["upload_id", "part_number"]
        );
        assert_eq!(
            index_columns(&connection, "object_part_object_part")?,
            ["object_sequence", "part_number"]
        );
        assert_eq!(
            index_columns(&connection, "object_part_completed_list")?,
            ["object_sequence", "is_multipart_part", "part_number"]
        );
        assert_eq!(
            index_columns(&connection, "object_blob_chunk_upload_order")?,
            ["upload_id", "part_number", "chunk_number"]
        );
        assert_eq!(
            index_columns(&connection, "object_blob_chunk_object_offset")?,
            ["object_sequence", "offset"]
        );
        assert!(
            index_sql(&connection, "multipart_upload_expiry")?.contains("DATETIME(last_modified)")
        );
        assert!(
            index_sql(&connection, "completed_multipart_upload_expiry")?
                .contains("DATETIME(last_modified)")
        );

        query_plan_uses_index(
            &connection,
            "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value \
             FROM multipart_upload \
             WHERE key >= 'prefix' \
             ORDER BY key, upload_id \
             LIMIT 10;",
            "multipart_upload_list",
        )?;
        query_plan_uses_index(
            &connection,
            "DELETE FROM multipart_upload \
             WHERE DATETIME(last_modified) < DATETIME('2026-01-01 00:00:00Z');",
            "multipart_upload_expiry",
        )?;
        query_plan_uses_index(
            &connection,
            "DELETE FROM completed_multipart_upload \
             WHERE DATETIME(last_modified) < DATETIME('2026-01-01 00:00:00Z');",
            "completed_multipart_upload_expiry",
        )?;

        Ok(())
    }

    #[test]
    fn nonzero_empty_schema_version_is_rejected() -> SqliteResult<()> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA user_version = 4;")?;
        let transaction = connection.transaction()?;

        let err = Sqlite::create_tables(&transaction).err();
        assert!(matches!(
            err,
            Some(SqliteError::UnsupportedSchemaVersion {
                found: 4,
                expected: super::BUCKET_SCHEMA_VERSION
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn configured_missing_bucket_fails_startup() -> SqliteResult<()> {
        let root = temp_root("missing-configured-bucket")?;
        let mut buckets = HashMap::new();
        buckets.insert(
            "missing".to_string(),
            Bucket {
                read_only: Some(true),
                sqlite: None,
            },
        );

        let err = Sqlite::new(&Config {
            root: root.clone(),
            buckets,
            ..Default::default()
        })
        .await
        .err();
        assert!(
            matches!(err, Some(SqliteError::Configuration(message)) if message.contains("missing"))
        );

        remove_root(&root);
        Ok(())
    }

    #[tokio::test]
    async fn prepare_bucket_file_skips_read_only_buckets() -> SqliteResult<()> {
        let root = temp_root("prepare-read-only")?;
        let path = root.join("bucket.sqlite3");
        Connection::open(&path)?.close().map_err(|(_, err)| err)?;
        let config = Config {
            read_only: true,
            ..Default::default()
        };

        Sqlite::prepare_bucket_file(path.clone(), &config, "bucket").await?;
        let connection = Connection::open(&path)?;
        let version: i32 = connection.query_row("PRAGMA user_version;", (), |row| row.get(0))?;
        assert_eq!(version, 0);

        remove_root(&root);
        Ok(())
    }

    #[tokio::test]
    async fn prepare_bucket_file_creates_schema_before_readers_open() -> SqliteResult<()> {
        let root = temp_root("prepare-writable")?;
        let path = root.join("bucket.sqlite3");

        Sqlite::prepare_bucket_file(path.clone(), &Config::default(), "bucket").await?;
        let connection = BucketConnection::open(&path, &Config::default(), "bucket").await?;
        let version: i32 = connection
            .read(|connection| {
                Ok(connection.query_row("PRAGMA user_version;", (), |row| row.get(0))?)
            })
            .await?;
        assert_eq!(version, super::BUCKET_SCHEMA_VERSION);

        connection.close().await?;
        remove_root(&root);
        Ok(())
    }

    #[tokio::test]
    async fn maintain_connection_runs_checkpoint_and_optional_vacuum() -> SqliteResult<()> {
        let root = temp_root("maintain")?;
        let path = root.join("bucket.sqlite3");
        let connection = BucketConnection::open(&path, &Config::default(), "bucket").await?;
        connection
            .write(|connection| {
                let transaction = connection.transaction()?;
                Sqlite::create_tables(&transaction)?;
                transaction.commit()?;
                Ok(())
            })
            .await?;

        Sqlite::maintain_connection(&connection, None, None, 0).await?;
        Sqlite::maintain_connection(&connection, None, None, 1).await?;

        connection.close().await?;
        remove_root(&root);
        Ok(())
    }

    #[tokio::test]
    async fn old_bucket_schema_is_rejected() -> SqliteResult<()> {
        let root = temp_root("old-schema")?;
        let connection = Connection::open(root.join("old.sqlite3"))?;
        connection.execute("CREATE TABLE data (key TEXT PRIMARY KEY, value BLOB);", ())?;
        drop(connection);

        let err = Sqlite::new(&Config {
            root: root.clone(),
            ..Default::default()
        })
        .await
        .err();
        assert!(matches!(
            err,
            Some(SqliteError::UnsupportedSchemaVersion {
                found: 0,
                expected: super::BUCKET_SCHEMA_VERSION
            })
        ));

        remove_root(&root);
        Ok(())
    }

    #[tokio::test]
    async fn zero_readers_fails_startup() -> SqliteResult<()> {
        let root = temp_root("zero-readers")?;

        let err = Sqlite::new(&Config {
            root: root.clone(),
            readers: 0,
            ..Default::default()
        })
        .await
        .err();
        assert!(matches!(
            err,
            Some(SqliteError::Configuration(message)) if message == "readers must be greater than zero"
        ));

        remove_root(&root);
        Ok(())
    }

    #[tokio::test]
    async fn zero_blob_readers_fails_startup() -> SqliteResult<()> {
        let root = temp_root("zero-blob-readers")?;

        let err = Sqlite::new(&Config {
            root: root.clone(),
            blob_readers: 0,
            ..Default::default()
        })
        .await
        .err();
        assert!(matches!(
            err,
            Some(SqliteError::Configuration(message))
                if message == "blob_readers must be greater than zero"
        ));

        remove_root(&root);
        Ok(())
    }

    #[test]
    fn object_rows_require_checksum_for_live_objects() -> SqliteResult<()> {
        let connection = open_schema()?;
        let result = connection.execute(
            "
            INSERT INTO object_version (
                key, version_id, is_latest, is_delete_marker, size, last_modified, md5
            )
            VALUES ('key', 'null', 1, 0, 1, ?1, 'md5');",
            [OffsetDateTime::now_utc()],
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn delete_markers_reject_checksum_columns() -> SqliteResult<()> {
        let connection = open_schema()?;
        let result = connection.execute(
            "
            INSERT INTO object_version (
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES ('key', 'null', 1, 1, 0, ?1, NULL, 3, 2, 'crc');",
            [OffsetDateTime::now_utc()],
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn object_blob_chunk_rows_cascade_with_object_versions() -> SqliteResult<()> {
        let connection = open_schema()?;
        connection.execute(
            "
            INSERT INTO object_version (
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES ('key', 'null', 1, 0, 1, ?1, 'md5', 3, 2, 'crc');",
            [OffsetDateTime::now_utc()],
        )?;
        let sequence = connection.last_insert_rowid();
        connection.execute(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size
            )
            VALUES (?1, NULL, 0, ?2, 1, 0, 1);",
            (sequence, OffsetDateTime::now_utc()),
        )?;
        connection.execute(
            "
            INSERT INTO object_blob_chunk (
                object_sequence,
                upload_id,
                part_number,
                chunk_number,
                offset,
                size,
                body
            )
            VALUES (?1, NULL, 1, 1, 0, 1, X'00');",
            [sequence],
        )?;

        connection.execute(
            "DELETE FROM object_version WHERE sequence = ?1;",
            [sequence],
        )?;
        let body_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM object_blob_chunk;", (), |row| {
                row.get(0)
            })?;

        assert_eq!(body_count, 0);
        Ok(())
    }

    #[test]
    fn multipart_config_rows_reject_values() -> SqliteResult<()> {
        let connection = open_schema()?;
        let result = connection.execute(
            "
            INSERT INTO multipart_upload (
                upload_id,
                bucket,
                key,
                initiated,
                last_modified,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (?1, 'bucket', 'key', ?2, ?2, 5, 1, 'sha');",
            (Uuid::new_v4(), OffsetDateTime::now_utc()),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn object_part_rows_reject_checksum_types() -> SqliteResult<()> {
        let connection = open_schema()?;
        let upload_id = Uuid::new_v4();
        connection.execute(
            "
            INSERT INTO multipart_upload (upload_id, bucket, key, initiated, last_modified)
            VALUES (?1, 'bucket', 'key', ?2, ?2);",
            (upload_id, OffsetDateTime::now_utc()),
        )?;

        let result = connection.execute(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (NULL, ?1, 1, ?2, 1, NULL, 1, 5, 1, 'sha');",
            (upload_id, OffsetDateTime::now_utc()),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn blob_chunk_rows_require_one_owner_and_matching_size() -> SqliteResult<()> {
        let connection = open_schema()?;
        connection.execute(
            "
            INSERT INTO object_version (
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES ('key', 'null', 1, 0, 1, ?1, 'md5', 3, 2, 'crc');",
            [OffsetDateTime::now_utc()],
        )?;
        let sequence = connection.last_insert_rowid();
        connection.execute(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size
            )
            VALUES (?1, NULL, 0, ?2, 1, 0, 1);",
            (sequence, OffsetDateTime::now_utc()),
        )?;

        let no_owner = connection.execute(
            "
            INSERT INTO object_blob_chunk (
                object_sequence,
                upload_id,
                part_number,
                chunk_number,
                offset,
                size,
                body
            )
            VALUES (NULL, NULL, 1, 1, 0, 1, X'00');",
            (),
        );
        let missing_part = connection.execute(
            "
            INSERT INTO object_blob_chunk (
                object_sequence,
                upload_id,
                part_number,
                chunk_number,
                offset,
                size,
                body
            )
            VALUES (?1, NULL, 2, 1, 0, 1, X'00');",
            [sequence],
        );
        let wrong_size = connection.execute(
            "
            INSERT INTO object_blob_chunk (
                object_sequence,
                upload_id,
                part_number,
                chunk_number,
                offset,
                size,
                body
            )
            VALUES (?1, NULL, 1, 1, 0, 2, X'00');",
            [sequence],
        );

        assert!(no_owner.is_err());
        assert!(missing_part.is_err());
        assert!(wrong_size.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn maintenance_task_runs_periodic_cleanup_for_writable_buckets() -> SqliteResult<()> {
        let root = temp_root("periodic-maintenance")?;
        let path = root.join("bucket.sqlite3");
        let connection =
            Arc::new(BucketConnection::open(&path, &Config::default(), "bucket").await?);
        connection
            .write(|connection| {
                let transaction = connection.transaction()?;
                Sqlite::create_tables(&transaction)?;
                let upload_id = Uuid::new_v4();
                Sqlite::create_multipart_upload(
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
                        checksum: None,
                        encryption: None,
                    },
                )?;
                transaction.execute(
                    "UPDATE multipart_upload SET last_modified = ?2 WHERE upload_id = ?1;",
                    (
                        upload_id,
                        OffsetDateTime::now_utc().saturating_sub(Duration::hours(2)),
                    ),
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await?;

        let mut bucket_map = HashMap::new();
        bucket_map.insert("bucket".to_string(), connection.clone());
        Sqlite::spawn_maintenance_task(
            Arc::new(RwLock::new(bucket_map)),
            Config {
                maintenance_interval_seconds: 1,
                maintenance_vacuum_pages: 0,
                multipart_upload_ttl_seconds: 1,
                completed_multipart_upload_ttl_seconds: 0,
                ..Default::default()
            },
        );
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

        let remaining: i64 = connection
            .read(|connection| {
                Ok(
                    connection.query_row("SELECT COUNT(*) FROM multipart_upload;", (), |row| {
                        row.get(0)
                    })?,
                )
            })
            .await?;
        assert_eq!(remaining, 0);

        connection.close().await?;
        remove_root(&root);
        Ok(())
    }
}

#[cfg(test)]
mod maintenance_tests {
    use rusqlite::Connection;

    use crate::checksum::{FullObjectChecksum, StoredChecksum};

    use super::{
        CompletedMultipartUploadRecord, Duration, Multipart, MultipartUploadState, NULL_VERSION_ID,
        OffsetDateTime, Sqlite, SqliteResult, Uuid,
    };

    fn open_test_connection() -> rusqlite::Result<Connection> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        Ok(connection)
    }

    #[test]
    fn multipart_touch_keeps_active_upload_out_of_cleanup() -> SqliteResult<()> {
        let mut connection = open_test_connection()?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;

        let upload_id = Uuid::new_v4();
        Sqlite::create_multipart_upload(
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
                checksum: None,
                encryption: None,
            },
        )?;
        transaction.execute(
            "UPDATE multipart_upload SET last_modified = ?2 WHERE upload_id = ?1;",
            (
                upload_id,
                OffsetDateTime::now_utc().saturating_sub(Duration::hours(2)),
            ),
        )?;

        Sqlite::touch_multipart_upload(&transaction, upload_id)?;
        Sqlite::delete_expired_multipart_uploads(
            &transaction,
            Some(OffsetDateTime::now_utc().saturating_sub(Duration::hours(1))),
            None,
        )?;

        let state = Sqlite::get_multipart_upload_state(&transaction, upload_id, "bucket", "key")?;
        assert!(state.is_some());

        Ok(())
    }

    #[test]
    fn multipart_cleanup_keeps_completed_ledger_separate() -> SqliteResult<()> {
        let mut connection = open_test_connection()?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;

        let upload_id = Uuid::new_v4();
        Sqlite::create_multipart_upload(
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
                checksum: None,
                encryption: None,
            },
        )?;
        let multipart = Multipart {
            upload_id,
            part_number: 1,
            last_modified: OffsetDateTime::now_utc(),
            value: b"part".to_vec(),
            size: 4,
            md5: None,
            checksum: None,
            encryption: None,
        };
        Sqlite::put_multipart(&transaction, &multipart)?;
        transaction.execute(
            "UPDATE multipart_upload SET last_modified = ?2 WHERE upload_id = ?1;",
            (
                upload_id,
                OffsetDateTime::now_utc().saturating_sub(Duration::hours(2)),
            ),
        )?;

        let completed_upload_id = Uuid::new_v4();
        let checksum = StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("crc".to_string()));
        Sqlite::put_completed_multipart_upload(
            &transaction,
            CompletedMultipartUploadRecord {
                upload_id: completed_upload_id,
                bucket: "bucket",
                key: "completed-key",
                access_key: None,
                md5: "etag",
                version_id: NULL_VERSION_ID,
                returned_version_id: None,
                checksum: &checksum,
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
        Sqlite::delete_expired_multipart_uploads(&transaction, Some(expire_before), None)?;

        let upload_state =
            Sqlite::get_multipart_upload_state(&transaction, upload_id, "bucket", "key")?;
        assert!(upload_state.is_none());
        assert!(Sqlite::get_multiparts(&transaction, upload_id)?.is_empty());

        let completed = Sqlite::get_completed_multipart_upload(
            &transaction,
            completed_upload_id,
            "bucket",
            "completed-key",
        )?;
        assert!(completed.is_some());

        Sqlite::delete_expired_multipart_uploads(&transaction, None, Some(expire_before))?;
        let completed = Sqlite::get_completed_multipart_upload(
            &transaction,
            completed_upload_id,
            "bucket",
            "completed-key",
        )?;
        assert!(completed.is_none());

        Ok(())
    }
}
