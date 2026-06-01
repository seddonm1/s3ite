use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Not,
    path::PathBuf,
    sync::Arc,
};

use rusqlite::{Error::ToSqlConversionFailure, Transaction};
use time::{Duration, OffsetDateTime};
use tokio::sync::RwLock;
use uuid::Uuid;

pub(crate) use crate::checksum::{
    MultipartChecksumConfig, MultipartCompletionChecksum, PartChecksum, StoredChecksum,
};
use crate::database::BucketConnection;

mod bucket;
mod checksum;
mod error;
mod list;
mod multipart;
mod object;

use self::checksum::{
    decode_base64, decode_md5_hex, encode_hex, optional_multipart_checksum_config,
    optional_part_checksum, optional_stored_checksum, required_stored_checksum,
};
use self::error::{NegativeObjectSize, NegativeSqlLimit};
pub use self::error::{SqliteError, SqliteResult};

pub(crate) const NULL_VERSION_ID: &str = "null";

const BUCKET_SCHEMA_VERSION: i32 = 5;
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
    UNIQUE(key, version_id),
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
        )
    )
) STRICT;

CREATE TABLE object_body (
    sequence INTEGER PRIMARY KEY,
    body BLOB NOT NULL,
    FOREIGN KEY (sequence) REFERENCES object_version (sequence) ON DELETE CASCADE
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
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    PRIMARY KEY (key, version_id, part_number),
    FOREIGN KEY (key, version_id) REFERENCES object_version (key, version_id) ON DELETE CASCADE,
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
    content_encoding TEXT,
    cache_control TEXT,
    expires TEXT,
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    UNIQUE(upload_id, bucket, key),
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

CREATE TABLE multipart_upload_part (
    upload_id BLOB NOT NULL,
    last_modified TEXT NOT NULL,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    value BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size >= 0),
    md5 TEXT,
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE,
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
) WITHOUT ROWID, STRICT;

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

INSERT INTO bucket_state (id, versioning_state)
VALUES (1, 'never_configured');

PRAGMA user_version = 5;
";

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

fn row_bool(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<bool> {
    Ok(row.get::<_, i32>(idx)? != 0)
}

fn expiration_threshold(ttl_seconds: u64) -> Option<OffsetDateTime> {
    if ttl_seconds == 0 {
        return None;
    }

    let seconds = i64::try_from(ttl_seconds).unwrap_or(i64::MAX);
    Some(OffsetDateTime::now_utc().saturating_sub(Duration::seconds(seconds)))
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
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) key_metadata: KeyMetadata,
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
    pub(crate) version_id: String,
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
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) checksum: StoredChecksum,
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
    pub(crate) md5: Option<String>,
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
}

/// SQLite-backed storage state and open bucket connections.
#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
}

impl Sqlite {
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
    use rusqlite::Connection;

    use super::{OffsetDateTime, Sqlite, SqliteResult, Uuid};

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::try_create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
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
    fn object_body_rows_cascade_with_object_versions() -> SqliteResult<()> {
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
            "INSERT INTO object_body (sequence, body) VALUES (?1, X'00');",
            [sequence],
        )?;

        connection.execute(
            "DELETE FROM object_version WHERE sequence = ?1;",
            [sequence],
        )?;
        let body_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM object_body;", (), |row| row.get(0))?;

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
    fn multipart_part_rows_reject_checksum_types() -> SqliteResult<()> {
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
            INSERT INTO multipart_upload_part (
                upload_id,
                last_modified,
                part_number,
                value,
                size,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (?1, ?2, 1, X'00', 1, 5, 1, 'sha');",
            (upload_id, OffsetDateTime::now_utc()),
        );

        assert!(result.is_err());
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
                checksum: None,
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
                checksum: None,
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
                checksum: None,
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
        let checksum = StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("crc".to_string()));
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
