use super::{Sqlite, SqliteError, SqliteResult, Transaction};

pub(super) const BUCKET_SCHEMA_VERSION: i32 = 3;
pub(super) const BUCKET_SCHEMA_SQL: &str = "
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
    content_encoding TEXT,
    cache_control TEXT,
    expires TEXT,
    last_modified TEXT NOT NULL,
    md5 TEXT,
    checksum_algorithm TEXT,
    checksum_type TEXT,
    checksum_crc32 TEXT,
    checksum_crc32c TEXT,
    checksum_crc64nvme TEXT,
    checksum_sha1 TEXT,
    checksum_sha256 TEXT,
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
            AND content_encoding IS NULL
            AND cache_control IS NULL
            AND expires IS NULL
            AND md5 IS NULL
            AND checksum_algorithm IS NULL
            AND checksum_type IS NULL
            AND checksum_crc32 IS NULL
            AND checksum_crc32c IS NULL
            AND checksum_crc64nvme IS NULL
            AND checksum_sha1 IS NULL
            AND checksum_sha256 IS NULL
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
    checksum_crc32 TEXT,
    checksum_crc32c TEXT,
    checksum_crc64nvme TEXT,
    checksum_sha1 TEXT,
    checksum_sha256 TEXT,
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
    content_encoding TEXT,
    cache_control TEXT,
    expires TEXT,
    checksum_algorithm TEXT,
    checksum_type TEXT,
    UNIQUE(upload_id, bucket, key)
) STRICT;

CREATE TABLE multipart_upload_part (
    upload_id BLOB NOT NULL,
    last_modified TEXT NOT NULL,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    value BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size >= 0),
    md5 TEXT,
    checksum_crc32 TEXT,
    checksum_crc32c TEXT,
    checksum_crc64nvme TEXT,
    checksum_sha1 TEXT,
    checksum_sha256 TEXT,
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
    checksum_type TEXT,
    checksum_crc32 TEXT,
    checksum_crc32c TEXT,
    checksum_crc64nvme TEXT,
    checksum_sha1 TEXT,
    checksum_sha256 TEXT,
    last_modified TEXT NOT NULL,
    UNIQUE(upload_id, bucket, key)
) STRICT;

INSERT INTO bucket_state (id, versioning_state)
VALUES (1, 'never_configured');

PRAGMA user_version = 3;
";

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
}
