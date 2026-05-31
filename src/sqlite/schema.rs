use super::{Sqlite, SqliteError, SqliteResult, Transaction};

pub(super) const BUCKET_SCHEMA_VERSION: i32 = 5;
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
    checksum_algorithm INTEGER CHECK(checksum_algorithm BETWEEN 1 AND 5),
    checksum_type INTEGER CHECK(checksum_type BETWEEN 1 AND 2),
    checksum_value TEXT,
    UNIQUE(key, version_id),
    CHECK (
        (
            is_delete_marker = 0
            AND body IS NOT NULL
            AND md5 IS NOT NULL
            AND checksum_algorithm IS NOT NULL
            AND checksum_type IS NOT NULL
            AND checksum_value IS NOT NULL
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
            AND checksum_value IS NULL
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

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::Sqlite;
    use crate::sqlite::SqliteResult;

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
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
                key, version_id, is_latest, is_delete_marker, body, size, last_modified, md5
            )
            VALUES ('key', 'null', 1, 0, X'00', 1, ?1, 'md5');",
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
                body,
                size,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES ('key', 'null', 1, 1, NULL, 0, ?1, NULL, 3, 2, 'crc');",
            [OffsetDateTime::now_utc()],
        );

        assert!(result.is_err());
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
