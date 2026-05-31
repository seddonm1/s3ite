use super::{
    BucketVersioningState, Not, OffsetDateTime, OptionalExtension, Sqlite, SqliteError,
    SqliteResult, Transaction,
};

pub(crate) const NULL_VERSION_ID: &str = "null";
pub(super) const VERSION_ID_LEN: usize = 31;
pub(super) const VERSION_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";

pub(super) fn generate_version_id() -> SqliteResult<String> {
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

impl Sqlite {
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

    /// Returns the storage sequence for an exact object version when present.
    pub(super) fn try_find_version_sequence(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<i64>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT sequence
            FROM object_version
            WHERE key = ?1 AND version_id = ?2;",
        )?;
        Ok(stmt
            .query_row((key, version_id), |row| row.get(0))
            .optional()?)
    }

    /// Generates an unused opaque version ID for a key.
    pub(super) fn try_generate_unique_version_id(
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
    pub(super) fn try_version_exists(
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
    pub(super) fn try_clear_latest(transaction: &Transaction, key: &str) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE object_version
            SET is_latest = 0
            WHERE key = ?1;",
        )?;
        Ok(stmt.execute([key])?)
    }

    /// Deletes one exact object version row.
    pub(super) fn try_delete_version_row(
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
    pub(super) fn try_promote_latest_version(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<usize> {
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
    pub(super) fn try_insert_delete_marker(
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
                content_encoding,
                cache_control,
                expires,
                last_modified,
                md5
            )
            VALUES (?1, ?2, 1, 1, NULL, 0, NULL, NULL, NULL, NULL, NULL, ?3, NULL);",
        )?;
        Ok(stmt.execute((key, version_id, OffsetDateTime::now_utc()))?)
    }
}
