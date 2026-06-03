//! `SQLite` object write and version ID helpers.

use std::ops::Not;

use rusqlite::{Error::ToSqlConversionFailure, Transaction};

use super::{
    BucketVersioningState, KeyValue, NULL_VERSION_ID, ObjectTagCondition, ObjectWriteConditions,
    ObjectWriteOutcome, Sqlite, SqliteError, SqliteResult, sql_size,
};

const VERSION_ID_LEN: usize = 31;
const VERSION_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";

/// Generates one opaque Ceph-shaped object version ID.
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

impl Sqlite {
    /// Generates an unused opaque version ID for a key.
    pub(super) fn generate_unique_version_id(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<String> {
        loop {
            let version_id = generate_version_id()?;
            if Self::version_exists(transaction, key, &version_id)?.not() {
                return Ok(version_id);
            }
        }
    }

    /// Returns whether an exact object version row exists.
    pub(super) fn version_exists(
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
    pub(super) fn clear_latest(transaction: &Transaction, key: &str) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE object_version
            SET is_latest = 0
            WHERE key = ?1;",
        )?;
        Ok(stmt.execute([key])?)
    }

    /// Checks object write preconditions against current metadata in the transaction.
    pub(crate) fn check_object_write_conditions(
        transaction: &Transaction,
        key: &str,
        conditions: &ObjectWriteConditions,
    ) -> SqliteResult<()> {
        if conditions.if_match.is_none() && conditions.if_none_match.is_none() {
            return Ok(());
        }

        let current = Self::get_metadata(transaction, key)?;
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

    /// Inserts an object version or replaces the `null` version.
    pub(crate) fn put_object(
        transaction: &Transaction,
        kv: KeyValue,
    ) -> SqliteResult<ObjectWriteOutcome> {
        let KeyValue {
            key,
            value,
            key_metadata,
            ..
        } = kv;
        let value = value.ok_or(SqliteError::ObjectBodyInconsistent)?;
        let state = Self::get_bucket_versioning_state(transaction)?;
        let version_id = match state {
            BucketVersioningState::Enabled => Self::generate_unique_version_id(transaction, &key)?,
            BucketVersioningState::NeverConfigured | BucketVersioningState::Suspended => {
                NULL_VERSION_ID.to_string()
            }
        };
        let returned_version_id = state.returns_write_version_id().then(|| version_id.clone());

        if state.returns_write_version_id().not() {
            Self::delete_version_row(transaction, &key, NULL_VERSION_ID)?;
        }
        Self::clear_latest(transaction, &key)?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_version (
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                metadata,
                content_type,
                content_encoding,
                cache_control,
                expires,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (?1, ?2, 1, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13);",
        )?;

        let size = sql_size(key_metadata.size)?;
        let checksum = key_metadata
            .checksum
            .as_ref()
            .ok_or(SqliteError::ChecksumMismatch)?;
        stmt.execute(rusqlite::params![
            &key,
            &version_id,
            size,
            key_metadata
                .metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            key_metadata.content_type,
            key_metadata.content_encoding,
            key_metadata.cache_control,
            key_metadata.expires,
            key_metadata.last_modified,
            key_metadata.md5.as_deref(),
            checksum.algorithm(),
            checksum.checksum_type(),
            checksum.as_str(),
        ])?;
        drop(stmt);

        let sequence = transaction.last_insert_rowid();
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_body (sequence, body)
            VALUES (?1, ?2);",
        )?;
        stmt.execute((sequence, value))?;

        Ok(ObjectWriteOutcome {
            version_id,
            returned_version_id,
            md5: key_metadata.md5,
            last_modified: key_metadata.last_modified,
        })
    }
}
