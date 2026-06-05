//! `SQLite` object write and version ID helpers.

use std::ops::Not;

use rusqlite::{Error::ToSqlConversionFailure, Transaction};
use time::OffsetDateTime;

use super::{
    BucketVersioningState, KeyMetadata, KeyValue, NULL_VERSION_ID, ObjectTagCondition,
    ObjectWriteConditions, ObjectWriteOutcome, Sqlite, SqliteError, SqliteResult, sql_size,
};

const VERSION_ID_LEN: usize = 31;
const VERSION_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";

/// Generates one opaque S3-safe object version ID.
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

    /// Inserts object version metadata or replaces the `null` version.
    pub(super) fn put_object_version(
        transaction: &Transaction,
        key: &str,
        key_metadata: KeyMetadata,
    ) -> SqliteResult<ObjectWriteOutcome> {
        let state = Self::get_bucket_versioning_state(transaction)?;
        let version_id = match state {
            BucketVersioningState::Enabled => Self::generate_unique_version_id(transaction, key)?,
            BucketVersioningState::NeverConfigured | BucketVersioningState::Suspended => {
                NULL_VERSION_ID.to_string()
            }
        };
        let returned_version_id = state.returns_write_version_id().then(|| version_id.clone());

        if state.returns_write_version_id().not() {
            Self::delete_version_row(transaction, key, NULL_VERSION_ID)?;
        }
        Self::clear_latest(transaction, key)?;

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

        Ok(ObjectWriteOutcome {
            sequence,
            version_id,
            returned_version_id,
            md5: key_metadata.md5,
            last_modified: key_metadata.last_modified,
        })
    }

    /// Inserts the storage part row that owns single-request object chunks.
    fn put_object_storage_part(
        transaction: &Transaction,
        object_sequence: i64,
        last_modified: OffsetDateTime,
        size: i64,
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (?1, NULL, 0, ?2, 1, 0, ?3, NULL, NULL, NULL, NULL);",
        )?;
        stmt.execute((object_sequence, last_modified, sql_size(size)?))?;
        Ok(())
    }

    /// Inserts body bytes as bounded chunks for an object or multipart owner.
    pub(super) fn put_object_blob_chunks(
        transaction: &Transaction,
        object_sequence: Option<i64>,
        upload_id: Option<uuid::Uuid>,
        part_number: i32,
        value: &[u8],
        base_offset: i64,
        chunk_size: usize,
    ) -> SqliteResult<()> {
        if chunk_size == 0 {
            return Err(SqliteError::Configuration(
                "object blob chunk size must be greater than zero".to_string(),
            ));
        }

        let mut stmt = transaction.prepare_cached(
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
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
        )?;

        for (index, chunk) in value.chunks(chunk_size).enumerate() {
            let chunk_number =
                i32::try_from(index + 1).map_err(|_| SqliteError::MultipartObjectTooLarge)?;
            let offset = base_offset
                .checked_add(
                    i64::try_from(
                        index
                            .checked_mul(chunk_size)
                            .ok_or(SqliteError::MultipartObjectTooLarge)?,
                    )
                    .map_err(|_| SqliteError::MultipartObjectTooLarge)?,
                )
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
            let size =
                i64::try_from(chunk.len()).map_err(|_| SqliteError::MultipartObjectTooLarge)?;
            stmt.execute((
                object_sequence,
                upload_id,
                part_number,
                chunk_number,
                sql_size(offset)?,
                sql_size(size)?,
                chunk,
            ))?;
        }

        Ok(())
    }

    /// Inserts an object version or replaces the `null` version.
    #[cfg(test)]
    pub(crate) fn put_object(
        transaction: &Transaction,
        kv: KeyValue,
    ) -> SqliteResult<ObjectWriteOutcome> {
        Self::put_object_with_chunk_size(transaction, kv, super::DEFAULT_OBJECT_BLOB_CHUNK_SIZE)
    }

    /// Inserts an object version using the requested body chunk size.
    pub(crate) fn put_object_with_chunk_size(
        transaction: &Transaction,
        kv: KeyValue,
        chunk_size: usize,
    ) -> SqliteResult<ObjectWriteOutcome> {
        let KeyValue {
            key,
            value,
            key_metadata,
            ..
        } = kv;
        let value = value.ok_or(SqliteError::ObjectBodyInconsistent)?;
        let expected_size =
            usize::try_from(key_metadata.size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        if value.len() != expected_size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }

        let write_outcome = Self::put_object_version(transaction, &key, key_metadata)?;
        Self::put_object_storage_part(
            transaction,
            write_outcome.sequence,
            write_outcome.last_modified,
            i64::try_from(value.len()).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
        )?;
        Self::put_object_blob_chunks(
            transaction,
            Some(write_outcome.sequence),
            None,
            1,
            &value,
            0,
            chunk_size,
        )?;
        Ok(write_outcome)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use time::OffsetDateTime;

    use crate::{
        checksum::{FullObjectChecksum, StoredChecksum},
        sqlite::{KeyMetadata, KeyValue, Sqlite, SqliteError, SqliteResult},
    };

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    fn key_value(key: &str, value: &[u8]) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(value.to_vec()),
            key_metadata: KeyMetadata {
                version_id: String::new(),
                size: i64::try_from(value.len())
                    .unwrap_or_else(|err| panic!("test body should fit i64: {err:?}")),
                metadata: None,
                content_type: None,
                content_encoding: None,
                cache_control: None,
                expires: None,
                last_modified: OffsetDateTime::UNIX_EPOCH,
                md5: Some("md5".to_string()),
                checksum: Some(StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                    "crc".to_string(),
                ))),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    #[test]
    fn put_object_with_small_chunk_size_splits_body() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;

        let outcome =
            Sqlite::put_object_with_chunk_size(&transaction, key_value("key", b"abcde"), 2)?;
        let chunks = {
            let mut stmt = transaction.prepare(
                "
                SELECT offset, size, body
                FROM object_blob_chunk
                WHERE object_sequence = ?1
                ORDER BY offset;",
            )?;
            stmt.query_map([outcome.sequence], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };

        assert_eq!(
            chunks,
            vec![
                (0, 2, b"ab".to_vec()),
                (2, 2, b"cd".to_vec()),
                (4, 1, b"e".to_vec()),
            ]
        );
        let object = Sqlite::get_object(&transaction, "key")?.ok_or(SqliteError::ObjectNotFound)?;
        assert_eq!(object.value.as_deref(), Some(b"abcde".as_slice()));
        Ok(())
    }
}
