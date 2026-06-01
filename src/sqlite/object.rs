use std::ops::Not;

use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction};
use time::OffsetDateTime;

use super::{
    BucketVersioningState, KeyMetadata, KeyValue, NULL_VERSION_ID, ObjectBodyReference,
    ObjectDeleteOutcome, ObjectTagCondition, ObjectWriteConditions, ObjectWriteOutcome, Sqlite,
    SqliteError, SqliteResult, optional_stored_checksum, row_bool, row_size, sql_size,
};

const VERSION_ID_LEN: usize = 31;
const VERSION_ID_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";

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
                size,
                metadata,
                content_type,
                content_encoding,
                cache_control,
                expires,
                last_modified,
                md5
            )
            VALUES (?1, ?2, 1, 1, 0, NULL, NULL, NULL, NULL, NULL, ?3, NULL);",
        )?;
        Ok(stmt.execute((key, version_id, OffsetDateTime::now_utc()))?)
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
                ov.key,
                ov.version_id,
                ob.body,
                ov.size,
                ov.metadata,
                ov.content_type,
                ov.content_encoding,
                ov.cache_control,
                ov.expires,
                ov.last_modified,
                ov.md5,
                ov.checksum_algorithm,
                ov.checksum_type,
                ov.checksum_value,
                ov.is_latest,
                ov.is_delete_marker
            FROM object_version AS ov
            LEFT JOIN object_body AS ob ON ob.sequence = ov.sequence
            WHERE ov.key = ?1
                AND (?2 IS NULL OR ov.version_id = ?2)
                AND (?2 IS NOT NULL OR ov.is_latest = 1)
                AND (?3 OR ov.is_delete_marker = 0);",
        )?;

        let object = stmt
            .query_row((key, version_id, include_delete_markers), |row| {
                Ok(KeyValue {
                    key: row.get(0)?,
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
                        content_encoding: row.get(6)?,
                        cache_control: row.get(7)?,
                        expires: row.get(8)?,
                        last_modified: row.get(9)?,
                        md5: row.get(10)?,
                        checksum: optional_stored_checksum(
                            row.get(11)?,
                            row.get(12)?,
                            row.get(13)?,
                        )?,
                        is_latest: row_bool(row, 14)?,
                        is_delete_marker: row_bool(row, 15)?,
                    },
                })
            })
            .optional()?;

        if let Some(object) = object.as_ref()
            && object.key_metadata.is_delete_marker.not()
        {
            let value = object
                .value
                .as_ref()
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
            let value_size =
                i64::try_from(value.len()).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            if value_size != object.key_metadata.size {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
        }

        Ok(object)
    }

    /// Fetches the current object metadata and body rowid by key.
    pub(crate) fn try_get_current_object_body_reference(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<ObjectBodyReference>> {
        Self::try_get_object_body_reference_internal(transaction, key, None, true)
    }

    /// Fetches exact visible object metadata and body rowid by key and version ID.
    pub(crate) fn try_get_object_version_body_reference(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<ObjectBodyReference>> {
        Self::try_get_object_body_reference_internal(transaction, key, Some(version_id), false)
    }

    /// Fetches metadata and rowid for opening an object body BLOB.
    fn try_get_object_body_reference_internal(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
        include_delete_markers: bool,
    ) -> SqliteResult<Option<ObjectBodyReference>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                version_id,
                sequence,
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
                checksum_value,
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
                let version_id = row.get(0)?;
                Ok(ObjectBodyReference {
                    version_id,
                    sequence: row.get(1)?,
                    key_metadata: KeyMetadata {
                        version_id: row.get(0)?,
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
                        content_encoding: row.get(5)?,
                        cache_control: row.get(6)?,
                        expires: row.get(7)?,
                        last_modified: row.get(8)?,
                        md5: row.get(9)?,
                        checksum: optional_stored_checksum(
                            row.get(10)?,
                            row.get(11)?,
                            row.get(12)?,
                        )?,
                        is_latest: row_bool(row, 13)?,
                        is_delete_marker: row_bool(row, 14)?,
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

    /// Fetches current object metadata including delete markers.
    pub(crate) fn try_get_current_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        Self::try_get_metadata_internal(transaction, key, None, true)
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
                content_encoding,
                cache_control,
                expires,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value,
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
                    content_encoding: row.get(4)?,
                    cache_control: row.get(5)?,
                    expires: row.get(6)?,
                    last_modified: row.get(7)?,
                    md5: row.get(8)?,
                    checksum: optional_stored_checksum(row.get(9)?, row.get(10)?, row.get(11)?)?,
                    is_latest: row_bool(row, 12)?,
                    is_delete_marker: row_bool(row, 13)?,
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
                content_encoding,
                cache_control,
                expires,
                last_modified,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value,
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
                    content_encoding: row.get(4)?,
                    cache_control: row.get(5)?,
                    expires: row.get(6)?,
                    last_modified: row.get(7)?,
                    md5: row.get(8)?,
                    checksum: optional_stored_checksum(row.get(9)?, row.get(10)?, row.get(11)?)?,
                    is_latest: row_bool(row, 12)?,
                    is_delete_marker: row_bool(row, 13)?,
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
        let value = value.ok_or(SqliteError::ObjectBodyInconsistent)?;
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
}
