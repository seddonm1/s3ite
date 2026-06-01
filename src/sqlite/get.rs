use std::ops::Not;

use rusqlite::{OptionalExtension, Transaction};

use super::{
    KeyMetadata, KeyValue, ObjectBodyReference, Sqlite, SqliteError, SqliteResult,
    optional_stored_checksum, row_bool, row_size,
};

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
}
