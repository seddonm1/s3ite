//! `SQLite` object read helpers.

#[cfg(test)]
use std::ops::Not;
use std::ops::Range as ByteRange;

use rusqlite::{OptionalExtension, Transaction};

use crate::database::BlobReadRequest;

#[cfg(test)]
use super::KeyValue;
use super::{
    KeyMetadata, ObjectBodyReference, Sqlite, SqliteError, SqliteResult, StoredBlobChunk,
    optional_stored_checksum, optional_stored_object_lock, optional_stored_sse, row_bool, row_size,
};

impl Sqlite {
    /// Returns the storage sequence for an exact object version when present.
    pub(super) fn find_version_sequence(
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
    pub(crate) fn has_objects(transaction: &Transaction) -> SqliteResult<bool> {
        let mut stmt =
            transaction.prepare_cached("SELECT EXISTS(SELECT 1 FROM object_version);")?;
        Ok(stmt.query_row((), |row| row.get(0))?)
    }

    /// Fetches the current visible object body and metadata by key.
    #[cfg(test)]
    pub(crate) fn get_object(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyValue>> {
        Self::get_object_internal(transaction, key, None, false)
    }

    /// Fetches an object body and metadata by current or exact version selector.
    #[cfg(test)]
    fn get_object_internal(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
        include_delete_markers: bool,
    ) -> SqliteResult<Option<KeyValue>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                ov.sequence,
                ov.key,
                ov.version_id,
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
                ov.sse_algorithm,
                ov.sse_key_nonce,
                ov.sse_key_ciphertext,
                ov.sse_key_tag,
                ov.object_lock_legal_hold,
                ov.object_lock_retention_mode,
                ov.object_lock_retain_until,
                ov.is_latest,
                ov.is_delete_marker
            FROM object_version AS ov
            WHERE ov.key = ?1
                AND (?2 IS NULL OR ov.version_id = ?2)
                AND (?2 IS NOT NULL OR ov.is_latest = 1)
                AND (?3 OR ov.is_delete_marker = 0);",
        )?;

        let object = stmt
            .query_row((key, version_id, include_delete_markers), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    KeyValue {
                        key: row.get(1)?,
                        value: None,
                        key_metadata: KeyMetadata {
                            version_id: row.get(2)?,
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
                            stored_sse: optional_stored_sse(
                                row.get(14)?,
                                row.get(15)?,
                                row.get(16)?,
                                row.get(17)?,
                            )?,
                            object_lock: optional_stored_object_lock(
                                row.get(18)?,
                                row.get(19)?,
                                row.get(20)?,
                            )?,
                            is_latest: row_bool(row, 21)?,
                            is_delete_marker: row_bool(row, 22)?,
                        },
                    },
                ))
            })
            .optional()?;

        let Some((sequence, mut object)) = object else {
            return Ok(None);
        };

        if object.key_metadata.is_delete_marker.not() {
            object.value = Some(Self::get_object_bytes(
                transaction,
                sequence,
                object.key_metadata.size,
            )?);
        }

        Ok(Some(object))
    }

    /// Reassembles object bytes from stored chunks for copy-style operations.
    pub(crate) fn get_object_bytes(
        transaction: &Transaction,
        object_sequence: i64,
        expected_size: i64,
    ) -> SqliteResult<Vec<u8>> {
        let chunks = Self::get_object_blob_chunks(transaction, object_sequence, expected_size)?;
        let expected_size =
            usize::try_from(expected_size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        let mut value = Vec::with_capacity(expected_size);
        for chunk in chunks {
            if chunk.sse_nonce.is_some() || chunk.sse_tag.is_some() {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            value.extend_from_slice(&chunk.body);
        }
        if value.len() != expected_size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        Ok(value)
    }

    /// Reads stored object chunks without decrypting them.
    pub(crate) fn get_object_blob_chunks(
        transaction: &Transaction,
        object_sequence: i64,
        expected_size: i64,
    ) -> SqliteResult<Vec<StoredBlobChunk>> {
        let expected_size =
            usize::try_from(expected_size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                offset,
                size,
                body,
                sse_nonce,
                sse_tag
            FROM object_blob_chunk
            WHERE object_sequence = ?1
            ORDER BY offset;",
        )?;
        let mut rows = stmt.query([object_sequence])?;
        let mut chunks = Vec::new();
        let mut expected_offset = 0usize;

        while let Some(row) = rows.next()? {
            let offset = usize::try_from(row_size(row, 0)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let size = usize::try_from(row_size(row, 1)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let body = row.get::<_, Vec<u8>>(2)?;
            let sse_nonce = row.get::<_, Option<Vec<u8>>>(3)?;
            let sse_tag = row.get::<_, Option<Vec<u8>>>(4)?;

            if offset != expected_offset {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            if body.len() != size {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            chunks.push(StoredBlobChunk {
                offset: i64::try_from(offset).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
                size: i64::try_from(size).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
                body,
                sse_nonce,
                sse_tag,
            });
            expected_offset = expected_offset
                .checked_add(size)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
        }

        if expected_offset != expected_size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }

        Ok(chunks)
    }

    /// Builds ordered `SQLite` BLOB read requests for an object byte range.
    pub(crate) fn object_blob_read_requests(
        transaction: &Transaction,
        object_sequence: i64,
        range: ByteRange<u64>,
    ) -> SqliteResult<Vec<BlobReadRequest>> {
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let start = i64::try_from(range.start).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        let end = i64::try_from(range.end).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                chunk_id,
                offset,
                size
            FROM object_blob_chunk
            WHERE object_sequence = ?1
                AND offset < ?3
                AND offset + size > ?2
            ORDER BY offset;",
        )?;
        let mut rows = stmt.query((object_sequence, start, end))?;
        let mut requests = Vec::new();
        let mut next = range.start;

        while let Some(row) = rows.next()? {
            let rowid = row.get(0)?;
            let offset = u64::try_from(row_size(row, 1)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let size = u64::try_from(row_size(row, 2)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let chunk_end = offset
                .checked_add(size)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
            if offset > next || chunk_end <= next {
                return Err(SqliteError::ObjectBodyInconsistent);
            }

            let read_start = next - offset;
            let read_end = range.end.min(chunk_end) - offset;
            let length = read_end
                .checked_sub(read_start)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
            requests.push(BlobReadRequest::new(
                "object_blob_chunk",
                "body",
                rowid,
                read_start,
                length,
                size,
            ));
            next = next
                .checked_add(length)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
            if next == range.end {
                break;
            }
        }

        if next != range.end {
            return Err(SqliteError::ObjectBodyInconsistent);
        }

        Ok(requests)
    }

    /// Fetches the current object metadata and body rowid by key.
    pub(crate) fn get_current_object_body_reference(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<ObjectBodyReference>> {
        Self::get_object_body_reference_internal(transaction, key, None, true)
    }

    /// Fetches exact visible object metadata and body rowid by key and version ID.
    pub(crate) fn get_object_version_body_reference(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<ObjectBodyReference>> {
        Self::get_object_body_reference_internal(transaction, key, Some(version_id), false)
    }

    /// Fetches metadata and rowid for opening an object body BLOB.
    fn get_object_body_reference_internal(
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
                sse_algorithm,
                sse_key_nonce,
                sse_key_ciphertext,
                sse_key_tag,
                object_lock_legal_hold,
                object_lock_retention_mode,
                object_lock_retain_until,
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
                        stored_sse: optional_stored_sse(
                            row.get(13)?,
                            row.get(14)?,
                            row.get(15)?,
                            row.get(16)?,
                        )?,
                        object_lock: optional_stored_object_lock(
                            row.get(17)?,
                            row.get(18)?,
                            row.get(19)?,
                        )?,
                        is_latest: row_bool(row, 20)?,
                        is_delete_marker: row_bool(row, 21)?,
                    },
                })
            })
            .optional()?)
    }

    /// Fetches current visible object metadata by key without reading the body.
    pub(crate) fn get_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        Self::get_metadata_internal(transaction, key, None, false)
    }

    /// Fetches current object metadata including delete markers.
    pub(crate) fn get_current_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        Self::get_metadata_internal(transaction, key, None, true)
    }

    /// Fetches exact object metadata, including delete markers, by key and version ID.
    pub(crate) fn get_version_metadata(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<KeyMetadata>> {
        Self::get_metadata_internal(transaction, key, Some(version_id), true)
    }

    /// Fetches metadata by current or exact version selector.
    fn get_metadata_internal(
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
                sse_algorithm,
                sse_key_nonce,
                sse_key_ciphertext,
                sse_key_tag,
                object_lock_legal_hold,
                object_lock_retention_mode,
                object_lock_retain_until,
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
                    stored_sse: optional_stored_sse(
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                    )?,
                    object_lock: optional_stored_object_lock(
                        row.get(16)?,
                        row.get(17)?,
                        row.get(18)?,
                    )?,
                    is_latest: row_bool(row, 19)?,
                    is_delete_marker: row_bool(row, 20)?,
                })
            })
            .optional()?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rusqlite::Connection;
    use time::OffsetDateTime;

    use crate::{
        s3::{FullObjectChecksum, StoredChecksum},
        sqlite::{Sqlite, SqliteError, SqliteResult},
    };

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    fn insert_object(
        connection: &Connection,
        key: &str,
        version_id: &str,
        size: i64,
        metadata: Option<&str>,
        body: Option<&[u8]>,
    ) -> SqliteResult<i64> {
        connection.execute(
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
            VALUES (?1, ?2, 1, 0, ?3, ?4, 'text/plain', 'gzip', 'max-age=1', ?5, ?6, 'md5', 3, 2, 'crc');",
            (
                key,
                version_id,
                size,
                metadata,
                OffsetDateTime::UNIX_EPOCH,
                OffsetDateTime::UNIX_EPOCH,
            ),
        )?;
        let sequence = connection.last_insert_rowid();
        if let Some(body) = body {
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
                VALUES (?1, NULL, 0, ?2, 1, 0, ?3);",
                (
                    sequence,
                    OffsetDateTime::UNIX_EPOCH,
                    i64::try_from(body.len()).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
                ),
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
                VALUES (?1, NULL, 1, 1, 0, ?2, ?3);",
                (
                    sequence,
                    i64::try_from(body.len()).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
                    body,
                ),
            )?;
        }
        Ok(sequence)
    }

    fn assert_conversion_error<T>(result: &SqliteResult<T>) {
        assert!(matches!(
            result,
            Err(SqliteError::Rusqlite(
                rusqlite::Error::FromSqlConversionFailure(..)
            ))
        ));
    }

    #[test]
    fn reads_current_object_metadata_body_and_blob_reference() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        insert_object(
            &connection,
            "key",
            "version",
            4,
            Some(r#"{"color":"blue"}"#),
            Some(b"body"),
        )?;
        let transaction = connection.transaction()?;

        let expected_metadata = {
            let mut metadata = HashMap::new();
            metadata.insert("color".to_string(), "blue".to_string());
            metadata
        };
        let object = Sqlite::get_object(&transaction, "key")?;
        assert_eq!(
            object.as_ref().map(|object| object.value.as_deref()),
            Some(Some(&b"body"[..]))
        );
        assert_eq!(
            object
                .as_ref()
                .and_then(|object| object.key_metadata.metadata.as_ref()),
            Some(&expected_metadata)
        );
        assert_eq!(
            object
                .as_ref()
                .and_then(|object| object.key_metadata.content_type.as_deref()),
            Some("text/plain")
        );
        assert_eq!(
            object
                .as_ref()
                .and_then(|object| object.key_metadata.checksum.as_ref()),
            Some(&StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                "crc".to_string()
            )))
        );

        let reference = Sqlite::get_current_object_body_reference(&transaction, "key")?;
        assert_eq!(
            reference.as_ref().map(|reference| reference.sequence),
            Some(1)
        );
        assert_eq!(
            reference
                .as_ref()
                .and_then(|reference| reference.key_metadata.metadata.as_ref()),
            Some(&expected_metadata)
        );

        let metadata = Sqlite::get_metadata(&transaction, "key")?;
        assert_eq!(
            metadata
                .as_ref()
                .and_then(|metadata| metadata.metadata.as_ref()),
            Some(&expected_metadata)
        );

        Ok(())
    }

    #[test]
    fn live_object_without_body_is_inconsistent() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        insert_object(&connection, "key", "version", 4, None, None)?;
        let transaction = connection.transaction()?;

        assert!(matches!(
            Sqlite::get_object(&transaction, "key"),
            Err(SqliteError::ObjectBodyInconsistent)
        ));
        Ok(())
    }

    #[test]
    fn malformed_metadata_fails_all_read_shapes() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        insert_object(
            &connection,
            "key",
            "version",
            4,
            Some("{not-json"),
            Some(b"body"),
        )?;
        let transaction = connection.transaction()?;

        assert_conversion_error(&Sqlite::get_object(&transaction, "key"));
        assert_conversion_error(&Sqlite::get_current_object_body_reference(
            &transaction,
            "key",
        ));
        assert_conversion_error(&Sqlite::get_metadata(&transaction, "key"));
        Ok(())
    }
}
