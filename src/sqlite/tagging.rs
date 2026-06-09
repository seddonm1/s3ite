//! `SQLite` S3 tag persistence helpers.

use rusqlite::{OptionalExtension, Transaction};
use uuid::Uuid;

use super::{ObjectTaggingTarget, Sqlite, SqliteResult, StoredTag};

impl Sqlite {
    /// Reads bucket tags in deterministic key order.
    pub(crate) fn get_bucket_tags(transaction: &Transaction) -> SqliteResult<Vec<StoredTag>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT key, value
            FROM bucket_tag
            ORDER BY key;",
        )?;

        Ok(stmt
            .query_map((), |row| {
                Ok(StoredTag {
                    key: row.get(0)?,
                    value: row.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Replaces all bucket tags.
    pub(crate) fn replace_bucket_tags(
        transaction: &Transaction,
        tags: &[StoredTag],
    ) -> SqliteResult<()> {
        transaction.execute("DELETE FROM bucket_tag;", ())?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO bucket_tag (key, value)
            VALUES (?1, ?2);",
        )?;
        for tag in tags {
            stmt.execute((tag.key.as_str(), tag.value.as_str()))?;
        }
        Ok(())
    }

    /// Resolves the current live object version used by object tagging APIs.
    pub(crate) fn get_current_object_tagging_target(
        transaction: &Transaction,
        key: &str,
    ) -> SqliteResult<Option<ObjectTaggingTarget>> {
        Self::get_object_tagging_target(transaction, key, None)
    }

    /// Resolves an exact live object version used by object tagging APIs.
    pub(crate) fn get_object_version_tagging_target(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<Option<ObjectTaggingTarget>> {
        Self::get_object_tagging_target(transaction, key, Some(version_id))
    }

    /// Reads tags for an object version in deterministic key order.
    pub(crate) fn get_object_tags(
        transaction: &Transaction,
        object_sequence: i64,
    ) -> SqliteResult<Vec<StoredTag>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT key, value
            FROM object_tag
            WHERE object_sequence = ?1
            ORDER BY key;",
        )?;

        Ok(stmt
            .query_map((object_sequence,), |row| {
                Ok(StoredTag {
                    key: row.get(0)?,
                    value: row.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Replaces all tags for an object version.
    pub(crate) fn replace_object_tags(
        transaction: &Transaction,
        object_sequence: i64,
        tags: &[StoredTag],
    ) -> SqliteResult<()> {
        let mut delete = transaction.prepare_cached(
            "
            DELETE FROM object_tag
            WHERE object_sequence = ?1;",
        )?;
        delete.execute((object_sequence,))?;
        drop(delete);

        Self::insert_object_tags(transaction, object_sequence, tags)
    }

    /// Stores tags captured when a multipart upload is initiated.
    pub(crate) fn put_multipart_upload_tags(
        transaction: &Transaction,
        upload_id: Uuid,
        tags: &[StoredTag],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload_tag (upload_id, key, value)
            VALUES (?1, ?2, ?3);",
        )?;
        for tag in tags {
            stmt.execute((upload_id, tag.key.as_str(), tag.value.as_str()))?;
        }
        Ok(())
    }

    /// Copies pending multipart tags onto a completed object version.
    pub(crate) fn copy_multipart_upload_tags_to_object(
        transaction: &Transaction,
        upload_id: Uuid,
        object_sequence: i64,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_tag (object_sequence, key, value)
            SELECT ?2, key, value
            FROM multipart_upload_tag
            WHERE upload_id = ?1
            ORDER BY key;",
        )?;
        Ok(stmt.execute((upload_id, object_sequence))?)
    }

    fn get_object_tagging_target(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
    ) -> SqliteResult<Option<ObjectTaggingTarget>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT sequence, version_id
            FROM object_version
            WHERE key = ?1
                AND (?2 IS NULL OR version_id = ?2)
                AND (?2 IS NOT NULL OR is_latest = 1)
                AND is_delete_marker = 0;",
        )?;

        Ok(stmt
            .query_row((key, version_id), |row| {
                Ok(ObjectTaggingTarget {
                    sequence: row.get(0)?,
                    version_id: row.get(1)?,
                })
            })
            .optional()?)
    }

    fn insert_object_tags(
        transaction: &Transaction,
        object_sequence: i64,
        tags: &[StoredTag],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_tag (object_sequence, key, value)
            VALUES (?1, ?2, ?3);",
        )?;
        for tag in tags {
            stmt.execute((object_sequence, tag.key.as_str(), tag.value.as_str()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::{
        s3::{FullObjectChecksum, StoredChecksum, StoredObjectLock, StoredTag},
        sqlite::{
            KeyMetadata, KeyValue, MultipartUploadState, NULL_VERSION_ID, Sqlite, SqliteResult,
        },
    };

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    fn tags() -> Vec<StoredTag> {
        vec![
            StoredTag {
                key: "b".to_string(),
                value: "two".to_string(),
            },
            StoredTag {
                key: "a".to_string(),
                value: "one".to_string(),
            },
        ]
    }

    fn key_value(key: &str, value: &[u8]) -> KeyValue {
        let Ok(size) = i64::try_from(value.len()) else {
            panic!("test object fits i64");
        };
        KeyValue {
            key: key.to_string(),
            value: Some(value.to_vec()),
            key_metadata: KeyMetadata {
                version_id: String::new(),
                size,
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
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    #[test]
    fn bucket_tags_replace_and_read_in_key_order() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;

        Sqlite::replace_bucket_tags(&transaction, &tags())?;
        assert_eq!(
            Sqlite::get_bucket_tags(&transaction)?,
            vec![
                StoredTag {
                    key: "a".to_string(),
                    value: "one".to_string(),
                },
                StoredTag {
                    key: "b".to_string(),
                    value: "two".to_string(),
                },
            ]
        );

        Sqlite::replace_bucket_tags(&transaction, &[])?;
        assert!(Sqlite::get_bucket_tags(&transaction)?.is_empty());
        Ok(())
    }

    #[test]
    fn object_tags_replace_and_read_in_key_order() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let outcome = Sqlite::put_object(&transaction, key_value("key", b"value"))?;

        Sqlite::replace_object_tags(&transaction, outcome.sequence, &tags())?;
        assert_eq!(
            Sqlite::get_object_tags(&transaction, outcome.sequence)?,
            vec![
                StoredTag {
                    key: "a".to_string(),
                    value: "one".to_string(),
                },
                StoredTag {
                    key: "b".to_string(),
                    value: "two".to_string(),
                },
            ]
        );

        Sqlite::replace_object_tags(&transaction, outcome.sequence, &[])?;
        assert!(Sqlite::get_object_tags(&transaction, outcome.sequence)?.is_empty());
        Ok(())
    }

    #[test]
    fn object_tags_cascade_with_object_version() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let outcome = Sqlite::put_object(&transaction, key_value("key", b"value"))?;
        Sqlite::replace_object_tags(&transaction, outcome.sequence, &tags())?;

        transaction.execute(
            "DELETE FROM object_version WHERE sequence = ?1;",
            (outcome.sequence,),
        )?;

        assert!(Sqlite::get_object_tags(&transaction, outcome.sequence)?.is_empty());
        Ok(())
    }

    #[test]
    fn multipart_tags_copy_to_completed_object_and_cascade() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
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
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
            },
        )?;
        Sqlite::put_multipart_upload_tags(&transaction, upload_id, &tags())?;
        let outcome = Sqlite::put_object(&transaction, key_value("key", b"value"))?;

        Sqlite::copy_multipart_upload_tags_to_object(&transaction, upload_id, outcome.sequence)?;
        Sqlite::delete_multipart(&transaction, upload_id)?;

        assert_eq!(
            Sqlite::get_object_tags(&transaction, outcome.sequence)?.len(),
            2
        );
        let pending: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM multipart_upload_tag WHERE upload_id = ?1;",
            (upload_id,),
            |row| row.get(0),
        )?;
        assert_eq!(pending, 0);
        assert_eq!(outcome.version_id, NULL_VERSION_ID);
        Ok(())
    }
}
