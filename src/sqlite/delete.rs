use rusqlite::Transaction;
use time::OffsetDateTime;

use super::{BucketVersioningState, NULL_VERSION_ID, ObjectDeleteOutcome, Sqlite, SqliteResult};

impl Sqlite {
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
