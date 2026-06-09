//! `SQLite` Object Lock persistence helpers.

use rusqlite::{OptionalExtension, Transaction};

use super::{
    BucketVersioningState, ObjectVersionTarget, Sqlite, SqliteResult, StoredObjectLockConfig,
    StoredObjectLockDefaultRetention, StoredObjectLockMode, StoredObjectRetention,
};

impl Sqlite {
    /// Enables Object Lock for a newly created bucket and turns versioning on.
    pub(crate) fn enable_bucket_object_lock(transaction: &Transaction) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE bucket_state
            SET object_lock_enabled = 1,
                versioning_state = ?1
            WHERE id = 1;",
        )?;
        Ok(stmt.execute([BucketVersioningState::Enabled])?)
    }

    /// Reads the bucket's Object Lock configuration.
    pub(crate) fn get_bucket_object_lock_config(
        transaction: &Transaction,
    ) -> SqliteResult<StoredObjectLockConfig> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                object_lock_enabled,
                object_lock_default_retention_mode,
                object_lock_default_retention_days,
                object_lock_default_retention_years
            FROM bucket_state
            WHERE id = 1;",
        )?;
        Ok(stmt.query_row((), |row| {
            let enabled = row.get::<_, i64>(0)? != 0;
            let mode = row
                .get::<_, Option<String>>(1)?
                .map(|mode| {
                    StoredObjectLockMode::from_sql(&mode).ok_or(rusqlite::Error::InvalidQuery)
                })
                .transpose()?;
            let days = row.get(2)?;
            let years = row.get(3)?;
            let default_retention =
                mode.map(|mode| StoredObjectLockDefaultRetention { mode, days, years });
            Ok(StoredObjectLockConfig {
                enabled,
                default_retention,
            })
        })?)
    }

    /// Replaces the bucket's Object Lock default retention configuration.
    pub(crate) fn put_bucket_object_lock_config(
        transaction: &Transaction,
        config: &StoredObjectLockConfig,
    ) -> SqliteResult<usize> {
        let default = config.default_retention.as_ref();
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE bucket_state
            SET object_lock_enabled = ?1,
                object_lock_default_retention_mode = ?2,
                object_lock_default_retention_days = ?3,
                object_lock_default_retention_years = ?4
            WHERE id = 1;",
        )?;
        Ok(stmt.execute((
            config.enabled,
            default.map(|default| default.mode.as_str()),
            default.and_then(|default| default.days),
            default.and_then(|default| default.years),
        ))?)
    }

    /// Resolves a live object version target for Object Lock metadata updates.
    pub(crate) fn get_object_lock_target(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
    ) -> SqliteResult<Option<ObjectVersionTarget>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT sequence
            FROM object_version
            WHERE key = ?1
                AND (?2 IS NULL OR version_id = ?2)
                AND (?2 IS NOT NULL OR is_latest = 1)
                AND is_delete_marker = 0;",
        )?;
        Ok(stmt
            .query_row((key, version_id), |row| {
                Ok(ObjectVersionTarget {
                    sequence: row.get(0)?,
                })
            })
            .optional()?)
    }

    /// Updates one object version's legal hold status.
    pub(crate) fn put_object_legal_hold(
        transaction: &Transaction,
        object_sequence: i64,
        legal_hold: bool,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE object_version
            SET object_lock_legal_hold = ?2
            WHERE sequence = ?1;",
        )?;
        Ok(stmt.execute((object_sequence, legal_hold))?)
    }

    /// Updates one object version's retention metadata.
    pub(crate) fn put_object_retention(
        transaction: &Transaction,
        object_sequence: i64,
        retention: Option<&StoredObjectRetention>,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE object_version
            SET object_lock_retention_mode = ?2,
                object_lock_retain_until = ?3
            WHERE sequence = ?1;",
        )?;
        Ok(stmt.execute((
            object_sequence,
            retention.map(|retention| retention.mode.as_str()),
            retention.map(|retention| retention.retain_until),
        ))?)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use time::{Duration, OffsetDateTime};

    use crate::{
        s3::{FullObjectChecksum, StoredChecksum, StoredObjectLock},
        sqlite::{KeyMetadata, KeyValue, Sqlite, SqliteResult},
    };

    use super::*;

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
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    #[test]
    fn bucket_object_lock_config_round_trips() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        Sqlite::enable_bucket_object_lock(&transaction)?;
        let config = StoredObjectLockConfig {
            enabled: true,
            default_retention: Some(StoredObjectLockDefaultRetention {
                mode: StoredObjectLockMode::Governance,
                days: Some(3),
                years: None,
            }),
        };
        Sqlite::put_bucket_object_lock_config(&transaction, &config)?;
        assert_eq!(Sqlite::get_bucket_object_lock_config(&transaction)?, config);
        Ok(())
    }

    #[test]
    fn per_version_lock_metadata_round_trips() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let outcome = Sqlite::put_object(&transaction, key_value("key", b"value"))?;
        Sqlite::put_object_legal_hold(&transaction, outcome.sequence, true)?;
        let retention = StoredObjectRetention {
            mode: StoredObjectLockMode::Compliance,
            retain_until: OffsetDateTime::UNIX_EPOCH + Duration::days(7),
        };
        Sqlite::put_object_retention(&transaction, outcome.sequence, Some(&retention))?;
        let metadata = Sqlite::get_metadata(&transaction, "key")?
            .unwrap_or_else(|| panic!("metadata should exist"));
        assert_eq!(metadata.object_lock.legal_hold, Some(true));
        assert_eq!(metadata.object_lock.retention, Some(retention));
        Ok(())
    }
}
