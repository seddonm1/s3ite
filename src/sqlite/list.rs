use super::{Not, ObjectSummary, Sqlite, SqliteResult, Transaction, row_bool, row_size, sql_limit};

#[derive(Debug)]
enum VersionMarker {
    Existing(i64),
    Deleted,
}

impl Sqlite {
    /// Lists object metadata ordered by key for keyset pagination.
    pub(crate) fn try_list_objects(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        limit: i64,
    ) -> SqliteResult<Vec<ObjectSummary>> {
        let limit = sql_limit(limit)?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let map_summary = |row: &rusqlite::Row<'_>| {
            Ok(ObjectSummary {
                key: row.get(0)?,
                version_id: row.get(1)?,
                size: row_size(row, 2)?,
                last_modified: row.get(3)?,
                md5: row.get(4)?,
                is_latest: true,
                is_delete_marker: false,
            })
        };

        let objects = match (prefix, after_key) {
            (Some(prefix), Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 AND key >= ?1 AND key > ?2 ORDER BY key LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 AND key >= ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((prefix, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 AND key > ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5 FROM object_version WHERE is_latest = 1 AND is_delete_marker = 0 ORDER BY key LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(objects)
    }

    /// Lists object versions ordered by key and newest version first.
    pub(crate) fn try_list_object_versions(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        after_version_id: Option<&str>,
        limit: i64,
    ) -> SqliteResult<Vec<ObjectSummary>> {
        let after_marker = match (after_key, after_version_id) {
            (Some(key), Some(version_id)) => Some(
                match Self::try_find_version_sequence(transaction, key, version_id)? {
                    Some(sequence) => VersionMarker::Existing(sequence),
                    None => VersionMarker::Deleted,
                },
            ),
            _ => None,
        };
        let limit = sql_limit(limit)?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let map_summary = |row: &rusqlite::Row<'_>| {
            Ok(ObjectSummary {
                key: row.get(0)?,
                version_id: row.get(1)?,
                size: row_size(row, 2)?,
                last_modified: row.get(3)?,
                md5: row.get(4)?,
                is_latest: row_bool(row, 5)?,
                is_delete_marker: row_bool(row, 6)?,
            })
        };

        let versions = match (prefix, after_key, after_marker) {
            (Some(prefix), Some(after_key), Some(VersionMarker::Existing(after_sequence))) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 AND (key > ?2 OR (key = ?2 AND sequence < ?3)) ORDER BY key, sequence DESC LIMIT ?4;",
                )?;
                stmt.query_map((prefix, after_key, after_sequence, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), Some(after_key), Some(VersionMarker::Deleted)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 AND key >= ?2 ORDER BY key, sequence DESC LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 AND key > ?2 ORDER BY key, sequence DESC LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 ORDER BY key, sequence DESC LIMIT ?2;",
                )?;
                stmt.query_map((prefix, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), Some(VersionMarker::Existing(after_sequence))) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key > ?1 OR (key = ?1 AND sequence < ?2) ORDER BY key, sequence DESC LIMIT ?3;",
                )?;
                stmt.query_map((after_key, after_sequence, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), Some(VersionMarker::Deleted)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key >= ?1 ORDER BY key, sequence DESC LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version WHERE key > ?1 ORDER BY key, sequence DESC LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, version_id, size, last_modified, md5, is_latest, is_delete_marker FROM object_version ORDER BY key, sequence DESC LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_summary)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{
        checksum::{FullObjectChecksum, StoredChecksum},
        sqlite::{BucketVersioningState, KeyMetadata, KeyValue, SqliteError},
    };

    use super::*;

    fn must<T, E: std::fmt::Debug>(result: Result<T, E>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => panic!("unexpected test setup error: {err:?}"),
        }
    }

    fn open_schema() -> Connection {
        let mut connection = must(Connection::open_in_memory());
        must(connection.execute_batch("PRAGMA foreign_keys=true;"));
        let transaction = must(connection.transaction());
        must(Sqlite::try_create_tables(&transaction));
        must(transaction.commit());
        connection
    }

    fn key_value(key: &str, value: &[u8]) -> KeyValue {
        let size = match i64::try_from(value.len()) {
            Ok(size) => size,
            Err(err) => panic!("test object is too large for SQLite size: {err:?}"),
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
                last_modified: time::OffsetDateTime::UNIX_EPOCH,
                md5: Some(format!("md5-{key}")),
                checksum: Some(StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                    format!("crc64-{key}"),
                ))),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    fn put_object(transaction: &Transaction, key: &str, value: &[u8]) -> String {
        must(Sqlite::try_put_object(transaction, key_value(key, value))).version_id
    }

    fn create_loose_object_version_table(connection: &Connection) {
        must(connection.execute_batch(
            "
            CREATE TABLE object_version (
                sequence INTEGER,
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                last_modified,
                md5
            );
            ",
        ));
    }

    fn assert_rusqlite_error<T>(result: &SqliteResult<T>) {
        assert!(matches!(result, Err(SqliteError::Rusqlite(_))));
    }

    #[test]
    fn lists_current_live_objects_with_keyset_bounds() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::try_put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        put_object(&transaction, "alpha", b"a");
        let beta_v1 = put_object(&transaction, "beta", b"old");
        let beta_v2 = put_object(&transaction, "beta", b"new");
        assert_ne!(beta_v1, beta_v2);
        must(Sqlite::try_insert_delete_marker(
            &transaction,
            "deleted",
            "deleted-marker",
        ));
        put_object(&transaction, "gamma", b"g");

        let all = must(Sqlite::try_list_objects(&transaction, None, None, 10));
        assert_eq!(
            all.iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
        assert_eq!(all[1].version_id, beta_v2);
        assert_eq!(all[1].size, 3);
        assert!(all[1].is_latest);
        assert!(all[1].is_delete_marker.not());

        let page = must(Sqlite::try_list_objects(
            &transaction,
            None,
            Some("alpha"),
            1,
        ));
        assert_eq!(
            page.iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["beta"]
        );

        let prefix_floor = must(Sqlite::try_list_objects(
            &transaction,
            Some("beta"),
            None,
            2,
        ));
        assert_eq!(
            prefix_floor
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["beta", "gamma"]
        );

        let prefix_after = must(Sqlite::try_list_objects(
            &transaction,
            Some("alpha"),
            Some("beta"),
            2,
        ));
        assert_eq!(
            prefix_after
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["gamma"]
        );

        assert!(matches!(
            Sqlite::try_list_objects(&transaction, None, None, -1),
            Err(SqliteError::Rusqlite(_))
        ));
    }

    #[test]
    fn lists_versions_in_key_and_newest_first_order() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::try_put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        let alpha_v1 = put_object(&transaction, "alpha", b"one");
        let alpha_v2 = put_object(&transaction, "alpha", b"two");
        must(Sqlite::try_insert_delete_marker(
            &transaction,
            "alpha",
            "alpha-delete",
        ));
        let beta_v1 = put_object(&transaction, "beta", b"one");

        let all = must(Sqlite::try_list_object_versions(
            &transaction,
            None,
            None,
            None,
            10,
        ));
        assert_eq!(
            all.iter()
                .map(|summary| (
                    summary.key.as_str(),
                    summary.version_id.as_str(),
                    summary.is_latest,
                    summary.is_delete_marker,
                ))
                .collect::<Vec<_>>(),
            vec![
                ("alpha", "alpha-delete", true, true),
                ("alpha", alpha_v2.as_str(), false, false),
                ("alpha", alpha_v1.as_str(), false, false),
                ("beta", beta_v1.as_str(), true, false),
            ]
        );

        let after_delete = Sqlite::try_list_object_versions(
            &transaction,
            None,
            Some("alpha"),
            Some("alpha-delete"),
            10,
        )
        .ok();
        assert_eq!(
            after_delete.as_ref().map(|versions| versions
                .iter()
                .map(|summary| summary.version_id.as_str())
                .collect::<Vec<_>>()),
            Some(vec![alpha_v2.as_str(), alpha_v1.as_str(), beta_v1.as_str()])
        );

        let after_middle = Sqlite::try_list_object_versions(
            &transaction,
            None,
            Some("alpha"),
            Some(&alpha_v2),
            10,
        )
        .ok();
        assert_eq!(
            after_middle.as_ref().map(|versions| versions
                .iter()
                .map(|summary| summary.version_id.as_str())
                .collect::<Vec<_>>()),
            Some(vec![alpha_v1.as_str(), beta_v1.as_str()])
        );
    }

    #[test]
    fn treats_missing_version_marker_as_deleted_marker_position() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::try_put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        let alpha_v1 = put_object(&transaction, "alpha", b"one");
        let alpha_v2 = put_object(&transaction, "alpha", b"two");
        let beta_v1 = put_object(&transaction, "beta", b"one");

        let after_deleted_marker = Sqlite::try_list_object_versions(
            &transaction,
            None,
            Some("alpha"),
            Some("deleted-marker-no-longer-present"),
            10,
        )
        .ok();
        assert_eq!(
            after_deleted_marker.as_ref().map(|versions| versions
                .iter()
                .map(|summary| summary.version_id.as_str())
                .collect::<Vec<_>>()),
            Some(vec![alpha_v2.as_str(), alpha_v1.as_str(), beta_v1.as_str()])
        );

        let after_key_only = must(Sqlite::try_list_object_versions(
            &transaction,
            None,
            Some("alpha"),
            None,
            10,
        ));
        assert_eq!(
            after_key_only
                .iter()
                .map(|summary| summary.version_id.as_str())
                .collect::<Vec<_>>(),
            vec![beta_v1.as_str()]
        );

        let prefix_after_key_only = must(Sqlite::try_list_object_versions(
            &transaction,
            Some("alpha"),
            Some("alpha"),
            None,
            10,
        ));
        assert_eq!(
            prefix_after_key_only
                .iter()
                .map(|summary| summary.version_id.as_str())
                .collect::<Vec<_>>(),
            vec![beta_v1.as_str()]
        );
    }

    #[test]
    fn list_queries_report_missing_schema_for_marker_shapes() {
        let mut connection = must(Connection::open_in_memory());
        let transaction = must(connection.transaction());

        for result in [
            Sqlite::try_list_objects(&transaction, Some("a"), Some("b"), 1),
            Sqlite::try_list_objects(&transaction, Some("a"), None, 1),
            Sqlite::try_list_objects(&transaction, None, Some("b"), 1),
            Sqlite::try_list_objects(&transaction, None, None, 1),
        ] {
            assert_rusqlite_error(&result);
        }

        for result in [
            Sqlite::try_list_object_versions(&transaction, Some("a"), Some("b"), None, 1),
            Sqlite::try_list_object_versions(&transaction, Some("a"), None, None, 1),
            Sqlite::try_list_object_versions(&transaction, None, Some("b"), None, 1),
            Sqlite::try_list_object_versions(&transaction, None, None, None, 1),
            Sqlite::try_list_object_versions(&transaction, None, Some("b"), Some("v"), 1),
        ] {
            assert_rusqlite_error(&result);
        }
    }

    #[test]
    fn list_objects_rejects_negative_row_size() {
        let mut connection = must(Connection::open_in_memory());
        create_loose_object_version_table(&connection);
        must(connection.execute(
            "
            INSERT INTO object_version (
                sequence,
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                last_modified,
                md5
            )
            VALUES (1, 'key', 'version', 1, 0, -1, ?1, 'md5');
            ",
            [time::OffsetDateTime::UNIX_EPOCH],
        ));

        let transaction = must(connection.transaction());
        assert_rusqlite_error(&Sqlite::try_list_objects(&transaction, None, None, 1));
    }

    #[test]
    fn list_versions_rejects_malformed_rows_and_limits() {
        let mut connection = must(Connection::open_in_memory());
        create_loose_object_version_table(&connection);
        must(connection.execute(
            "
            INSERT INTO object_version (
                sequence,
                key,
                version_id,
                is_latest,
                is_delete_marker,
                size,
                last_modified,
                md5
            )
            VALUES (1, 'key', 'version', 'latest', 0, 4, ?1, 'md5');
            ",
            [time::OffsetDateTime::UNIX_EPOCH],
        ));

        let transaction = must(connection.transaction());
        assert_rusqlite_error(&Sqlite::try_list_object_versions(
            &transaction,
            None,
            None,
            None,
            1,
        ));
        assert_rusqlite_error(&Sqlite::try_list_object_versions(
            &transaction,
            None,
            None,
            None,
            -1,
        ));
    }
}
