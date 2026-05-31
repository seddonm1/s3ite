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
