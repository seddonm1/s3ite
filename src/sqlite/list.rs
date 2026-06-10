use super::{
    Not, ObjectSummary, ObjectVersionListEntry, ObjectVersionListPage, Sqlite, SqliteResult,
    Transaction, row_bool, row_size, sql_limit,
};

const MARKER_NONE: i64 = 0;
const MARKER_KEY_ONLY: i64 = 1;
const MARKER_EXISTING_VERSION: i64 = 2;
const MARKER_DELETED_VERSION: i64 = 3;
const OBJECT_ENTRY_KIND: i64 = 0;
const COMMON_PREFIX_ENTRY_KIND: i64 = 1;

impl ObjectVersionListEntry {
    fn marker(&self) -> (&str, &str) {
        match self {
            Self::Object(summary) => (&summary.key, &summary.version_id),
            Self::CommonPrefix {
                marker_key,
                marker_version_id,
                ..
            } => (marker_key, marker_version_id),
        }
    }
}

/// Builds a page from one more emitted entry than the client limit.
///
/// The extra entry only determines truncation. When the page is truncated, the
/// next markers come from the last retained emitted entry, whether that entry is
/// an object version or a collapsed common prefix.
fn object_version_list_page(
    mut entries: Vec<ObjectVersionListEntry>,
    limit: i64,
) -> ObjectVersionListPage {
    if limit == 0 {
        return ObjectVersionListPage {
            entries: Vec::new(),
            next_key_marker: None,
            next_version_id_marker: None,
            is_truncated: false,
        };
    }

    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let is_truncated = entries.len() > limit;
    if is_truncated {
        entries.truncate(limit);
    }
    let (next_key_marker, next_version_id_marker) = if is_truncated {
        entries.last().map_or((None, None), |entry| {
            let (key, version_id) = entry.marker();
            (Some(key.to_string()), Some(version_id.to_string()))
        })
    } else {
        (None, None)
    };

    ObjectVersionListPage {
        entries,
        next_key_marker,
        next_version_id_marker,
        is_truncated,
    }
}

/// Classifies version-list markers before applying them in SQL.
///
/// Key-only markers resume after the key. Exact version markers resume after
/// the matching `(key, sequence)`. If the version marker is no longer present,
/// it is treated as a deleted-marker position and resumes at `key >= marker`.
fn object_version_marker_state(
    transaction: &Transaction,
    after_key: Option<&str>,
    after_version_id: Option<&str>,
) -> SqliteResult<(i64, Option<i64>)> {
    let after_sequence = match (after_key, after_version_id) {
        (Some(key), Some(version_id)) => {
            Sqlite::find_version_sequence(transaction, key, version_id)?
        }
        _ => None,
    };
    let marker_kind = match (after_key, after_version_id, after_sequence) {
        (Some(_), Some(_), Some(_)) => MARKER_EXISTING_VERSION,
        (Some(_), Some(_), None) => MARKER_DELETED_VERSION,
        (Some(_), None, _) => MARKER_KEY_ONLY,
        (None, _, _) => MARKER_NONE,
    };

    Ok((marker_kind, after_sequence))
}

fn object_version_entry_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ObjectVersionListEntry> {
    let entry_kind: i64 = row.get(0)?;
    match entry_kind {
        OBJECT_ENTRY_KIND => Ok(ObjectVersionListEntry::Object(ObjectSummary {
            key: row.get(1)?,
            version_id: row.get(2)?,
            size: row_size(row, 3)?,
            last_modified: row.get(4)?,
            md5: row.get(5)?,
            is_latest: row_bool(row, 6)?,
            is_delete_marker: row_bool(row, 7)?,
        })),
        COMMON_PREFIX_ENTRY_KIND => Ok(ObjectVersionListEntry::CommonPrefix {
            prefix: row.get(8)?,
            marker_key: row.get(9)?,
            marker_version_id: row.get(10)?,
        }),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

/// Queries object-version list entries in storage order.
///
/// The CTE first applies prefix and marker filters, then derives delimiter
/// common prefixes. Object rows without a common prefix are emitted directly;
/// grouped prefixes emit one entry, using the first raw `(key, sequence DESC)`
/// row in that group as the source for pagination markers.
fn query_object_version_entries(
    transaction: &Transaction,
    prefix: &str,
    delimiter: &str,
    marker_kind: i64,
    after_key: Option<&str>,
    after_sequence: Option<i64>,
    fetch_limit: i64,
) -> SqliteResult<Vec<ObjectVersionListEntry>> {
    let mut stmt = transaction.prepare_cached(
        "
        WITH filtered AS (
            SELECT
                sequence, key, version_id, size, last_modified, md5,
                is_latest, is_delete_marker,
                CASE
                    WHEN ?2 = '' THEN NULL
                    WHEN instr(substr(key, length(?1) + 1), ?2) = 0 THEN NULL
                    ELSE substr(
                        key, 1,
                        length(?1) + instr(substr(key, length(?1) + 1), ?2) + length(?2) - 1
                    )
                END AS common_prefix
            FROM object_version
            WHERE
                (?1 = '' OR (key >= ?1 AND substr(key, 1, length(?1)) = ?1))
                AND (
                    ?3 = 0
                    OR (?3 = 1 AND key > ?4)
                    OR (?3 = 2 AND (key > ?4 OR (key = ?4 AND sequence < ?5)))
                    OR (?3 = 3 AND key >= ?4)
                )
        ),
        prefix_rows AS (
            SELECT
                *,
                row_number() OVER (PARTITION BY common_prefix ORDER BY key, sequence DESC)
                    AS prefix_rank
            FROM filtered
            WHERE common_prefix IS NOT NULL
        )
        SELECT
            entry_kind, key, version_id, size, last_modified, md5,
            is_latest, is_delete_marker, common_prefix, marker_key, marker_version_id
        FROM (
            SELECT
                0 AS entry_kind, key, version_id, size, last_modified, md5,
                is_latest, is_delete_marker,
                NULL AS common_prefix,
                key AS marker_key,
                version_id AS marker_version_id,
                key AS sort_key,
                sequence AS sort_sequence
            FROM filtered
            WHERE common_prefix IS NULL

            UNION ALL

            SELECT
                1 AS entry_kind,
                NULL AS key, NULL AS version_id, NULL AS size, NULL AS last_modified,
                NULL AS md5, NULL AS is_latest, NULL AS is_delete_marker,
                common_prefix,
                key AS marker_key,
                version_id AS marker_version_id,
                common_prefix AS sort_key,
                sequence AS sort_sequence
            FROM prefix_rows
            WHERE
                prefix_rank = 1
                AND (?4 IS NULL OR common_prefix > ?4)
        )
        ORDER BY sort_key, sort_sequence DESC
        LIMIT ?6;",
    )?;
    let rows = stmt.query_map(
        (
            prefix,
            delimiter,
            marker_kind,
            after_key,
            after_sequence,
            fetch_limit,
        ),
        object_version_entry_from_row,
    )?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

impl Sqlite {
    /// Lists object metadata ordered by key for keyset pagination.
    pub(crate) fn list_objects(
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

    /// Lists object versions and collapsed common prefixes as storage entries.
    ///
    /// This is the storage-level `ListObjectVersions` pager. It returns plain
    /// object summaries and common-prefix entries; S3 DTO conversion, owner
    /// fields, and URL encoding stay in the S3 layer.
    pub(crate) fn list_object_version_entries(
        transaction: &Transaction,
        prefix: &str,
        delimiter: Option<&str>,
        after_key: Option<&str>,
        after_version_id: Option<&str>,
        limit: i64,
    ) -> SqliteResult<ObjectVersionListPage> {
        let limit = sql_limit(limit)?;
        if limit == 0 {
            return Ok(object_version_list_page(Vec::new(), limit));
        }

        let (marker_kind, after_sequence) =
            object_version_marker_state(transaction, after_key, after_version_id)?;
        let delimiter = delimiter
            .filter(|delimiter| delimiter.is_empty().not())
            .unwrap_or("");
        let entries = query_object_version_entries(
            transaction,
            prefix,
            delimiter,
            marker_kind,
            after_key,
            after_sequence,
            limit.saturating_add(1),
        )?;

        Ok(object_version_list_page(entries, limit))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{
        s3::{FullObjectChecksum, StoredChecksum, StoredObjectLock},
        sqlite::{
            BucketVersioningState, KeyMetadata, KeyValue, SqliteError,
            put::test_support as put_test_support,
        },
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
        must(Sqlite::create_tables(&transaction));
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
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    fn put_object(transaction: &Transaction, key: &str, value: &[u8]) -> String {
        must(put_test_support::put_object(
            transaction,
            key_value(key, value),
        ))
        .version_id
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

    fn object_entry(entry: &ObjectVersionListEntry) -> (&str, &str, bool) {
        match entry {
            ObjectVersionListEntry::Object(summary) => (
                summary.key.as_str(),
                summary.version_id.as_str(),
                summary.is_delete_marker,
            ),
            ObjectVersionListEntry::CommonPrefix { .. } => {
                panic!("expected object version entry")
            }
        }
    }

    fn common_prefix_entry(entry: &ObjectVersionListEntry) -> (&str, &str, &str) {
        match entry {
            ObjectVersionListEntry::CommonPrefix {
                prefix,
                marker_key,
                marker_version_id,
            } => (
                prefix.as_str(),
                marker_key.as_str(),
                marker_version_id.as_str(),
            ),
            ObjectVersionListEntry::Object(_) => panic!("expected common prefix entry"),
        }
    }

    #[test]
    fn lists_current_live_objects_with_keyset_bounds() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        put_object(&transaction, "alpha", b"a");
        let beta_v1 = put_object(&transaction, "beta", b"old");
        let beta_v2 = put_object(&transaction, "beta", b"new");
        assert_ne!(beta_v1, beta_v2);
        must(Sqlite::insert_delete_marker(
            &transaction,
            "deleted",
            "deleted-marker",
        ));
        put_object(&transaction, "gamma", b"g");

        let all = must(Sqlite::list_objects(&transaction, None, None, 10));
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

        let page = must(Sqlite::list_objects(&transaction, None, Some("alpha"), 1));
        assert_eq!(
            page.iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["beta"]
        );

        let prefix_floor = must(Sqlite::list_objects(&transaction, Some("beta"), None, 2));
        assert_eq!(
            prefix_floor
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["beta", "gamma"]
        );

        let prefix_after = must(Sqlite::list_objects(
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
            Sqlite::list_objects(&transaction, None, None, -1),
            Err(SqliteError::Rusqlite(_))
        ));
    }

    #[test]
    fn lists_version_entries_with_page_markers() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        let alpha_v1 = put_object(&transaction, "alpha", b"one");
        let alpha_v2 = put_object(&transaction, "alpha", b"two");
        must(Sqlite::insert_delete_marker(
            &transaction,
            "alpha",
            "alpha-delete",
        ));
        let beta_v1 = put_object(&transaction, "beta", b"one");

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "",
            None,
            None,
            None,
            2,
        ));
        assert!(page.is_truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("alpha"));
        assert_eq!(
            page.next_version_id_marker.as_deref(),
            Some(alpha_v2.as_str())
        );
        assert_eq!(
            page.entries.iter().map(object_entry).collect::<Vec<_>>(),
            vec![
                ("alpha", "alpha-delete", true),
                ("alpha", alpha_v2.as_str(), false),
            ]
        );

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "",
            None,
            page.next_key_marker.as_deref(),
            page.next_version_id_marker.as_deref(),
            10,
        ));
        assert!(page.is_truncated.not());
        assert_eq!(
            page.entries.iter().map(object_entry).collect::<Vec<_>>(),
            vec![
                ("alpha", alpha_v1.as_str(), false),
                ("beta", beta_v1.as_str(), false),
            ]
        );
    }

    #[test]
    fn list_version_entries_collapse_common_prefixes() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        put_object(&transaction, "version/dir/a", b"one");
        put_object(&transaction, "version/dir/a", b"two");
        let dir_latest = put_object(&transaction, "version/dir/a", b"three");
        put_object(&transaction, "version/dir/b", b"one");
        let file = put_object(&transaction, "version/file", b"one");

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "version/",
            Some("/"),
            None,
            None,
            1,
        ));
        assert!(page.is_truncated);
        assert_eq!(
            page.entries
                .iter()
                .map(common_prefix_entry)
                .collect::<Vec<_>>(),
            vec![("version/dir/", "version/dir/a", dir_latest.as_str())]
        );
        assert_eq!(page.next_key_marker.as_deref(), Some("version/dir/a"));
        assert_eq!(
            page.next_version_id_marker.as_deref(),
            Some(dir_latest.as_str())
        );

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "version/",
            Some("/"),
            page.next_key_marker.as_deref(),
            page.next_version_id_marker.as_deref(),
            10,
        ));
        assert!(page.is_truncated.not());
        assert_eq!(
            page.entries.iter().map(object_entry).collect::<Vec<_>>(),
            vec![("version/file", file.as_str(), false)]
        );
    }

    #[test]
    fn list_version_entries_treat_missing_marker_as_deleted_position() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        must(Sqlite::put_bucket_versioning_state(
            &transaction,
            BucketVersioningState::Enabled,
        ));

        let alpha_v1 = put_object(&transaction, "alpha", b"one");
        let alpha_v2 = put_object(&transaction, "alpha", b"two");
        let beta_v1 = put_object(&transaction, "beta", b"one");

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "",
            None,
            Some("alpha"),
            Some("deleted-marker-no-longer-present"),
            10,
        ));
        assert_eq!(
            page.entries.iter().map(object_entry).collect::<Vec<_>>(),
            vec![
                ("alpha", alpha_v2.as_str(), false),
                ("alpha", alpha_v1.as_str(), false),
                ("beta", beta_v1.as_str(), false),
            ]
        );

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "",
            None,
            Some("alpha"),
            None,
            10,
        ));
        assert_eq!(
            page.entries.iter().map(object_entry).collect::<Vec<_>>(),
            vec![("beta", beta_v1.as_str(), false)]
        );

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "alpha",
            None,
            Some("alpha"),
            None,
            10,
        ));
        assert!(page.entries.is_empty());
    }

    #[test]
    fn list_version_entries_accept_zero_limit() {
        let mut connection = open_schema();
        let transaction = must(connection.transaction());
        put_object(&transaction, "key", b"value");

        let page = must(Sqlite::list_object_version_entries(
            &transaction,
            "",
            Some("/"),
            None,
            None,
            0,
        ));
        assert!(page.entries.is_empty());
        assert!(page.next_key_marker.is_none());
        assert!(page.next_version_id_marker.is_none());
        assert!(page.is_truncated.not());
    }

    #[test]
    fn list_queries_report_missing_schema_for_marker_shapes() {
        let mut connection = must(Connection::open_in_memory());
        let transaction = must(connection.transaction());

        for result in [
            Sqlite::list_objects(&transaction, Some("a"), Some("b"), 1),
            Sqlite::list_objects(&transaction, Some("a"), None, 1),
            Sqlite::list_objects(&transaction, None, Some("b"), 1),
            Sqlite::list_objects(&transaction, None, None, 1),
        ] {
            assert_rusqlite_error(&result);
        }

        for result in [
            Sqlite::list_object_version_entries(&transaction, "a", None, Some("b"), None, 1),
            Sqlite::list_object_version_entries(&transaction, "a", None, None, None, 1),
            Sqlite::list_object_version_entries(&transaction, "", None, Some("b"), None, 1),
            Sqlite::list_object_version_entries(&transaction, "", None, None, None, 1),
            Sqlite::list_object_version_entries(&transaction, "", None, Some("b"), Some("v"), 1),
            Sqlite::list_object_version_entries(
                &transaction,
                "a",
                Some("/"),
                Some("b"),
                Some("v"),
                1,
            ),
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
        assert_rusqlite_error(&Sqlite::list_objects(&transaction, None, None, 1));
    }

    #[test]
    fn list_version_entries_reject_malformed_rows_and_limits() {
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
        assert_rusqlite_error(&Sqlite::list_object_version_entries(
            &transaction,
            "",
            None,
            None,
            None,
            1,
        ));
        assert_rusqlite_error(&Sqlite::list_object_version_entries(
            &transaction,
            "",
            None,
            None,
            None,
            -1,
        ));
    }
}
