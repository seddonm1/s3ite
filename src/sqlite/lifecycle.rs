//! `SQLite` S3 lifecycle persistence and maintenance helpers.

use std::collections::HashMap;

use rusqlite::{Error::ToSqlConversionFailure, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    LifecycleExpirationHeader, Sqlite, SqliteResult, StoredLifecycleAndFilter,
    StoredLifecycleExpiration, StoredLifecycleFilter, StoredLifecycleRule,
    StoredNoncurrentVersionExpiration, StoredObjectLock, StoredTag, current_expiration_time,
    lifecycle_expiration_header, lifecycle_rule_matches_object, noncurrent_expiration_time,
    object_lock_allows_lifecycle_delete, optional_stored_object_lock, row_size,
};

#[derive(Debug)]
struct LifecycleObjectCandidate {
    sequence: i64,
    key: String,
    version_id: String,
    size: i64,
    last_modified: OffsetDateTime,
    noncurrent_since: Option<OffsetDateTime>,
    object_lock: StoredObjectLock,
}

#[derive(Debug)]
struct LifecycleDeleteMarkerCandidate {
    key: String,
    version_id: String,
    last_modified: OffsetDateTime,
}

#[derive(Debug)]
struct LifecycleMultipartCandidate {
    upload_id: Uuid,
    key: String,
    initiated: OffsetDateTime,
}

impl Sqlite {
    /// Reads bucket lifecycle rules in persisted rule order.
    pub(crate) fn get_bucket_lifecycle_rules(
        transaction: &Transaction,
    ) -> SqliteResult<Vec<StoredLifecycleRule>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                sequence,
                id,
                enabled,
                expiration_days,
                expiration_date,
                expiration_delete_marker,
                noncurrent_days,
                newer_noncurrent_versions,
                abort_incomplete_multipart_days
            FROM bucket_lifecycle_rule
            ORDER BY sequence;",
        )?;

        let rows = stmt
            .query_map((), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, Option<i32>>(3)?,
                    row.get::<_, Option<OffsetDateTime>>(4)?,
                    row.get::<_, bool>(5)?,
                    row.get::<_, Option<i32>>(6)?,
                    row.get::<_, Option<i32>>(7)?,
                    row.get::<_, Option<i32>>(8)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        rows.into_iter()
            .map(
                |(
                    sequence,
                    id,
                    enabled,
                    expiration_days,
                    expiration_date,
                    expiration_delete_marker,
                    noncurrent_days,
                    newer_noncurrent_versions,
                    abort_incomplete_multipart_days,
                )| {
                    Ok(StoredLifecycleRule {
                        id,
                        enabled,
                        filter: Self::get_bucket_lifecycle_filter(transaction, sequence)?,
                        expiration: (expiration_days.is_some()
                            || expiration_date.is_some()
                            || expiration_delete_marker)
                            .then_some(StoredLifecycleExpiration {
                                days: expiration_days,
                                date: expiration_date,
                                expired_object_delete_marker: expiration_delete_marker,
                            }),
                        noncurrent_expiration: noncurrent_days.map(|noncurrent_days| {
                            StoredNoncurrentVersionExpiration {
                                noncurrent_days,
                                newer_noncurrent_versions,
                            }
                        }),
                        abort_incomplete_multipart_days,
                    })
                },
            )
            .collect()
    }

    /// Replaces all bucket lifecycle rules.
    pub(crate) fn replace_bucket_lifecycle_rules(
        transaction: &Transaction,
        rules: &[StoredLifecycleRule],
    ) -> SqliteResult<()> {
        transaction.execute("DELETE FROM bucket_lifecycle_rule;", ())?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO bucket_lifecycle_rule (
                sequence,
                id,
                enabled,
                expiration_days,
                expiration_date,
                expiration_delete_marker,
                noncurrent_days,
                newer_noncurrent_versions,
                abort_incomplete_multipart_days
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
        )?;
        for (sequence, rule) in rules.iter().enumerate() {
            let sequence =
                i64::try_from(sequence).map_err(|err| ToSqlConversionFailure(Box::new(err)))?;
            stmt.execute((
                sequence,
                rule.id.as_deref(),
                rule.enabled,
                rule.expiration
                    .as_ref()
                    .and_then(|expiration| expiration.days),
                rule.expiration
                    .as_ref()
                    .and_then(|expiration| expiration.date),
                rule.expiration
                    .as_ref()
                    .is_some_and(|expiration| expiration.expired_object_delete_marker),
                rule.noncurrent_expiration
                    .as_ref()
                    .map(|expiration| expiration.noncurrent_days),
                rule.noncurrent_expiration
                    .as_ref()
                    .and_then(|expiration| expiration.newer_noncurrent_versions),
                rule.abort_incomplete_multipart_days,
            ))?;
            Self::insert_bucket_lifecycle_filter(transaction, sequence, &rule.filter)?;
        }

        Ok(())
    }

    /// Applies bucket lifecycle cleanup rules to stored objects and multipart uploads.
    pub(crate) fn apply_lifecycle_rules(
        transaction: &Transaction,
        now: OffsetDateTime,
    ) -> SqliteResult<()> {
        let rules = Self::get_bucket_lifecycle_rules(transaction)?;
        if rules.is_empty() {
            return Ok(());
        }

        Self::abort_lifecycle_multipart_uploads(transaction, &rules, now)?;
        Self::expire_lifecycle_current_objects(transaction, &rules, now)?;
        Self::expire_lifecycle_noncurrent_objects(transaction, &rules, now)?;
        Self::expire_lifecycle_delete_markers(transaction, &rules, now)?;
        Ok(())
    }

    /// Computes an object's lifecycle expiration header, when a current expiration rule applies.
    pub(crate) fn lifecycle_expiration_header_for_object(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        size: i64,
        last_modified: OffsetDateTime,
    ) -> SqliteResult<Option<LifecycleExpirationHeader>> {
        let rules = Self::get_bucket_lifecycle_rules(transaction)?;
        if rules.is_empty() {
            return Ok(None);
        }
        let Some(sequence) = Self::find_version_sequence(transaction, key, version_id)? else {
            return Ok(None);
        };
        let tags = Self::get_object_tags(transaction, sequence)?;
        Ok(lifecycle_expiration_header(
            &rules,
            key,
            size,
            last_modified,
            &tags,
        ))
    }

    fn get_bucket_lifecycle_filter(
        transaction: &Transaction,
        rule_sequence: i64,
    ) -> SqliteResult<StoredLifecycleFilter> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                kind,
                prefix,
                tag_key,
                tag_value,
                object_size_greater_than,
                object_size_less_than
            FROM bucket_lifecycle_filter
            WHERE rule_sequence = ?1;",
        )?;

        let (kind, prefix, tag_key, tag_value, min_size, max_size) =
            stmt.query_row((rule_sequence,), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                ))
            })?;

        Ok(match kind.as_str() {
            "empty" => StoredLifecycleFilter::Empty,
            "legacy_prefix" => StoredLifecycleFilter::LegacyPrefix(prefix.unwrap_or_default()),
            "prefix" => StoredLifecycleFilter::Prefix(prefix.unwrap_or_default()),
            "tag" => StoredLifecycleFilter::Tag(StoredTag {
                key: tag_key.unwrap_or_default(),
                value: tag_value.unwrap_or_default(),
            }),
            "size_gt" => StoredLifecycleFilter::SizeGreaterThan(min_size.unwrap_or_default()),
            "size_lt" => StoredLifecycleFilter::SizeLessThan(max_size.unwrap_or_default()),
            "and" => StoredLifecycleFilter::And(StoredLifecycleAndFilter {
                prefix,
                tags: Self::get_bucket_lifecycle_and_tags(transaction, rule_sequence)?,
                object_size_greater_than: min_size,
                object_size_less_than: max_size,
            }),
            _ => return Err(rusqlite::Error::InvalidQuery.into()),
        })
    }

    fn get_bucket_lifecycle_and_tags(
        transaction: &Transaction,
        rule_sequence: i64,
    ) -> SqliteResult<Vec<StoredTag>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT key, value
            FROM bucket_lifecycle_and_tag
            WHERE rule_sequence = ?1
            ORDER BY position;",
        )?;

        Ok(stmt
            .query_map((rule_sequence,), |row| {
                Ok(StoredTag {
                    key: row.get(0)?,
                    value: row.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn insert_bucket_lifecycle_filter(
        transaction: &Transaction,
        rule_sequence: i64,
        filter: &StoredLifecycleFilter,
    ) -> SqliteResult<()> {
        let (kind, prefix, tag_key, tag_value, min_size, max_size, and_tags) = match filter {
            StoredLifecycleFilter::Empty => ("empty", None, None, None, None, None, &[][..]),
            StoredLifecycleFilter::LegacyPrefix(prefix) => (
                "legacy_prefix",
                Some(prefix.as_str()),
                None,
                None,
                None,
                None,
                &[][..],
            ),
            StoredLifecycleFilter::Prefix(prefix) => (
                "prefix",
                Some(prefix.as_str()),
                None,
                None,
                None,
                None,
                &[][..],
            ),
            StoredLifecycleFilter::Tag(tag) => (
                "tag",
                None,
                Some(tag.key.as_str()),
                Some(tag.value.as_str()),
                None,
                None,
                &[][..],
            ),
            StoredLifecycleFilter::SizeGreaterThan(size) => {
                ("size_gt", None, None, None, Some(*size), None, &[][..])
            }
            StoredLifecycleFilter::SizeLessThan(size) => {
                ("size_lt", None, None, None, None, Some(*size), &[][..])
            }
            StoredLifecycleFilter::And(and) => (
                "and",
                and.prefix.as_deref(),
                None,
                None,
                and.object_size_greater_than,
                and.object_size_less_than,
                and.tags.as_slice(),
            ),
        };

        transaction.execute(
            "
            INSERT INTO bucket_lifecycle_filter (
                rule_sequence,
                kind,
                prefix,
                tag_key,
                tag_value,
                object_size_greater_than,
                object_size_less_than
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            (
                rule_sequence,
                kind,
                prefix,
                tag_key,
                tag_value,
                min_size,
                max_size,
            ),
        )?;
        Self::insert_bucket_lifecycle_and_tags(transaction, rule_sequence, and_tags)
    }

    fn insert_bucket_lifecycle_and_tags(
        transaction: &Transaction,
        rule_sequence: i64,
        tags: &[StoredTag],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO bucket_lifecycle_and_tag (rule_sequence, position, key, value)
            VALUES (?1, ?2, ?3, ?4);",
        )?;
        for (position, tag) in tags.iter().enumerate() {
            let position =
                i64::try_from(position).map_err(|err| ToSqlConversionFailure(Box::new(err)))?;
            stmt.execute((
                rule_sequence,
                position,
                tag.key.as_str(),
                tag.value.as_str(),
            ))?;
        }
        Ok(())
    }

    fn abort_lifecycle_multipart_uploads(
        transaction: &Transaction,
        rules: &[StoredLifecycleRule],
        now: OffsetDateTime,
    ) -> SqliteResult<()> {
        let uploads = Self::lifecycle_multipart_candidates(transaction)?;
        for upload in uploads {
            let tags = Self::get_multipart_upload_tags(transaction, upload.upload_id)?;
            let should_abort = rules.iter().any(|rule| {
                rule.enabled
                    && lifecycle_rule_matches_object(rule, &upload.key, 0, &tags)
                    && rule.abort_incomplete_multipart_days.is_some_and(|days| {
                        upload.initiated + time::Duration::days(i64::from(days)) <= now
                    })
            });
            if should_abort {
                Self::delete_multipart(transaction, upload.upload_id)?;
            }
        }
        Ok(())
    }

    fn expire_lifecycle_current_objects(
        transaction: &Transaction,
        rules: &[StoredLifecycleRule],
        now: OffsetDateTime,
    ) -> SqliteResult<()> {
        let versioning_state = Self::get_bucket_versioning_state(transaction)?;
        let objects = Self::lifecycle_current_object_candidates(transaction)?;
        for object in objects {
            let version_id = object.version_id.clone();
            let tags = Self::get_object_tags(transaction, object.sequence)?;
            let should_expire = rules.iter().any(|rule| {
                rule.enabled
                    && lifecycle_rule_matches_object(rule, &object.key, object.size, &tags)
                    && rule.expiration.as_ref().is_some_and(|expiration| {
                        current_expiration_time(expiration, object.last_modified)
                            .is_some_and(|expires_at| expires_at <= now)
                    })
            });
            if should_expire {
                if versioning_state == super::BucketVersioningState::Enabled {
                    Self::delete_object(transaction, &object.key, None)?;
                } else if object_lock_allows_lifecycle_delete(&object.object_lock, now) {
                    Self::delete_version_row(transaction, &object.key, &version_id)?;
                    Self::promote_latest_version(transaction, &object.key)?;
                }
            }
        }
        Ok(())
    }

    fn expire_lifecycle_noncurrent_objects(
        transaction: &Transaction,
        rules: &[StoredLifecycleRule],
        now: OffsetDateTime,
    ) -> SqliteResult<()> {
        let objects = Self::lifecycle_noncurrent_object_candidates(transaction)?;
        let mut noncurrent_rank_by_key = HashMap::<String, i32>::new();
        for object in objects {
            let rank = noncurrent_rank_by_key
                .entry(object.key.clone())
                .and_modify(|rank| *rank += 1)
                .or_insert(0);
            let tags = Self::get_object_tags(transaction, object.sequence)?;
            let should_expire = rules.iter().any(|rule| {
                rule.enabled
                    && lifecycle_rule_matches_object(rule, &object.key, object.size, &tags)
                    && rule
                        .noncurrent_expiration
                        .as_ref()
                        .is_some_and(|expiration| {
                            if expiration
                                .newer_noncurrent_versions
                                .is_some_and(|newer| *rank < newer)
                            {
                                return false;
                            }
                            let noncurrent_since =
                                object.noncurrent_since.unwrap_or(object.last_modified);
                            noncurrent_expiration_time(expiration, noncurrent_since) <= now
                        })
            });
            if should_expire && object_lock_allows_lifecycle_delete(&object.object_lock, now) {
                Self::delete_version_row(transaction, &object.key, &object.version_id)?;
            }
        }
        Ok(())
    }

    fn expire_lifecycle_delete_markers(
        transaction: &Transaction,
        rules: &[StoredLifecycleRule],
        now: OffsetDateTime,
    ) -> SqliteResult<()> {
        let markers = Self::lifecycle_delete_marker_candidates(transaction)?;
        for marker in markers {
            if Self::object_version_count(transaction, &marker.key)? != 1 {
                continue;
            }
            let should_expire = rules.iter().any(|rule| {
                rule.enabled
                    && lifecycle_rule_matches_object(rule, &marker.key, 0, &[])
                    && rule.expiration.as_ref().is_some_and(|expiration| {
                        expiration.expired_object_delete_marker
                            || current_expiration_time(expiration, marker.last_modified)
                                .is_some_and(|expires_at| expires_at <= now)
                    })
            });
            if should_expire {
                Self::delete_version_row(transaction, &marker.key, &marker.version_id)?;
            }
        }
        Ok(())
    }

    fn lifecycle_current_object_candidates(
        transaction: &Transaction,
    ) -> SqliteResult<Vec<LifecycleObjectCandidate>> {
        Self::lifecycle_object_candidates(
            transaction,
            "
            SELECT
                sequence,
                key,
                version_id,
                size,
                last_modified,
                noncurrent_since,
                object_lock_legal_hold,
                object_lock_retention_mode,
                object_lock_retain_until
            FROM object_version
            WHERE is_latest = 1 AND is_delete_marker = 0
            ORDER BY key;",
        )
    }

    fn lifecycle_noncurrent_object_candidates(
        transaction: &Transaction,
    ) -> SqliteResult<Vec<LifecycleObjectCandidate>> {
        Self::lifecycle_object_candidates(
            transaction,
            "
            SELECT
                sequence,
                key,
                version_id,
                size,
                last_modified,
                noncurrent_since,
                object_lock_legal_hold,
                object_lock_retention_mode,
                object_lock_retain_until
            FROM object_version
            WHERE is_latest = 0 AND is_delete_marker = 0
            ORDER BY key, sequence DESC;",
        )
    }

    fn lifecycle_object_candidates(
        transaction: &Transaction,
        sql: &str,
    ) -> SqliteResult<Vec<LifecycleObjectCandidate>> {
        let mut stmt = transaction.prepare_cached(sql)?;
        Ok(stmt
            .query_map((), |row| {
                Ok(LifecycleObjectCandidate {
                    sequence: row.get(0)?,
                    key: row.get(1)?,
                    version_id: row.get(2)?,
                    size: row_size(row, 3)?,
                    last_modified: row.get(4)?,
                    noncurrent_since: row.get(5)?,
                    object_lock: optional_stored_object_lock(
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    )?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn lifecycle_delete_marker_candidates(
        transaction: &Transaction,
    ) -> SqliteResult<Vec<LifecycleDeleteMarkerCandidate>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT key, version_id, last_modified
            FROM object_version
            WHERE is_latest = 1 AND is_delete_marker = 1
            ORDER BY key;",
        )?;
        Ok(stmt
            .query_map((), |row| {
                Ok(LifecycleDeleteMarkerCandidate {
                    key: row.get(0)?,
                    version_id: row.get(1)?,
                    last_modified: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn lifecycle_multipart_candidates(
        transaction: &Transaction,
    ) -> SqliteResult<Vec<LifecycleMultipartCandidate>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT upload_id, key, initiated
            FROM multipart_upload
            ORDER BY key, upload_id;",
        )?;
        Ok(stmt
            .query_map((), |row| {
                Ok(LifecycleMultipartCandidate {
                    upload_id: row.get(0)?,
                    key: row.get(1)?,
                    initiated: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn get_multipart_upload_tags(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<Vec<StoredTag>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT key, value
            FROM multipart_upload_tag
            WHERE upload_id = ?1
            ORDER BY key;",
        )?;
        Ok(stmt
            .query_map((upload_id,), |row| {
                Ok(StoredTag {
                    key: row.get(0)?,
                    value: row.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn object_version_count(transaction: &Transaction, key: &str) -> SqliteResult<i64> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT COUNT(*)
            FROM object_version
            WHERE key = ?1;",
        )?;
        Ok(stmt.query_row((key,), |row| row.get(0))?)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::{
        s3::{
            FullObjectChecksum, StoredChecksum, StoredLifecycleExpiration, StoredLifecycleFilter,
            StoredLifecycleRule, StoredNoncurrentVersionExpiration, StoredObjectLock, StoredTag,
        },
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

    fn checksum() -> StoredChecksum {
        StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("crc".to_string()))
    }

    fn key_value(key: &str, value: &[u8], last_modified: OffsetDateTime) -> KeyValue {
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
                last_modified,
                md5: Some("md5".to_string()),
                checksum: Some(checksum()),
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
        }
    }

    fn expiration_rule(days: i32) -> StoredLifecycleRule {
        StoredLifecycleRule {
            id: Some("expire".to_string()),
            enabled: true,
            filter: StoredLifecycleFilter::Prefix("logs/".to_string()),
            expiration: Some(StoredLifecycleExpiration {
                days: Some(days),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart_days: None,
        }
    }

    #[test]
    fn lifecycle_rules_replace_and_read_in_rule_order() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let rules = vec![
            expiration_rule(1),
            StoredLifecycleRule {
                id: Some("noncurrent".to_string()),
                enabled: true,
                filter: StoredLifecycleFilter::Empty,
                expiration: None,
                noncurrent_expiration: Some(StoredNoncurrentVersionExpiration {
                    noncurrent_days: 2,
                    newer_noncurrent_versions: Some(1),
                }),
                abort_incomplete_multipart_days: Some(3),
            },
        ];

        Sqlite::replace_bucket_lifecycle_rules(&transaction, &rules)?;
        assert_eq!(Sqlite::get_bucket_lifecycle_rules(&transaction)?, rules);

        Sqlite::replace_bucket_lifecycle_rules(&transaction, &[])?;
        assert!(Sqlite::get_bucket_lifecycle_rules(&transaction)?.is_empty());
        Ok(())
    }

    #[test]
    fn lifecycle_current_expiration_deletes_matching_unversioned_object() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::days(10);
        Sqlite::replace_bucket_lifecycle_rules(&transaction, &[expiration_rule(1)])?;
        Sqlite::put_object(
            &transaction,
            key_value("logs/old", b"value", OffsetDateTime::UNIX_EPOCH),
        )?;
        Sqlite::put_object(
            &transaction,
            key_value("other/old", b"value", OffsetDateTime::UNIX_EPOCH),
        )?;

        Sqlite::apply_lifecycle_rules(&transaction, now)?;

        assert!(Sqlite::get_metadata(&transaction, "logs/old")?.is_none());
        assert!(Sqlite::get_metadata(&transaction, "other/old")?.is_some());
        Ok(())
    }

    #[test]
    fn lifecycle_abort_multipart_upload_uses_pending_tags() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let upload_id = Uuid::new_v4();
        Sqlite::replace_bucket_lifecycle_rules(
            &transaction,
            &[StoredLifecycleRule {
                id: Some("abort".to_string()),
                enabled: true,
                filter: StoredLifecycleFilter::Tag(StoredTag {
                    key: "tmp".to_string(),
                    value: "yes".to_string(),
                }),
                expiration: None,
                noncurrent_expiration: None,
                abort_incomplete_multipart_days: Some(1),
            }],
        )?;
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
        Sqlite::put_multipart_upload_tags(
            &transaction,
            upload_id,
            &[StoredTag {
                key: "tmp".to_string(),
                value: "yes".to_string(),
            }],
        )?;

        Sqlite::apply_lifecycle_rules(
            &transaction,
            OffsetDateTime::now_utc() + time::Duration::days(2),
        )?;

        assert!(
            Sqlite::get_multipart_upload_state(&transaction, upload_id, "bucket", "key")?.is_none()
        );
        Ok(())
    }

    #[test]
    fn lifecycle_expired_delete_marker_requires_only_remaining_version() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        Sqlite::replace_bucket_lifecycle_rules(
            &transaction,
            &[StoredLifecycleRule {
                id: Some("marker".to_string()),
                enabled: true,
                filter: StoredLifecycleFilter::Empty,
                expiration: Some(StoredLifecycleExpiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: true,
                }),
                noncurrent_expiration: None,
                abort_incomplete_multipart_days: None,
            }],
        )?;
        Sqlite::insert_delete_marker(&transaction, "deleted", NULL_VERSION_ID)?;

        Sqlite::apply_lifecycle_rules(&transaction, OffsetDateTime::now_utc())?;

        assert!(Sqlite::get_current_metadata(&transaction, "deleted")?.is_none());
        Ok(())
    }
}
