//! `SQLite` helpers for object copy sources.

use rusqlite::Transaction;

use super::{KeyValue, Sqlite, SqliteError, SqliteResult};

impl Sqlite {
    /// Fetches the visible source object for copy-style operations.
    pub(crate) fn get_copy_source_object(
        transaction: &Transaction,
        key: &str,
        version_id: Option<&str>,
    ) -> SqliteResult<KeyValue> {
        match version_id {
            Some(version_id) => Self::get_object_version(transaction, key, version_id)?
                .ok_or(SqliteError::ObjectVersionNotFound),
            None => Self::get_object(transaction, key)?.ok_or(SqliteError::ObjectNotFound),
        }
    }
}
