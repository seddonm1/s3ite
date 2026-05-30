use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Not,
    path::{Path, PathBuf},
    sync::Arc,
};

use path_absolutize::Absolutize;
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction};
use s3s::{
    S3Error,
    S3ErrorCode::{InternalError, MethodNotAllowed},
    dto, s3_error,
};
use time::{Duration, OffsetDateTime};
use tokio::{fs, sync::RwLock};
use uuid::Uuid;

use crate::{database::BucketConnection, error::Result, utils::repeat_vars};

fn row_u64(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(idx)?;
    u64::try_from(value).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Integer,
            Box::new(err),
        )
    })
}

fn sql_i64(value: u64) -> rusqlite::Result<i64> {
    i64::try_from(value).map_err(|err| ToSqlConversionFailure(Box::new(err)))
}

#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<BucketConnection>>>>,
}

#[derive(Debug)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) size: u64,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) content_type: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

#[derive(Debug)]
pub(crate) struct KeySize {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

#[derive(Debug)]
pub(crate) struct KeyMetadata {
    pub(crate) size: u64,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) content_type: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Multipart {
    pub(crate) upload_id: Uuid,
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) value: Vec<u8>,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
}

#[derive(Debug)]
pub(crate) struct MultipartMetadata {
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) size: i64,
}

#[derive(Debug)]
pub(crate) struct MultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) content_type: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
}

#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) md5: String,
}

impl Sqlite {
    /// Opens configured bucket databases and starts background maintenance.
    ///
    /// # Errors
    ///
    /// Returns an error if the root path, configured bucket databases, or bucket
    /// configuration cannot be loaded.
    pub async fn new(config: &crate::Config) -> Result<Self> {
        if config.readers == 0 {
            Err(crate::S3ite::Configuration(
                "readers must be greater than zero".to_string(),
            ))?;
        }

        let root = env::current_dir()?.join(&config.root).canonicalize()?;

        let mut buckets = HashMap::new();

        let mut iter = fs::read_dir(root.clone()).await?;
        while let Some(entry) = iter.next_entry().await? {
            let file_type = entry.file_type().await?;
            let config = config.clone();

            if file_type.is_file() {
                let path = entry.path();
                if let Some(extension) = path.extension()
                    && extension == "sqlite3"
                {
                    let Some(bucket) = path
                        .file_stem()
                        .and_then(|file_stem| file_stem.to_str())
                        .map(str::to_string)
                    else {
                        continue;
                    };
                    let connection = BucketConnection::open(path, &config, &bucket).await?;
                    if config.read_only(Some(&bucket)).not() {
                        let bucket_clone = bucket.clone();
                        connection
                            .write(move |connection| {
                                connection.execute_batch(&config.to_sql(Some(&bucket_clone)))?;

                                connection.execute_batch(
                                    "
                                    PRAGMA analysis_limit=1000;
                                    PRAGMA optimize;
                                    ",
                                )?;

                                let transaction = connection.transaction()?;
                                Self::try_create_tables(&transaction)?;
                                Self::try_delete_multipart_expired(
                                    &transaction,
                                    OffsetDateTime::now_utc().saturating_sub(Duration::hours(1)),
                                )?;

                                Ok(transaction.commit()?)
                            })
                            .await
                            .map_err(|_| rusqlite::Error::InvalidQuery)?;
                    }

                    buckets.insert(bucket, Arc::new(connection));
                }
            }
        }

        // validate that any specified bucket configurations have existing bucket
        let difference = config
            .buckets
            .keys()
            .collect::<HashSet<_>>()
            .difference(&buckets.keys().collect::<HashSet<_>>())
            .copied()
            .collect::<Vec<_>>();
        if difference.is_empty().not() {
            Err(S3Error::with_message(
                InternalError,
                format!("found configurations for buckets: {difference:?} that do not exist"),
            ))?;
        }

        let buckets = Arc::new(RwLock::new(buckets));

        // start a garbage collection process for database maintenance
        let buckets_clone = buckets.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;

                let connections = buckets_clone
                    .read()
                    .await
                    .iter()
                    .filter(|(bucket, _)| config_clone.read_only(Some(bucket)).not())
                    .map(|(_, connection)| connection.clone())
                    .collect::<Vec<_>>();

                for connection in connections {
                    connection
                        .write(move |connection| {
                            Ok(connection.execute_batch(
                                "
                                    PRAGMA wal_checkpoint(TRUNCATE);
                                    PRAGMA incremental_vacuum(100);
                                    ",
                            )?)
                        })
                        .await
                        .ok();
                }
            }
        });

        Ok(Self {
            root,
            config: config.clone(),
            buckets,
        })
    }

    /// Resolves a path relative to the configured virtual root.
    pub(crate) fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    /// Resolves the database file path for a bucket.
    pub(crate) fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = PathBuf::from(format!("{bucket}.sqlite3"));
        self.resolve_abs_path(dir)
    }

    /// Returns an open database connection for a bucket.
    pub(crate) async fn try_get_connection(&self, bucket: &str) -> Result<Arc<BucketConnection>> {
        Ok(self
            .buckets
            .read()
            .await
            .get(bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?
            .to_owned())
    }

    /// Creates a bucket database and registers its connection.
    pub(crate) async fn try_create_bucket(&self, bucket: &str, file_path: PathBuf) -> Result<()> {
        let config = self.config.clone();

        let connection = BucketConnection::open(file_path, &config, bucket).await?;

        connection
            .write(move |connection| {
                connection.execute_batch(&config.to_sql(None))?;

                let transaction = connection.transaction()?;
                Self::try_create_tables(&transaction)?;
                Ok(transaction.commit()?)
            })
            .await?;

        self.buckets
            .write()
            .await
            .insert(bucket.to_string(), Arc::new(connection));

        Ok(())
    }

    /// Ensures the bucket database schema exists and applies additive migrations.
    pub(crate) fn try_create_tables(transaction: &Transaction) -> rusqlite::Result<usize> {
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS data (
                    key TEXT PRIMARY KEY,
                    value BLOB
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    metadata TEXT,
                    content_type TEXT,
                    cache_control TEXT,
                    expires TEXT,
                    last_modified TEXT NOT NULL,
                    md5 TEXT,
                    FOREIGN KEY (key) REFERENCES data (key) ON DELETE CASCADE
                ) WITHOUT ROWID;",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS multipart_upload (
                    upload_id BLOB NOT NULL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    access_key TEXT,
                    metadata TEXT,
                    content_type TEXT,
                    cache_control TEXT,
                    expires TEXT,
                    UNIQUE(upload_id, bucket, key)
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS completed_multipart_upload (
                    upload_id BLOB NOT NULL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    access_key TEXT,
                    md5 TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    UNIQUE(upload_id, bucket, key)
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS multipart_upload_part (
                    upload_id BLOB NOT NULL,
                    last_modified TEXT NOT NULL,
                    part_number INTEGER NOT NULL,
                    value BLOB NOT NULL,
                    size INTEGER NOT NULL,
                    md5 TEXT,
                    PRIMARY KEY (upload_id, part_number),
                    FOREIGN KEY (upload_id) REFERENCES multipart_upload (upload_id) ON DELETE CASCADE
                );",
            (),
        )?;

        // `CREATE TABLE IF NOT EXISTS` does not update existing bucket databases,
        // so keep additive schema changes here until a formal migration system exists.
        for (table, column, definition) in [
            ("metadata", "content_type", "content_type TEXT"),
            ("metadata", "cache_control", "cache_control TEXT"),
            ("metadata", "expires", "expires TEXT"),
            ("multipart_upload", "metadata", "metadata TEXT"),
            ("multipart_upload", "content_type", "content_type TEXT"),
            ("multipart_upload", "cache_control", "cache_control TEXT"),
            ("multipart_upload", "expires", "expires TEXT"),
        ] {
            if Self::try_has_column(transaction, table, column)?.not() {
                transaction
                    .execute(&format!("ALTER TABLE {table} ADD COLUMN {definition};"), ())?;
            }
        }

        Ok(0)
    }

    /// Returns whether a table already contains a column.
    fn try_has_column(
        transaction: &Transaction,
        table: &str,
        column: &str,
    ) -> rusqlite::Result<bool> {
        let mut stmt = transaction.prepare(&format!("PRAGMA table_info({table});"))?;
        let columns = stmt
            .query_map((), |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(columns.iter().any(|name| name == column))
    }

    /// Lists object metadata ordered by key for keyset pagination.
    pub(crate) fn try_list_objects(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        limit: usize,
    ) -> rusqlite::Result<Vec<KeySize>> {
        let limit = i64::try_from(limit).map_err(|err| ToSqlConversionFailure(Box::new(err)))?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let map_key_size = |row: &rusqlite::Row<'_>| {
            Ok(KeySize {
                key: row.get(0)?,
                size: row_u64(row, 1)?,
                last_modified: row.get(2)?,
                md5: row.get(3)?,
            })
        };

        let objects = match (prefix, after_key) {
            (Some(prefix), Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata WHERE key >= ?1 AND key > ?2 ORDER BY key LIMIT ?3;",
                )?;
                stmt.query_map((prefix, after_key, limit), map_key_size)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata WHERE key >= ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((prefix, limit), map_key_size)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata WHERE key > ?1 ORDER BY key LIMIT ?2;",
                )?;
                stmt.query_map((after_key, limit), map_key_size)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, size, last_modified, md5 FROM metadata ORDER BY key LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_key_size)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(objects)
    }

    /// Returns whether the bucket contains any objects.
    pub(crate) fn try_has_objects(transaction: &Transaction) -> rusqlite::Result<bool> {
        let mut stmt = transaction.prepare_cached("SELECT EXISTS(SELECT 1 FROM metadata);")?;
        stmt.query_row((), |row| row.get(0))
    }

    /// Fetches an object body and metadata by key.
    pub(crate) fn try_get_object(
        transaction: &Transaction,
        key: &str,
    ) -> rusqlite::Result<Option<KeyValue>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                metadata.key,
                data.value,
                metadata.size,
                metadata.metadata,
                metadata.content_type,
                metadata.cache_control,
                metadata.expires,
                metadata.last_modified,
                metadata.md5
            FROM metadata
            INNER JOIN data ON metadata.key = data.key
            WHERE metadata.key = ?;",
        )?;

        stmt.query_row([key], |row| {
            Ok(KeyValue {
                key: row.get(0)?,
                value: row.get(1)?,
                size: row_u64(row, 2)?,
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
                cache_control: row.get(5)?,
                expires: row.get(6)?,
                last_modified: row.get(7)?,
                md5: row.get(8)?,
            })
        })
        .optional()
    }

    /// Fetches object metadata by key without reading the body.
    pub(crate) fn try_get_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> rusqlite::Result<Option<KeyMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                size,
                metadata,
                content_type,
                cache_control,
                expires,
                last_modified,
                md5
            FROM metadata
            WHERE key = ?;",
        )?;

        stmt.query_row([key], |row| {
            Ok(KeyMetadata {
                size: row_u64(row, 0)?,
                metadata: row
                    .get::<_, Option<String>>(1)?
                    .map(|metadata| serde_json::from_str(&metadata))
                    .transpose()
                    .map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                content_type: row.get(2)?,
                cache_control: row.get(3)?,
                expires: row.get(4)?,
                last_modified: row.get(5)?,
                md5: row.get(6)?,
            })
        })
        .optional()
    }

    /// Inserts or replaces an object body and metadata.
    pub(crate) fn try_put_object(
        transaction: &Transaction,
        kv: KeyValue,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO data (key, value)
            VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE
            SET value=excluded.value;",
        )?;

        stmt.execute((&kv.key, kv.value))?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO metadata (key, size, metadata, content_type, cache_control, expires, last_modified, md5)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(key) DO UPDATE
            SET size=excluded.size, metadata=excluded.metadata, content_type=excluded.content_type, cache_control=excluded.cache_control, expires=excluded.expires, last_modified=excluded.last_modified, md5=excluded.md5;",
        )?;

        let size = sql_i64(kv.size)?;
        stmt.execute((
            kv.key,
            size,
            kv.metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            kv.content_type,
            kv.cache_control,
            kv.expires,
            kv.last_modified,
            kv.md5,
        ))
    }

    /// Deletes an object by key.
    pub(crate) fn try_delete_object(
        transaction: &Transaction,
        key: &str,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM data
            WHERE key = ?1;",
        )?;
        stmt.execute([key])
    }

    /// Deletes multiple objects and returns the keys that existed.
    pub(crate) fn try_delete_objects(
        transaction: &Transaction,
        keys: &[String],
    ) -> rusqlite::Result<Vec<String>> {
        let vars = repeat_vars(keys.len());

        let mut stmt = transaction.prepare(&format!(
            "
            DELETE FROM data
            WHERE key IN ({vars})
            RETURNING key;"
        ))?;

        let keys = stmt
            .query_map(rusqlite::params_from_iter(keys), |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(keys)
    }

    /// Creates multipart upload state for an upload ID.
    pub(crate) fn try_create_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        state: MultipartUploadState,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload (upload_id, last_modified, bucket, key, access_key, metadata, content_type, cache_control, expires)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
        )?;

        let now = OffsetDateTime::now_utc();
        stmt.execute((
            upload_id,
            now,
            bucket,
            key,
            state.access_key,
            state
                .metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            state.content_type,
            state.cache_control,
            state.expires,
        ))
    }

    /// Fetches multipart upload state for validation and completion.
    pub(crate) fn try_get_multipart_upload_state(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
    ) -> rusqlite::Result<Option<MultipartUploadState>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT access_key, metadata, content_type, cache_control, expires
            FROM multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        stmt.query_row((upload_id, bucket, key), |row| {
            Ok(MultipartUploadState {
                access_key: row.get(0)?,
                metadata: row
                    .get::<_, Option<String>>(1)?
                    .map(|metadata| serde_json::from_str(&metadata))
                    .transpose()
                    .map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                content_type: row.get(2)?,
                cache_control: row.get(3)?,
                expires: row.get(4)?,
            })
        })
        .optional()
    }

    /// Records a completed multipart upload for idempotent retries.
    pub(crate) fn try_put_completed_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        access_key: Option<String>,
        md5: &str,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO completed_multipart_upload (upload_id, bucket, key, access_key, md5, last_modified)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(upload_id) DO UPDATE
            SET bucket=excluded.bucket, key=excluded.key, access_key=excluded.access_key, md5=excluded.md5, last_modified=excluded.last_modified;",
        )?;

        stmt.execute((
            upload_id,
            bucket,
            key,
            access_key,
            md5,
            OffsetDateTime::now_utc(),
        ))
    }

    /// Fetches completed multipart upload state by upload ID.
    pub(crate) fn try_get_completed_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
    ) -> rusqlite::Result<Option<CompletedMultipartUploadState>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT access_key, md5
            FROM completed_multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        stmt.query_row((upload_id, bucket, key), |row| {
            Ok(CompletedMultipartUploadState {
                access_key: row.get(0)?,
                md5: row.get(1)?,
            })
        })
        .optional()
    }

    /// Inserts or replaces a multipart upload part.
    pub(crate) fn try_put_multipart(
        transaction: &Transaction,
        multipart: Multipart,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload_part (upload_id, last_modified, part_number, value, size, md5)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(upload_id, part_number) DO UPDATE
            SET last_modified=excluded.last_modified, value=excluded.value, size=excluded.size, md5=excluded.md5;",
        )?;

        stmt.execute((
            multipart.upload_id,
            multipart.last_modified,
            multipart.part_number,
            multipart.value,
            multipart.size,
            multipart.md5,
        ))
    }

    /// Lists multipart upload part metadata ordered by part number.
    pub(crate) fn try_list_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> rusqlite::Result<Vec<MultipartMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                size
            FROM multipart_upload_part
            WHERE upload_id = ?1
            ORDER BY part_number;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map([upload_id], |row| {
                Ok(MultipartMetadata {
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    size: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Fetches multipart upload parts with bodies ordered by part number.
    pub(crate) fn try_get_multiparts(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> rusqlite::Result<Vec<Multipart>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                value,
                size,
                md5
            FROM multipart_upload_part
            WHERE upload_id = ?1
            ORDER BY part_number;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map([upload_id], |row| {
                Ok(Multipart {
                    upload_id,
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    value: row.get(2)?,
                    size: row.get(3)?,
                    md5: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Deletes multipart upload state and cascading parts.
    pub(crate) fn try_delete_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM multipart_upload
            WHERE upload_id = ?1;",
        )?;
        stmt.execute([upload_id])
    }

    /// Deletes multipart uploads older than the expiration timestamp.
    pub(crate) fn try_delete_multipart_expired(
        transaction: &Transaction,
        expire_before: OffsetDateTime,
    ) -> rusqlite::Result<()> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM multipart_upload
            WHERE DATETIME(last_modified) < DATETIME(?1);",
        )?;
        stmt.execute([expire_before])?;

        Ok(())
    }

    /// Rejects mutations for read-only bucket configuration.
    pub(crate) fn validate_mutable_bucket(&self, bucket: &str) -> Result<()> {
        if self.config.read_only(Some(bucket)) {
            Err(S3Error::with_message(
                MethodNotAllowed,
                "database is in read-only mode",
            ))?;
        }
        Ok(())
    }
}
