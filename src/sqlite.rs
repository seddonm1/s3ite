use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Not,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
};

use path_absolutize::Absolutize;
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, ToSql, Transaction};
use s3s::{
    auth::Credentials,
    dto, s3_error, S3Error,
    S3ErrorCode::{InternalError, MethodNotAllowed},
};
use time::{Duration, OffsetDateTime};
use tokio::{fs, sync::RwLock};
use uuid::Uuid;

use crate::{database::Connection, error::Result, utils::repeat_vars};

#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Arc<Connection>>>>,
    pub(crate) continuation_tokens: Arc<Mutex<HashMap<String, ContinuationToken>>>,
}

#[derive(Debug)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) size: u64,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

#[derive(Debug)]
pub(crate) struct KeySize {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: String,
}

#[derive(Debug)]
pub(crate) struct KeyMetadata {
    pub(crate) size: u64,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) last_modified: OffsetDateTime,
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
pub(crate) struct ContinuationToken {
    pub(crate) token: String,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) key_sizes: Vec<KeySize>,
}

impl Sqlite {
    /// # Panics
    pub async fn new(config: &crate::Config) -> Result<Self> {
        let root = env::current_dir()?.join(&config.root).canonicalize()?;

        let mut buckets = HashMap::new();

        let mut iter = fs::read_dir(root.clone()).await?;
        while let Some(entry) = iter.next_entry().await? {
            let file_type = entry.file_type().await?;
            let config = config.clone();

            if file_type.is_file() {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    if extension == "sqlite3" {
                        let bucket = path.file_stem().unwrap().to_str().unwrap().to_string();
                        let bucket_clone = bucket.clone();
                        let connection = Connection::open(path, &config, &bucket).await?;
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
                                Self::try_delete_multipart_expired(
                                    &transaction,
                                    OffsetDateTime::now_utc().saturating_sub(Duration::hours(1)),
                                )?;

                                Ok(transaction.commit()?)
                            })
                            .await
                            .map_err(|_| rusqlite::Error::InvalidQuery)?;

                        buckets.insert(bucket, Arc::new(connection));
                    }
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
        let continuation_tokens = Arc::new(Mutex::new(HashMap::<String, ContinuationToken>::new()));

        // start a garbage collection process for:
        // - run the vacuum process
        // - cleaning up expired continuation_tokens
        let buckets_clone = buckets.clone();
        let continuation_tokens_clone = continuation_tokens.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10000)).await;

                // database maintenance
                let buckets = buckets_clone.write().await;
                for connection in buckets.values() {
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

                // remove any redundant state (i.e. cancelled `list_objects` request snapshots)
                let mut continuation_tokens = continuation_tokens_clone.lock().unwrap();
                continuation_tokens.retain(|_, value| {
                    (OffsetDateTime::now_utc() - value.last_modified).as_seconds_f32() < 120.0
                });
            }
        });

        Ok(Self {
            root,
            config: config.clone(),
            buckets,
            continuation_tokens,
        })
    }

    pub(crate) fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    /// resolve bucket path under the virtual root
    pub(crate) fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = PathBuf::from_str(&format!("{}.sqlite3", &bucket)).unwrap();
        self.resolve_abs_path(dir)
    }

    pub(crate) async fn try_get_connection(&self, bucket: &str) -> Result<Arc<Connection>> {
        Ok(self
            .buckets
            .read()
            .await
            .get(bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?
            .to_owned())
    }

    pub(crate) async fn try_create_bucket(&self, bucket: &str, file_path: PathBuf) -> Result<()> {
        let config = self.config.clone();

        let connection = Connection::open(file_path, &config, bucket).await?;

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

    /// resolve object path under the virtual root
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
        )
    }

    /// resolve object path under the virtual root
    pub(crate) fn try_list_objects(
        transaction: &Transaction,
        prefix: Option<&String>,
        start_after: Option<&String>,
    ) -> rusqlite::Result<Vec<KeySize>> {
        // prefix with the sqlite wildcard
        let prefix = prefix.as_ref().and_then(|prefix| {
            if prefix.is_empty() {
                None
            } else {
                Some(format!("{prefix}%"))
            }
        });

        let (query, params): (&str, Vec<&dyn ToSql>) = match (&prefix, start_after) {
            (Some(prefix), Some(start_after)) => (
                "SELECT key, size, last_modified, md5 FROM metadata WHERE key LIKE ?1 AND key > ?2 ORDER BY key;",
                vec![prefix, start_after],
            ),
            (Some(prefix), None) => (
                "SELECT key, size, last_modified, md5 FROM metadata WHERE key LIKE ?1 ORDER BY key;",
                vec![prefix],
            ),
            (None, Some(start_after)) => (
                "SELECT key, size, last_modified, md5 FROM metadata WHERE key > ?1 ORDER BY key;",
                vec![start_after],
            ),
            (None, None) => (
                "SELECT key, size, last_modified, md5 FROM metadata ORDER BY key;",
                vec![],
            ),
        };

        let mut stmt = transaction.prepare_cached(query)?;

        let objects = stmt
            .query_map(params.as_slice(), |row| {
                Ok(KeySize {
                    key: row.get(0)?,
                    size: row.get(1)?,
                    last_modified: row.get(2)?,
                    md5: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// resolve object path under the virtual root
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
                metadata.last_modified,
                metadata.md5
            FROM metadata
            INNER JOIN data ON metadata.key = data.key
            WHERE metadata.key = ?;",
        )?;

        stmt.query_row([key], |row| {
            Ok(KeyValue {
                key: row.get(0)?,
                value: Some(row.get::<_, Vec<u8>>(1)?),
                size: row.get(2)?,
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
                last_modified: row.get(4)?,
                md5: row.get(5)?,
            })
        })
        .optional()
    }

    pub(crate) fn try_get_metadata(
        transaction: &Transaction,
        key: &str,
    ) -> rusqlite::Result<Option<KeyMetadata>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                size,
                metadata,
                last_modified
            FROM metadata
            WHERE key = ?;",
        )?;

        stmt.query_row([key], |row| {
            Ok(KeyMetadata {
                size: row.get(0)?,
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
                last_modified: row.get(2)?,
            })
        })
        .optional()
    }

    /// resolve object path under the virtual root
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
            INSERT INTO metadata (key, size, metadata, last_modified, md5)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(key) DO UPDATE
            SET size=excluded.size, metadata=excluded.metadata, last_modified=excluded.last_modified, md5=excluded.md5;",
        )?;

        stmt.execute((
            kv.key,
            kv.size,
            kv.metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            kv.last_modified,
            kv.md5,
        ))
    }

    /// resolve object path under the virtual root
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

    pub(crate) fn try_delete_objects_like(
        transaction: &Transaction,
        key: &str,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM data
            WHERE key LIKE ?1;",
        )?;

        stmt.execute([format!("{key}%")])
    }

    pub(crate) fn try_create_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        credentials: Option<Credentials>,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload (upload_id, last_modified, bucket, key, access_key)
            VALUES (?1, ?2, ?3, ?4, ?5);",
        )?;

        let now = OffsetDateTime::now_utc();
        stmt.execute((
            upload_id,
            now,
            bucket,
            key,
            credentials.map(|credentials| credentials.access_key),
        ))
    }

    pub(crate) fn try_verify_upload_id(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        credentials: Option<Credentials>,
    ) -> rusqlite::Result<bool> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT access_key
            FROM multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        let access_key = stmt.query_row((upload_id, bucket, key), |row| {
            row.get::<_, Option<String>>(0)
        })?;

        Ok(access_key == credentials.map(|credentials| credentials.access_key))
    }

    pub(crate) fn try_put_multipart(
        transaction: &Transaction,
        multipart: Multipart,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload_part (upload_id, last_modified, part_number, value, size, md5)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
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

    pub(crate) fn try_delete_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> rusqlite::Result<()> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM multipart_upload
            WHERE upload_id = ?1;",
        )?;
        stmt.execute([upload_id])?;

        Ok(())
    }

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
