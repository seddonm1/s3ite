use crate::error::*;
use crate::utils::repeat_vars;

use deadpool_sqlite::rusqlite::Transaction;
use deadpool_sqlite::{Object, Pool};
use rusqlite::Error::ToSqlConversionFailure;
use rusqlite::{OptionalExtension, ToSql};
use s3s::auth::Credentials;
use s3s::S3ErrorCode::{InternalError, MethodNotAllowed};
use s3s::{dto, s3_error, S3Error};
use time::{Duration, OffsetDateTime};
use tokio::sync::RwLock;

use std::collections::{HashMap, HashSet};
use std::env;
use std::ops::Not;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use tokio::fs;

use deadpool_sqlite::{Config, Runtime};
use path_absolutize::Absolutize;
use uuid::Uuid;

#[derive(Debug)]
pub struct Sqlite {
    pub(crate) root: PathBuf,
    pub(crate) config: crate::Config,
    pub(crate) buckets: Arc<RwLock<HashMap<String, Pool>>>,
    pub(crate) continuation_tokens: Arc<std::sync::RwLock<HashMap<String, ContinuationToken>>>,
}

pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) size: u64,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
}

pub(crate) struct KeyMetadata {
    pub(crate) size: u64,
    pub(crate) metadata: Option<dto::Metadata>,
    pub(crate) last_modified: OffsetDateTime,
}

pub(crate) struct Multipart {
    pub(crate) upload_id: Uuid,
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) value: Vec<u8>,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
}

pub(crate) struct MultipartMetadata {
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) size: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct ContinuationToken {
    pub(crate) token: String,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) offset: usize,
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

                        let cfg = Config::new(path.clone());
                        let pool = cfg.create_pool(Runtime::Tokio1)?;
                        let connection = pool.get().await.unwrap();
                        connection
                            .interact(move |connection| {
                                connection.execute_batch(&config.to_sql(Some(&bucket_clone)))?;

                                connection.execute_batch(
                                    "
                                    PRAGMA analysis_limit=1000;
                                    PRAGMA optimize;
                                    VACUUM;
                                    ",
                                )?;

                                let transaction = connection.transaction()?;
                                Self::try_delete_multipart_expired(
                                    &transaction,
                                    OffsetDateTime::now_utc().saturating_sub(Duration::hours(1)),
                                )?;
                                transaction.commit()
                            })
                            .await
                            .map_err(|_| rusqlite::Error::InvalidQuery)??;

                        buckets.insert(bucket, pool);
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

        Ok(Self {
            root,
            config: config.clone(),
            buckets: Arc::new(RwLock::new(buckets)),
            continuation_tokens: Arc::new(std::sync::RwLock::new(HashMap::new())),
        })
    }

    pub(crate) fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(path.as_ref().absolutize_virtually(&self.root)?.into_owned())
    }

    /// resolve bucket path under the virtual root
    pub(crate) fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = PathBuf::from_str(&format!("{}.sqlite3", &bucket))?;
        self.resolve_abs_path(dir)
    }

    pub(crate) async fn try_create_bucket(
        &self,
        bucket: &str,
        file_path: PathBuf,
    ) -> rusqlite::Result<()> {
        let config = self.config.clone();

        let cfg = Config::new(file_path);
        let pool = cfg.create_pool(Runtime::Tokio1).unwrap();
        let connection = pool.get().await.unwrap();

        connection
            .interact(move |connection| {
                connection.execute_batch(&config.to_sql(None))?;

                let transaction = connection.transaction()?;
                Self::try_create_tables(&transaction)?;
                transaction.commit()
            })
            .await
            .map_err(|_| rusqlite::Error::InvalidQuery)??;

        self.buckets.write().await.insert(bucket.to_string(), pool);

        Ok(())
    }

    /// resolve object path under the virtual root
    pub(crate) fn try_create_tables(transaction: &Transaction) -> rusqlite::Result<usize> {
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    metadata TEXT,
                    last_modified TEXT NOT NULL,
                    md5 TEXT
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS data (
                    key TEXT PRIMARY KEY,
                    value BLOB
                );",
            (),
        )?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS multipart_upload (
                    upload_id BLOB NOT NULL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    access_key TEXT
                );",
            (),
        )?;
        transaction.execute(
            "CREATE UNIQUE INDEX multipart_upload_unique_upload_id_bucket_key ON multipart_upload(upload_id, bucket, key);",
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

    pub(crate) async fn try_get_bucket_pool(&self, bucket: &str) -> Result<Object> {
        Ok(self
            .buckets
            .read()
            .await
            .get(bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?
            .get()
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?)
    }

    /// resolve object path under the virtual root
    pub(crate) fn try_list_objects(
        transaction: &Transaction,
        prefix: Option<String>,
        start_after: &Option<String>,
        max_keys: i32,
        continuation_token: &Option<ContinuationToken>,
    ) -> rusqlite::Result<Vec<KeyValue>> {
        // prefix with the sqlite wildcard
        let prefix = prefix.map(|prefix| format!("{prefix}%"));

        // add one additional key to the query to determine if more keys are available
        // results are then truncated to max_keys before sending to client
        let max_keys = max_keys + 1;

        let (query, params): (&str, Vec<&dyn ToSql>) = match (&prefix, start_after, continuation_token) {
            (None, None, None) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata ORDER BY key LIMIT $1;",
                vec![&max_keys],
            ),
            (None, None, Some(continuation_token)) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata ORDER BY key LIMIT $1, OFFSET ?2;",
                vec![&max_keys, &continuation_token.offset],
            ),
            (None, Some(start_after), None) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata WHERE key > ?1 ORDER BY key LIMIT $2;",
                vec![start_after, &max_keys],
            ),
            (None, Some(start_after), Some(continuation_token)) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata WHERE key > ?1 ORDER BY key LIMIT $2 OFFSET ?3;",
                vec![start_after, &max_keys, &continuation_token.offset],
            ),
            (Some(prefix), None, None) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata WHERE key LIKE ?1 ORDER BY key LIMIT $2;",
                vec![prefix, &max_keys],
            ),
            (Some(prefix), None, Some(continuation_token)) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata WHERE key LIKE ?1 ORDER BY key LIMIT ?2 OFFSET ?3;",
                vec![prefix,&max_keys, &continuation_token.offset],
            ),
            (Some(prefix), Some(start_after), None) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata WHERE key LIKE ?1 AND key > ?2 ORDER BY key LIMIT ?3;",
                vec![prefix, start_after, &max_keys],
            ),
            (Some(prefix), Some(start_after), Some(continuation_token)) => (
                "SELECT key, size, metadata, last_modified, md5 FROM metadata WHERE key LIKE ?1 AND key > ?2 ORDER BY key LIMIT ?3 OFFSET ?4;",
                vec![prefix, start_after, &max_keys, &continuation_token.offset],
            ),
        };

        let mut stmt = transaction.prepare_cached(query)?;

        let objects = stmt
            .query_map(params.as_slice(), |row| {
                Ok(KeyValue {
                    key: row.get(0)?,
                    value: None,
                    size: row.get(1)?,
                    metadata: row
                        .get::<_, Option<String>>(2)?
                        .map(|metadata| serde_json::from_str(&metadata))
                        .transpose()
                        .map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    last_modified: row.get(3)?,
                    md5: row.get(4)?,
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
            "SELECT metadata.key, data.value, metadata.size, metadata.metadata, metadata.last_modified, metadata.md5 FROM metadata INNER JOIN data ON metadata.key = data.key WHERE metadata.key = ?;",
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
        let mut stmt = transaction
            .prepare_cached("SELECT size, metadata, last_modified FROM metadata WHERE key = ?;")?;

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
        let mut stmt = transaction
                    .prepare_cached("INSERT INTO metadata (key, size, metadata, last_modified, md5) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(key) DO UPDATE SET size=excluded.size, metadata=excluded.metadata, last_modified=excluded.last_modified, md5=excluded.md5;")?;

        stmt.execute((
            &kv.key,
            kv.size,
            kv.metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            kv.last_modified,
            kv.md5,
        ))?;

        let mut stmt = transaction
                    .prepare_cached("INSERT INTO data (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value;")?;

        stmt.execute((kv.key, kv.value))
    }

    /// resolve object path under the virtual root
    pub(crate) fn try_delete_object(
        transaction: &Transaction,
        key: &str,
    ) -> rusqlite::Result<usize> {
        let mut stmt = transaction.prepare_cached("DELETE FROM metadata WHERE key = ?;")?;
        stmt.execute([key])?;

        let mut stmt = transaction.prepare_cached("DELETE FROM data WHERE key = ?;")?;
        stmt.execute([key])
    }

    pub(crate) fn try_delete_objects(
        transaction: &Transaction,
        keys: &[String],
    ) -> rusqlite::Result<Vec<String>> {
        let vars = repeat_vars(keys.len());

        let mut stmt =
            transaction.prepare(&format!("DELETE FROM metadata WHERE key IN ({vars});"))?;

        stmt.execute(rusqlite::params_from_iter(keys))?;

        let mut stmt = transaction.prepare(&format!(
            "DELETE FROM data WHERE key IN ({vars}) RETURNING key;"
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
        let mut stmt = transaction.prepare_cached("DELETE FROM metadata WHERE key LIKE ?;")?;

        stmt.execute([format!("{key}%")])?;

        let mut stmt = transaction.prepare_cached("DELETE FROM data WHERE key LIKE ?;")?;

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
            "INSERT INTO multipart_upload (upload_id, last_modified, bucket, key, access_key) VALUES (?1, ?2, ?3, ?4, ?5);",
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
        let mut stmt = transaction
            .prepare_cached("SELECT access_key FROM multipart_upload WHERE upload_id = $1 AND bucket = $2 AND key = $3;")?;

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
            "INSERT INTO multipart_upload_part (upload_id, last_modified, part_number, value, size, md5) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
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
            "SELECT last_modified, part_number, size FROM multipart_upload_part WHERE upload_id = ?1 ORDER BY part_number;",
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
                "SELECT last_modified, part_number, value, size, md5 FROM multipart_upload_part WHERE upload_id = ?1 ORDER BY part_number;",
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
        let mut stmt =
            transaction.prepare_cached("DELETE FROM multipart_upload WHERE upload_id = ?1;")?;
        stmt.execute([upload_id])?;

        Ok(())
    }

    pub(crate) fn try_delete_multipart_expired(
        transaction: &Transaction,
        expire_before: OffsetDateTime,
    ) -> rusqlite::Result<()> {
        let mut stmt = transaction.prepare_cached(
            "DELETE FROM multipart_upload WHERE DATETIME(last_modified) < DATETIME($1);",
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
