use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    str::FromStr,
};

use clap::ValueEnum;
use serde::Deserialize;

#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The base path where the `.sqlite3` files will be created.
    /// All `.sqlite3` files at this path will be loaded at startup and exposed via this service.
    #[serde(default = "default_root")]
    pub root: PathBuf,

    /// The IP address to listen on for this service. Use `0.0.0.0` to listen on all interfaces.
    #[serde(default = "default_host")]
    pub host: IpAddr,

    /// The port to listen on for this service.
    #[serde(default = "default_port")]
    pub port: u16,

    /// The access key ID that is used to authenticate for this service.
    pub access_key: Option<String>,

    /// The secret access key that is used to authenticate for this service.
    pub secret_key: Option<String>,

    #[serde(default = "default_concurrency_limit")]
    /// Enforces a limit on the concurrent number of requests the underlying service can handle.
    /// This can be tuned depending on infrastructure as SSD/HDD will deal with resource contention very differently.
    pub concurrency_limit: u16,

    /// Allow permissive Cross-Origin Resource Sharing (CORS) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    #[serde(default = "default_permissive_cors")]
    pub permissive_cors: bool,

    /// The domain to use to allow parsing virtual-hosted-style requests.
    pub domain_name: Option<String>,

    /// If this service should be read-only
    #[serde(default = "default_read_only")]
    pub read_only: bool,

    /// Service level `SQLite` configurations
    #[serde(flatten, default = "default_pragmas")]
    pub sqlite: Pragmas,

    /// Bucket specific configurations
    #[serde(default = "HashMap::new")]
    pub buckets: HashMap<String, Bucket>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            root: default_root(),
            host: default_host(),
            port: default_port(),
            access_key: None,
            secret_key: None,
            concurrency_limit: default_concurrency_limit(),
            permissive_cors: default_permissive_cors(),
            read_only: default_read_only(),
            domain_name: None,
            sqlite: default_pragmas(),
            buckets: HashMap::default(),
        }
    }
}

impl Config {
    #[must_use]
    pub fn read_only(&self, bucket: Option<&str>) -> bool {
        bucket
            .and_then(|bucket| self.buckets.get(bucket).and_then(|bucket| bucket.read_only))
            .unwrap_or(self.read_only)
    }

    #[must_use]
    pub fn journal_mode(&self, bucket: Option<&str>) -> JournalMode {
        bucket
            .and_then(|bucket| {
                self.buckets.get(bucket).and_then(|bucket| {
                    bucket
                        .sqlite
                        .as_ref()
                        .and_then(|sqlite| sqlite.journal_mode)
                })
            })
            .unwrap_or(self.sqlite.journal_mode)
    }

    #[must_use]
    pub fn synchronous(&self, bucket: Option<&str>) -> Synchronous {
        bucket
            .and_then(|bucket| {
                self.buckets
                    .get(bucket)
                    .and_then(|bucket| bucket.sqlite.as_ref().and_then(|sqlite| sqlite.synchronous))
            })
            .unwrap_or(self.sqlite.synchronous)
    }

    #[must_use]
    pub fn temp_store(&self, bucket: Option<&str>) -> TempStore {
        bucket
            .and_then(|bucket| {
                self.buckets
                    .get(bucket)
                    .and_then(|bucket| bucket.sqlite.as_ref().and_then(|sqlite| sqlite.temp_store))
            })
            .unwrap_or(self.sqlite.temp_store)
    }

    #[must_use]
    pub fn cache_size(&self, bucket: Option<&str>) -> u32 {
        bucket
            .and_then(|bucket| {
                self.buckets
                    .get(bucket)
                    .and_then(|bucket| bucket.sqlite.as_ref().and_then(|sqlite| sqlite.cache_size))
            })
            .unwrap_or(self.sqlite.cache_size)
    }

    #[must_use]
    pub fn to_sql(&self, bucket: Option<&str>) -> String {
        format!(
            "
            PRAGMA journal_mode={:?};
            PRAGMA synchronous={:?};
            PRAGMA temp_store={:?};
            PRAGMA cache_size=-{};
            PRAGMA query_only={};
            PRAGMA foreign_keys=true;
            PRAGMA auto_vacuum=INCREMENTAL;
        ",
            self.journal_mode(bucket),
            self.synchronous(bucket),
            self.temp_store(bucket),
            self.cache_size(bucket),
            self.read_only(bucket),
        )
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct Pragmas {
    /// Controls the `SQLite` `journal_mode` flag pragma.
    #[serde(default = "default_journal_mode")]
    pub journal_mode: JournalMode,

    /// Controls the `SQLite` `synchronous` pragma.
    #[serde(default = "default_synchronous")]
    pub synchronous: Synchronous,

    /// Controls the `SQLite` `temp_store` pragma.
    #[serde(default = "default_temp_store")]
    pub temp_store: TempStore,

    /// Controls the `SQLite` `cache_size` pragma in kilobytes.
    #[serde(default = "default_cache_size")]
    pub cache_size: u32,
}

impl Default for Pragmas {
    fn default() -> Self {
        Self {
            journal_mode: JournalMode::WAL,
            synchronous: Synchronous::NORMAL,
            temp_store: TempStore::MEMORY,
            cache_size: 67_108_864,
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct Bucket {
    /// If this bucket should be read-only
    pub read_only: Option<bool>,

    /// Bucket level `SQLite` configurations
    pub sqlite: Option<BucketPragmas>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct BucketPragmas {
    /// Controls the `SQLite` `journal_mode` flag pragma.
    pub journal_mode: Option<JournalMode>,

    /// Controls the `SQLite` `synchronous` pragma.
    pub synchronous: Option<Synchronous>,

    /// Controls the `SQLite` `temp_store` pragma.
    pub temp_store: Option<TempStore>,

    /// Controls the `SQLite` `cache_size` pragma in kilobytes.
    pub cache_size: Option<u32>,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
pub enum JournalMode {
    DELETE,
    TRUNCATE,
    PERSIST,
    MEMORY,
    #[default]
    WAL,
    OFF,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
pub enum Synchronous {
    OFF,
    #[default]
    NORMAL,
    FULL,
    EXTRA,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
pub enum TempStore {
    DEFAULT,
    FILE,
    #[default]
    MEMORY,
}

fn default_root() -> PathBuf {
    PathBuf::from_str(".").unwrap()
}

fn default_host() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))
}

fn default_port() -> u16 {
    8014
}

fn default_concurrency_limit() -> u16 {
    16
}

fn default_permissive_cors() -> bool {
    true
}

fn default_read_only() -> bool {
    false
}

fn default_pragmas() -> Pragmas {
    Pragmas::default()
}

fn default_journal_mode() -> JournalMode {
    JournalMode::default()
}

fn default_synchronous() -> Synchronous {
    Synchronous::default()
}

fn default_temp_store() -> TempStore {
    TempStore::default()
}

fn default_cache_size() -> u32 {
    67_108_864
}
