use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

#[cfg(feature = "binary")]
use clap::{Parser, ValueEnum};
use serde::Deserialize;

#[cfg(feature = "binary")]
use crate::error::{Result, S3ite};

/// Command-line arguments that override YAML or default configuration.
#[cfg(feature = "binary")]
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct ConfigArgs {
    /// The base path where the `.sqlite3` files will be created.
    /// All `.sqlite3` files at this path will be loaded at startup and exposed via this service.
    #[clap(long)]
    root: Option<PathBuf>,

    /// Path to a YAML configuration file.
    #[clap(long)]
    config: Option<PathBuf>,

    /// The IP address to listen on for this service. Use `0.0.0.0` to listen on all interfaces.
    #[clap(long)]
    host: Option<IpAddr>,

    /// The port to listen on for this service.
    #[clap(long)]
    port: Option<u16>,

    /// Allow permissive Cross-Origin Resource Sharing (`CORS`) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    #[clap(long)]
    permissive_cors: Option<bool>,

    /// The access key ID that is used to authenticate for this service.
    #[clap(long, requires = "secret_key")]
    access_key: Option<String>,

    /// The secret access key that is used to authenticate for this service.
    #[clap(long, requires = "access_key")]
    secret_key: Option<String>,

    /// The domain to use to allow parsing virtual-hosted-style requests.
    #[clap(long)]
    domain_name: Option<String>,

    /// Enforces a limit on the concurrent number of requests the underlying service can handle.
    /// This can be tuned depending on infrastructure as `SSD`/`HDD` will deal with resource contention very differently.
    #[clap(long)]
    concurrency_limit: Option<u16>,

    /// If this service should be read-only.
    #[clap(long)]
    read_only: Option<bool>,

    /// Controls the `SQLite` `journal_mode` flag pragma.
    #[clap(long)]
    journal_mode: Option<JournalMode>,

    /// Controls the `SQLite` `synchronous` pragma.
    #[clap(long)]
    synchronous: Option<Synchronous>,

    /// Controls the `SQLite` `temp_store` pragma.
    #[clap(long)]
    temp_store: Option<TempStore>,

    /// Controls the `SQLite` `cache_size` pragma in kilobytes.
    #[clap(long)]
    cache_size: Option<u32>,

    /// Controls the number of reader connections to `SQLite`.
    #[clap(long)]
    readers: Option<usize>,
}

/// Runtime configuration for the s3ite service.
///
/// Values can be loaded from YAML and, when the `binary` feature is enabled,
/// selectively overridden by command-line arguments.
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

    /// Enforces a limit on the concurrent number of requests the underlying service can handle.
    /// This can be tuned depending on infrastructure as SSD/HDD will deal with resource contention very differently.
    #[serde(default = "default_concurrency_limit")]
    pub concurrency_limit: u16,

    /// Controls the number of reader connections to `SQLite`.
    #[serde(default = "default_readers")]
    pub readers: usize,

    /// Allow permissive Cross-Origin Resource Sharing (CORS) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    #[serde(default = "default_permissive_cors")]
    pub permissive_cors: bool,

    /// The domain to use to allow parsing virtual-hosted-style requests.
    pub domain_name: Option<String>,

    /// Whether this service should reject write operations by default.
    #[serde(default = "default_read_only")]
    pub read_only: bool,

    /// Service-level `SQLite` pragmas applied to each bucket database.
    #[serde(flatten, default = "default_pragmas")]
    pub sqlite: Pragmas,

    /// Bucket-specific settings keyed by bucket name.
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
            readers: default_readers(),
            permissive_cors: default_permissive_cors(),
            read_only: default_read_only(),
            domain_name: None,
            sqlite: default_pragmas(),
            buckets: HashMap::default(),
        }
    }
}

impl Config {
    /// Loads configuration from command-line arguments and an optional YAML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured YAML file cannot be read or parsed.
    #[cfg(feature = "binary")]
    pub fn from_args() -> Result<Self> {
        Self::from_config_args(ConfigArgs::parse())
    }

    /// Loads configuration from parsed command-line arguments.
    ///
    /// If a YAML config path is provided, values from the file are loaded first
    /// and then overridden by any command-line values present in `args`.
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML file cannot be read or parsed.
    #[cfg(feature = "binary")]
    fn from_config_args(args: ConfigArgs) -> Result<Self> {
        let mut config = args
            .config
            .as_ref()
            .map(|path| {
                let config = std::fs::read_to_string(path)?;
                Ok::<_, S3ite>(yaml_serde::from_str::<Self>(&config)?)
            })
            .transpose()?
            .unwrap_or_default();

        config.apply_args(args);

        Ok(config)
    }

    /// Applies command-line values over the current configuration.
    #[cfg(feature = "binary")]
    fn apply_args(&mut self, args: ConfigArgs) {
        if let Some(root) = args.root {
            self.root = root;
        }
        if let Some(host) = args.host {
            self.host = host;
        }
        if let Some(port) = args.port {
            self.port = port;
        }
        if let Some(access_key) = args.access_key {
            self.access_key = Some(access_key);
        }
        if let Some(secret_key) = args.secret_key {
            self.secret_key = Some(secret_key);
        }
        if let Some(permissive_cors) = args.permissive_cors {
            self.permissive_cors = permissive_cors;
        }
        if let Some(domain_name) = args.domain_name {
            self.domain_name = Some(domain_name);
        }
        if let Some(concurrency_limit) = args.concurrency_limit {
            self.concurrency_limit = concurrency_limit;
        }
        if let Some(readers) = args.readers {
            self.readers = readers;
        }
        if let Some(read_only) = args.read_only {
            self.read_only = read_only;
        }
        if let Some(journal_mode) = args.journal_mode {
            self.sqlite.journal_mode = journal_mode;
        }
        if let Some(synchronous) = args.synchronous {
            self.sqlite.synchronous = synchronous;
        }
        if let Some(temp_store) = args.temp_store {
            self.sqlite.temp_store = temp_store;
        }
        if let Some(cache_size) = args.cache_size {
            self.sqlite.cache_size = cache_size;
        }
    }

    /// Returns whether a bucket should reject write operations.
    ///
    /// A bucket-level `read_only` override wins when present; otherwise the
    /// service-level `read_only` value is used.
    #[must_use]
    pub fn read_only(&self, bucket: Option<&str>) -> bool {
        bucket
            .and_then(|bucket| self.buckets.get(bucket).and_then(|bucket| bucket.read_only))
            .unwrap_or(self.read_only)
    }

    /// Returns the effective `SQLite` `journal_mode` for a bucket connection.
    ///
    /// A bucket-level pragma override wins when present; otherwise the
    /// service-level pragma is used.
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

    /// Returns the effective `SQLite` `synchronous` mode for a bucket connection.
    ///
    /// A bucket-level pragma override wins when present; otherwise the
    /// service-level pragma is used.
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

    /// Returns the effective `SQLite` `temp_store` mode for a bucket connection.
    ///
    /// A bucket-level pragma override wins when present; otherwise the
    /// service-level pragma is used.
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

    /// Returns the effective `SQLite` cache size in kibibytes.
    ///
    /// A bucket-level pragma override wins when present; otherwise the
    /// service-level pragma is used.
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

    /// Renders the `SQLite` PRAGMA statements for a bucket connection.
    ///
    /// `SQLite` interprets negative `cache_size` values as kibibytes, so the
    /// configured positive KiB value is emitted as `PRAGMA cache_size=-N`.
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

/// `SQLite` PRAGMA settings applied at service level.
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
            cache_size: default_cache_size(),
        }
    }
}

/// Bucket-specific configuration overrides.
#[derive(Clone, Deserialize, Debug)]
pub struct Bucket {
    /// Whether this bucket should reject write operations.
    pub read_only: Option<bool>,

    /// Bucket-level `SQLite` pragma overrides.
    pub sqlite: Option<BucketPragmas>,
}

/// Optional `SQLite` PRAGMA overrides for one bucket.
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

/// `SQLite` `journal_mode` values supported by configuration.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[cfg_attr(feature = "binary", derive(ValueEnum))]
pub enum JournalMode {
    /// Use `SQLite`'s rollback journal and delete it after each transaction.
    DELETE,
    /// Use `SQLite`'s rollback journal and truncate it after each transaction.
    TRUNCATE,
    /// Use `SQLite`'s rollback journal and keep it after each transaction.
    PERSIST,
    /// Store the rollback journal in memory.
    MEMORY,
    /// Use `SQLite` write-ahead logging.
    #[default]
    WAL,
    /// Disable rollback journaling.
    OFF,
}

/// `SQLite` `synchronous` values supported by configuration.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[cfg_attr(feature = "binary", derive(ValueEnum))]
pub enum Synchronous {
    /// Let `SQLite` skip sync calls for maximum throughput and lowest durability.
    OFF,
    /// Sync at critical moments while avoiding the extra checks of `FULL`.
    #[default]
    NORMAL,
    /// Sync after each critical write for stronger durability guarantees.
    FULL,
    /// Use `SQLite`'s strongest sync setting with additional directory syncs.
    EXTRA,
}

/// `SQLite` `temp_store` values supported by configuration.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[cfg_attr(feature = "binary", derive(ValueEnum))]
pub enum TempStore {
    /// Let `SQLite` choose the temporary storage location.
    DEFAULT,
    /// Store temporary tables and indexes in files.
    FILE,
    /// Store temporary tables and indexes in memory.
    #[default]
    MEMORY,
}

fn default_root() -> PathBuf {
    PathBuf::from(".")
}

fn default_host() -> IpAddr {
    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

fn default_port() -> u16 {
    8014
}

fn default_concurrency_limit() -> u16 {
    16
}

fn default_readers() -> usize {
    4
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
    65_536
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sqlite_cache_size_is_kib() {
        let config = Config::default();
        assert_eq!(config.sqlite.cache_size, 65_536);
        assert!(config.to_sql(None).contains("PRAGMA cache_size=-65536;"));
    }

    #[test]
    fn bucket_cache_size_overrides_service_default() {
        let mut config = Config::default();
        config.buckets.insert(
            "bucket".to_string(),
            Bucket {
                read_only: None,
                sqlite: Some(BucketPragmas {
                    journal_mode: None,
                    synchronous: None,
                    temp_store: None,
                    cache_size: Some(512),
                }),
            },
        );

        assert!(
            config
                .to_sql(Some("bucket"))
                .contains("PRAGMA cache_size=-512;")
        );
    }

    #[test]
    fn readers_default_and_deserialize_override()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        assert_eq!(Config::default().readers, 4);

        let config: Config = yaml_serde::from_str("readers: 7\n")?;
        assert_eq!(config.readers, 7);

        Ok(())
    }

    #[cfg(feature = "binary")]
    #[test]
    fn cli_args_override_yaml_config() -> std::result::Result<(), Box<dyn std::error::Error>> {
        use clap::Parser as _;

        let path = std::env::temp_dir().join(format!("s3ite-{}.yaml", uuid::Uuid::new_v4()));
        std::fs::write(
            &path,
            "
            port: 9000
            readers: 2
            cache_size: 512
            ",
        )?;

        let path_arg = path.to_string_lossy().into_owned();

        let args = ConfigArgs::try_parse_from([
            "s3ite",
            "--config",
            &path_arg,
            "--port",
            "9001",
            "--cache-size",
            "1024",
        ])?;
        let config = Config::from_config_args(args)?;

        assert_eq!(config.port, 9001);
        assert_eq!(config.readers, 2);
        assert_eq!(config.sqlite.cache_size, 1024);

        std::fs::remove_file(path).ok();
        Ok(())
    }
}
