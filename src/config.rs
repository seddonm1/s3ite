use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

#[cfg(feature = "binary")]
use clap::{Parser, ValueEnum};
use serde::Deserialize;

/// Result type returned by binary configuration loading.
#[cfg(feature = "binary")]
pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

/// Errors raised while loading binary configuration.
#[cfg(feature = "binary")]
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A configuration file could not be read.
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    /// A configuration file could not be parsed as YAML.
    #[error("Yaml {0}")]
    Yaml(#[from] yaml_serde::Error),
}

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

    /// Controls the number of long-lived BLOB reader connections to `SQLite`.
    #[clap(long)]
    blob_readers: Option<usize>,

    /// Seconds a BLOB reader may stay blocked by an unconsumed response. Set to `0` to disable.
    #[clap(long)]
    blob_read_timeout_seconds: Option<u64>,

    /// Interval, in seconds, for background database maintenance. Set to `0` to disable.
    #[clap(long)]
    maintenance_interval_seconds: Option<u64>,

    /// Number of free pages to reclaim during each maintenance pass. Set to `0` to disable.
    #[clap(long)]
    maintenance_vacuum_pages: Option<u32>,

    /// TTL, in seconds, for inactive incomplete multipart uploads. Set to `0` to disable.
    #[clap(long)]
    multipart_upload_ttl_seconds: Option<u64>,

    /// TTL, in seconds, for completed multipart idempotency records. Set to `0` to disable.
    #[clap(long)]
    completed_multipart_upload_ttl_seconds: Option<u64>,
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

    /// Controls the number of long-lived BLOB reader connections to `SQLite`.
    #[serde(default = "default_blob_readers")]
    pub blob_readers: usize,

    /// Seconds a BLOB reader may stay blocked by an unconsumed response.
    ///
    /// Set to `0` to disable the timeout.
    #[serde(default = "default_blob_read_timeout_seconds")]
    pub blob_read_timeout_seconds: u64,

    /// Interval, in seconds, for background database maintenance.
    ///
    /// Set to `0` to disable periodic maintenance. Startup cleanup still runs.
    #[serde(default = "default_maintenance_interval_seconds")]
    pub maintenance_interval_seconds: u64,

    /// Number of free pages to reclaim during each maintenance pass.
    ///
    /// Set to `0` to skip `PRAGMA incremental_vacuum(N)`.
    #[serde(default = "default_maintenance_vacuum_pages")]
    pub maintenance_vacuum_pages: u32,

    /// TTL, in seconds, for inactive incomplete multipart uploads.
    ///
    /// Set to `0` to disable background cleanup of incomplete multipart
    /// uploads.
    #[serde(default = "default_multipart_upload_ttl_seconds")]
    pub multipart_upload_ttl_seconds: u64,

    /// TTL, in seconds, for completed multipart idempotency records.
    ///
    /// Set to `0` to retain completion records until explicitly removed.
    #[serde(default = "default_completed_upload_ttl_seconds")]
    pub completed_multipart_upload_ttl_seconds: u64,

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
            blob_readers: default_blob_readers(),
            blob_read_timeout_seconds: default_blob_read_timeout_seconds(),
            maintenance_interval_seconds: default_maintenance_interval_seconds(),
            maintenance_vacuum_pages: default_maintenance_vacuum_pages(),
            multipart_upload_ttl_seconds: default_multipart_upload_ttl_seconds(),
            completed_multipart_upload_ttl_seconds: default_completed_upload_ttl_seconds(),
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
    pub fn from_args() -> ConfigResult<Self> {
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
    fn from_config_args(args: ConfigArgs) -> ConfigResult<Self> {
        let mut config = args
            .config
            .as_ref()
            .map(|path| {
                let config = std::fs::read_to_string(path)?;
                Ok::<_, ConfigError>(yaml_serde::from_str::<Self>(&config)?)
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
        if let Some(blob_readers) = args.blob_readers {
            self.blob_readers = blob_readers;
        }
        if let Some(blob_read_timeout_seconds) = args.blob_read_timeout_seconds {
            self.blob_read_timeout_seconds = blob_read_timeout_seconds;
        }
        if let Some(maintenance_interval_seconds) = args.maintenance_interval_seconds {
            self.maintenance_interval_seconds = maintenance_interval_seconds;
        }
        if let Some(maintenance_vacuum_pages) = args.maintenance_vacuum_pages {
            self.maintenance_vacuum_pages = maintenance_vacuum_pages;
        }
        if let Some(multipart_upload_ttl_seconds) = args.multipart_upload_ttl_seconds {
            self.multipart_upload_ttl_seconds = multipart_upload_ttl_seconds;
        }
        if let Some(completed_multipart_upload_ttl_seconds) =
            args.completed_multipart_upload_ttl_seconds
        {
            self.completed_multipart_upload_ttl_seconds = completed_multipart_upload_ttl_seconds;
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

fn default_blob_readers() -> usize {
    16
}

fn default_blob_read_timeout_seconds() -> u64 {
    120
}

fn default_maintenance_interval_seconds() -> u64 {
    60
}

fn default_maintenance_vacuum_pages() -> u32 {
    100
}

fn default_multipart_upload_ttl_seconds() -> u64 {
    86_400
}

fn default_completed_upload_ttl_seconds() -> u64 {
    86_400
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

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

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
    fn readers_default_and_deserialize_override() -> TestResult {
        assert_eq!(Config::default().readers, 4);
        assert_eq!(Config::default().blob_readers, 16);
        assert_eq!(Config::default().blob_read_timeout_seconds, 120);

        let config: Config = yaml_serde::from_str(
            "
            readers: 7
            blob_readers: 21
            blob_read_timeout_seconds: 42
            ",
        )?;
        assert_eq!(config.readers, 7);
        assert_eq!(config.blob_readers, 21);
        assert_eq!(config.blob_read_timeout_seconds, 42);

        let config: Config = yaml_serde::from_str("blob_read_timeout_seconds: 0\n")?;
        assert_eq!(config.blob_read_timeout_seconds, 0);

        Ok(())
    }

    #[test]
    fn maintenance_interval_default_and_deserialize_override() -> TestResult {
        assert_eq!(Config::default().maintenance_interval_seconds, 60);
        assert_eq!(Config::default().maintenance_vacuum_pages, 100);

        let config: Config = yaml_serde::from_str(
            "
            maintenance_interval_seconds: 300
            maintenance_vacuum_pages: 42
            ",
        )?;
        assert_eq!(config.maintenance_interval_seconds, 300);
        assert_eq!(config.maintenance_vacuum_pages, 42);

        let config: Config = yaml_serde::from_str(
            "
            maintenance_interval_seconds: 0
            maintenance_vacuum_pages: 0
            ",
        )?;
        assert_eq!(config.maintenance_interval_seconds, 0);
        assert_eq!(config.maintenance_vacuum_pages, 0);

        Ok(())
    }

    #[test]
    fn multipart_cleanup_ttls_default_enabled_and_zero_disables() -> TestResult {
        let config = Config::default();
        assert_eq!(config.multipart_upload_ttl_seconds, 86_400);
        assert_eq!(config.completed_multipart_upload_ttl_seconds, 86_400);

        let config: Config = yaml_serde::from_str(
            "
            multipart_upload_ttl_seconds: 3600
            completed_multipart_upload_ttl_seconds: 86400
            ",
        )?;
        assert_eq!(config.multipart_upload_ttl_seconds, 3600);
        assert_eq!(config.completed_multipart_upload_ttl_seconds, 86400);

        let config: Config = yaml_serde::from_str(
            "
            multipart_upload_ttl_seconds: 0
            completed_multipart_upload_ttl_seconds: 0
            ",
        )?;
        assert_eq!(config.multipart_upload_ttl_seconds, 0);
        assert_eq!(config.completed_multipart_upload_ttl_seconds, 0);

        Ok(())
    }

    #[cfg(feature = "binary")]
    #[test]
    fn cli_args_override_yaml_config() -> TestResult {
        use clap::Parser as _;

        let path = std::env::temp_dir().join(format!("s3ite-{}.yaml", uuid::Uuid::new_v4()));
        std::fs::write(
            &path,
            "
            port: 9000
            readers: 2
            blob_readers: 6
            blob_read_timeout_seconds: 90
            maintenance_interval_seconds: 60
            maintenance_vacuum_pages: 50
            cache_size: 512
            multipart_upload_ttl_seconds: 100
            completed_multipart_upload_ttl_seconds: 200
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
            "--blob-readers",
            "8",
            "--blob-read-timeout-seconds",
            "180",
            "--maintenance-interval-seconds",
            "120",
            "--maintenance-vacuum-pages",
            "75",
            "--multipart-upload-ttl-seconds",
            "300",
            "--completed-multipart-upload-ttl-seconds",
            "400",
        ])?;
        let config = Config::from_config_args(args)?;

        assert_eq!(config.port, 9001);
        assert_eq!(config.readers, 2);
        assert_eq!(config.blob_readers, 8);
        assert_eq!(config.blob_read_timeout_seconds, 180);
        assert_eq!(config.maintenance_interval_seconds, 120);
        assert_eq!(config.maintenance_vacuum_pages, 75);
        assert_eq!(config.sqlite.cache_size, 1024);
        assert_eq!(config.multipart_upload_ttl_seconds, 300);
        assert_eq!(config.completed_multipart_upload_ttl_seconds, 400);

        std::fs::remove_file(path).ok();
        Ok(())
    }
}
