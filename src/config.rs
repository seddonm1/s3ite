use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    time::Duration,
};

use clap::{Parser, ValueEnum};
use hyper::http::{HeaderName, HeaderValue, Method};
use serde::{Deserialize, de};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

/// Result type returned by binary configuration loading.
pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

/// Errors raised while loading binary configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A configuration file could not be read.
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    /// A configuration file could not be parsed as YAML.
    #[error("Yaml {0}")]
    Yaml(#[from] yaml_serde::Error),
    /// A configuration value is not valid.
    #[error("configuration error: {0}")]
    Configuration(String),
}

/// Command-line arguments that override YAML or default configuration.
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

    /// Configures Cross-Origin Resource Sharing (`CORS`) handling.
    #[clap(long, value_enum, conflicts_with = "permissive_cors")]
    cors: Option<CliCorsMode>,

    /// Allow permissive Cross-Origin Resource Sharing (`CORS`) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    #[clap(long, conflicts_with = "cors")]
    permissive_cors: Option<bool>,

    /// The access key ID that is used to authenticate for this service.
    #[clap(long, requires = "secret_key")]
    access_key: Option<String>,

    /// The secret access key that is used to authenticate for this service.
    #[clap(long, requires = "access_key")]
    secret_key: Option<String>,

    /// Base64-encoded 32-byte master key used for SSE-S3 object encryption.
    #[clap(long)]
    sse_s3_key: Option<String>,

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

    /// Controls the `SQLite` `busy_timeout` pragma in milliseconds.
    #[clap(long)]
    busy_timeout_milliseconds: Option<u64>,

    /// Controls the `SQLite` `journal_size_limit` pragma in bytes.
    #[clap(long)]
    journal_size_limit: Option<i64>,

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

/// CLI-accessible `CORS` modes.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum CliCorsMode {
    /// Disable CORS response handling.
    Disabled,
    /// Mirror request origins, methods, and headers and allow credentials.
    Permissive,
}

impl From<CliCorsMode> for CorsMode {
    fn from(mode: CliCorsMode) -> Self {
        match mode {
            CliCorsMode::Disabled => Self::Disabled,
            CliCorsMode::Permissive => Self::Permissive,
        }
    }
}

/// Runtime configuration for the s3ite service.
///
/// Values can be loaded from YAML and selectively overridden by command-line
/// arguments.
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

    /// Base64-encoded 32-byte master key used for SSE-S3 object encryption.
    pub sse_s3_key: Option<String>,

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

    /// Cross-Origin Resource Sharing (`CORS`) handling.
    #[serde(default = "default_cors")]
    pub cors: CorsMode,

    /// Deprecated compatibility alias for `cors.mode`.
    #[serde(default)]
    pub(crate) permissive_cors: Option<bool>,

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
            sse_s3_key: None,
            concurrency_limit: default_concurrency_limit(),
            readers: default_readers(),
            blob_readers: default_blob_readers(),
            blob_read_timeout_seconds: default_blob_read_timeout_seconds(),
            maintenance_interval_seconds: default_maintenance_interval_seconds(),
            maintenance_vacuum_pages: default_maintenance_vacuum_pages(),
            multipart_upload_ttl_seconds: default_multipart_upload_ttl_seconds(),
            completed_multipart_upload_ttl_seconds: default_completed_upload_ttl_seconds(),
            cors: default_cors(),
            permissive_cors: None,
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
        config.validate()?;

        Ok(config)
    }

    /// Applies command-line values over the current configuration.
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
        if let Some(sse_s3_key) = args.sse_s3_key {
            self.sse_s3_key = Some(sse_s3_key);
        }
        if let Some(cors) = args.cors {
            self.cors = cors.into();
            self.permissive_cors = None;
        }
        if let Some(permissive_cors) = args.permissive_cors {
            self.cors = if permissive_cors {
                CorsMode::Permissive
            } else {
                CorsMode::Disabled
            };
            self.permissive_cors = None;
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
        if let Some(busy_timeout_milliseconds) = args.busy_timeout_milliseconds {
            self.sqlite.busy_timeout_milliseconds = busy_timeout_milliseconds;
        }
        if let Some(journal_size_limit) = args.journal_size_limit {
            self.sqlite.journal_size_limit = journal_size_limit;
        }
    }

    /// Returns the effective `CORS` mode, including deprecated aliases.
    #[must_use]
    pub(crate) fn cors_mode(&self) -> CorsMode {
        self.permissive_cors.map_or_else(
            || self.cors.clone(),
            |permissive| {
                if permissive {
                    CorsMode::Permissive
                } else {
                    CorsMode::Disabled
                }
            },
        )
    }

    /// Validates semantic configuration constraints.
    fn validate(&self) -> ConfigResult<()> {
        self.cors_mode()
            .validate()
            .map_err(ConfigError::Configuration)?;
        if let Some(key) = self.sse_s3_key.as_deref() {
            crate::sse::parse_master_key(key).map_err(ConfigError::Configuration)?;
        }
        self.cors_layer().map(|_| ())
    }

    /// Returns the decoded SSE-S3 master key when configured.
    pub(crate) fn sse_s3_master_key(&self) -> Option<[u8; 32]> {
        self.sse_s3_key
            .as_deref()
            .and_then(|key| crate::sse::parse_master_key(key).ok())
    }

    /// Builds the configured `CORS` middleware layer.
    pub(crate) fn cors_layer(&self) -> ConfigResult<Option<CorsLayer>> {
        self.cors_mode().cors_layer()
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

    /// Returns the effective `SQLite` busy timeout in milliseconds.
    ///
    /// A bucket-level pragma override wins when present; otherwise the
    /// service-level pragma is used.
    #[must_use]
    pub fn busy_timeout_milliseconds(&self, bucket: Option<&str>) -> u64 {
        bucket
            .and_then(|bucket| {
                self.buckets.get(bucket).and_then(|bucket| {
                    bucket
                        .sqlite
                        .as_ref()
                        .and_then(|sqlite| sqlite.busy_timeout_milliseconds)
                })
            })
            .unwrap_or(self.sqlite.busy_timeout_milliseconds)
    }

    /// Returns the effective `SQLite` journal/WAL size limit in bytes.
    ///
    /// A bucket-level pragma override wins when present; otherwise the
    /// service-level pragma is used.
    #[must_use]
    pub fn journal_size_limit(&self, bucket: Option<&str>) -> i64 {
        bucket
            .and_then(|bucket| {
                self.buckets.get(bucket).and_then(|bucket| {
                    bucket
                        .sqlite
                        .as_ref()
                        .and_then(|sqlite| sqlite.journal_size_limit)
                })
            })
            .unwrap_or(self.sqlite.journal_size_limit)
    }
}

/// Cross-Origin Resource Sharing behavior.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CorsMode {
    /// Do not add CORS response headers.
    Disabled,
    /// Mirror request origins, methods, and headers and allow credentials.
    #[default]
    Permissive,
    /// Apply explicit CORS rules from configuration.
    Custom(CorsConfig),
}

impl<'de> Deserialize<'de> for CorsMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Mode {
            Disabled,
            Permissive,
            Custom,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawCorsMode {
            mode: Mode,
            allow_origins: Option<Vec<String>>,
            allow_methods: Option<Vec<String>>,
            allow_headers: Option<Vec<String>>,
            expose_headers: Option<Vec<String>>,
            allow_credentials: Option<bool>,
            allow_private_network: Option<bool>,
            max_age_seconds: Option<u64>,
        }

        let raw = RawCorsMode::deserialize(deserializer)?;
        let has_custom_fields = raw.allow_origins.is_some()
            || raw.allow_methods.is_some()
            || raw.allow_headers.is_some()
            || raw.expose_headers.is_some()
            || raw.allow_credentials.is_some()
            || raw.allow_private_network.is_some()
            || raw.max_age_seconds.is_some();

        match raw.mode {
            Mode::Disabled | Mode::Permissive if has_custom_fields => Err(de::Error::custom(
                "custom CORS fields require `cors.mode: custom`",
            )),
            Mode::Disabled => Ok(Self::Disabled),
            Mode::Permissive => Ok(Self::Permissive),
            Mode::Custom => Ok(Self::Custom(CorsConfig {
                allow_origins: raw
                    .allow_origins
                    .ok_or_else(|| de::Error::missing_field("allow_origins"))?,
                allow_methods: raw.allow_methods.unwrap_or_else(default_cors_allow_methods),
                allow_headers: raw.allow_headers.unwrap_or_else(default_cors_allow_headers),
                expose_headers: raw
                    .expose_headers
                    .unwrap_or_else(default_cors_expose_headers),
                allow_credentials: raw.allow_credentials.unwrap_or(false),
                allow_private_network: raw.allow_private_network.unwrap_or(false),
                max_age_seconds: raw.max_age_seconds,
            })),
        }
    }
}

impl CorsMode {
    /// Checks whether the selected CORS configuration can be served safely.
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Disabled | Self::Permissive => Ok(()),
            Self::Custom(config) => config.validate(),
        }
    }

    /// Builds the `tower-http` CORS layer represented by this mode.
    fn cors_layer(&self) -> ConfigResult<Option<CorsLayer>> {
        match self {
            Self::Disabled => Ok(None),
            Self::Permissive => Ok(Some(CorsLayer::very_permissive())),
            Self::Custom(config) => config.cors_layer().map(Some),
        }
    }
}

/// Explicit Cross-Origin Resource Sharing rules.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CorsConfig {
    /// Origins allowed to access this service. Use `*` to allow any origin.
    pub allow_origins: Vec<String>,
    /// HTTP methods allowed by preflight responses.
    #[serde(default = "default_cors_allow_methods")]
    pub allow_methods: Vec<String>,
    /// Request headers allowed by preflight responses.
    #[serde(default = "default_cors_allow_headers")]
    pub allow_headers: Vec<String>,
    /// Response headers exposed to browser clients.
    #[serde(default = "default_cors_expose_headers")]
    pub expose_headers: Vec<String>,
    /// Whether browser credentials are allowed.
    #[serde(default)]
    pub allow_credentials: bool,
    /// Whether private-network preflight requests are allowed.
    #[serde(default)]
    pub allow_private_network: bool,
    /// Browser preflight cache duration in seconds.
    pub max_age_seconds: Option<u64>,
}

impl CorsConfig {
    /// Checks for invalid wildcard and required-field combinations.
    fn validate(&self) -> Result<(), String> {
        if self.allow_origins.is_empty() {
            return Err("custom CORS requires at least one allowed origin".to_string());
        }
        if self.allow_methods.is_empty() {
            return Err("custom CORS requires at least one allowed method".to_string());
        }

        for (field, values) in [
            ("allow_origins", self.allow_origins.as_slice()),
            ("allow_methods", self.allow_methods.as_slice()),
            ("allow_headers", self.allow_headers.as_slice()),
            ("expose_headers", self.expose_headers.as_slice()),
        ] {
            if has_mixed_wildcard(values) {
                return Err(format!(
                    "custom CORS `{field}` cannot mix `*` with explicit values"
                ));
            }
        }

        if self.allow_credentials {
            for (field, values) in [
                ("allow_origins", self.allow_origins.as_slice()),
                ("allow_methods", self.allow_methods.as_slice()),
                ("allow_headers", self.allow_headers.as_slice()),
                ("expose_headers", self.expose_headers.as_slice()),
            ] {
                if is_wildcard_list(values) {
                    return Err(format!(
                        "custom CORS `{field}` cannot be `*` when credentials are allowed"
                    ));
                }
            }
        }

        Ok(())
    }

    /// Builds the explicit `tower-http` CORS layer.
    fn cors_layer(&self) -> ConfigResult<CorsLayer> {
        let mut layer = CorsLayer::new()
            .allow_credentials(self.allow_credentials)
            .allow_private_network(self.allow_private_network);

        if is_wildcard_list(&self.allow_origins) {
            layer = layer.allow_origin(Any);
        } else {
            layer = layer.allow_origin(AllowOrigin::list(parse_header_values(
                &self.allow_origins,
                "cors.allow_origins",
            )?));
        }

        if is_wildcard_list(&self.allow_methods) {
            layer = layer.allow_methods(Any);
        } else {
            layer = layer.allow_methods(parse_methods(&self.allow_methods)?);
        }

        if is_wildcard_list(&self.allow_headers) {
            layer = layer.allow_headers(Any);
        } else if !self.allow_headers.is_empty() {
            layer = layer.allow_headers(parse_header_names(
                &self.allow_headers,
                "cors.allow_headers",
            )?);
        }

        if is_wildcard_list(&self.expose_headers) {
            layer = layer.expose_headers(Any);
        } else if !self.expose_headers.is_empty() {
            layer = layer.expose_headers(parse_header_names(
                &self.expose_headers,
                "cors.expose_headers",
            )?);
        }

        if let Some(max_age_seconds) = self.max_age_seconds {
            layer = layer.max_age(Duration::from_secs(max_age_seconds));
        }

        Ok(layer)
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

    /// Controls the `SQLite` `busy_timeout` pragma in milliseconds.
    #[serde(default = "default_busy_timeout_milliseconds")]
    pub busy_timeout_milliseconds: u64,

    /// Controls the `SQLite` `journal_size_limit` pragma in bytes.
    #[serde(default = "default_journal_size_limit")]
    pub journal_size_limit: i64,
}

impl Default for Pragmas {
    fn default() -> Self {
        Self {
            journal_mode: JournalMode::Wal,
            synchronous: Synchronous::Normal,
            temp_store: TempStore::Memory,
            cache_size: default_cache_size(),
            busy_timeout_milliseconds: default_busy_timeout_milliseconds(),
            journal_size_limit: default_journal_size_limit(),
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

    /// Controls the `SQLite` `busy_timeout` pragma in milliseconds.
    pub busy_timeout_milliseconds: Option<u64>,

    /// Controls the `SQLite` `journal_size_limit` pragma in bytes.
    pub journal_size_limit: Option<i64>,
}

/// `SQLite` `journal_mode` values supported by configuration.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum JournalMode {
    /// Use `SQLite`'s rollback journal and delete it after each transaction.
    #[serde(alias = "DELETE")]
    Delete,
    /// Use `SQLite`'s rollback journal and truncate it after each transaction.
    #[serde(alias = "TRUNCATE")]
    Truncate,
    /// Use `SQLite`'s rollback journal and keep it after each transaction.
    #[serde(alias = "PERSIST")]
    Persist,
    /// Store the rollback journal in memory.
    #[serde(alias = "MEMORY")]
    Memory,
    /// Use `SQLite` write-ahead logging.
    #[default]
    #[serde(alias = "WAL")]
    Wal,
    /// Disable rollback journaling.
    #[serde(alias = "OFF")]
    Off,
}

/// `SQLite` `synchronous` values supported by configuration.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Synchronous {
    /// Let `SQLite` skip sync calls for maximum throughput and lowest durability.
    #[serde(alias = "OFF")]
    Off,
    /// Sync at critical moments while avoiding the extra checks of `FULL`.
    #[default]
    #[serde(alias = "NORMAL")]
    Normal,
    /// Sync after each critical write for stronger durability guarantees.
    #[serde(alias = "FULL")]
    Full,
    /// Use `SQLite`'s strongest sync setting with additional directory syncs.
    #[serde(alias = "EXTRA")]
    Extra,
}

/// `SQLite` `temp_store` values supported by configuration.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum TempStore {
    /// Let `SQLite` choose the temporary storage location.
    #[serde(alias = "DEFAULT")]
    Default,
    /// Store temporary tables and indexes in files.
    #[serde(alias = "FILE")]
    File,
    /// Store temporary tables and indexes in memory.
    #[default]
    #[serde(alias = "MEMORY")]
    Memory,
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

fn default_cors() -> CorsMode {
    CorsMode::default()
}

fn default_cors_allow_methods() -> Vec<String> {
    ["GET", "HEAD", "PUT", "POST", "DELETE"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_cors_allow_headers() -> Vec<String> {
    ["*"].into_iter().map(str::to_string).collect()
}

fn default_cors_expose_headers() -> Vec<String> {
    [
        "ETag",
        "x-amz-checksum-crc32",
        "x-amz-checksum-crc32c",
        "x-amz-checksum-crc64nvme",
        "x-amz-checksum-sha1",
        "x-amz-checksum-sha256",
        "x-amz-delete-marker",
        "x-amz-request-id",
        "x-amz-version-id",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn is_wildcard_list(values: &[String]) -> bool {
    values.len() == 1 && values[0] == "*"
}

fn has_mixed_wildcard(values: &[String]) -> bool {
    values.len() > 1 && values.iter().any(|value| value == "*")
}

fn parse_header_values(values: &[String], field: &str) -> ConfigResult<Vec<HeaderValue>> {
    values
        .iter()
        .map(|value| {
            HeaderValue::from_str(value).map_err(|err| {
                ConfigError::Configuration(format!(
                    "{field} contains invalid value `{value}`: {err}"
                ))
            })
        })
        .collect()
}

fn parse_header_names(values: &[String], field: &str) -> ConfigResult<Vec<HeaderName>> {
    values
        .iter()
        .map(|value| {
            HeaderName::from_bytes(value.as_bytes()).map_err(|err| {
                ConfigError::Configuration(format!(
                    "{field} contains invalid header `{value}`: {err}"
                ))
            })
        })
        .collect()
}

fn parse_methods(values: &[String]) -> ConfigResult<Vec<Method>> {
    values
        .iter()
        .map(|value| {
            Method::from_bytes(value.as_bytes()).map_err(|err| {
                ConfigError::Configuration(format!(
                    "cors.allow_methods contains invalid method `{value}`: {err}"
                ))
            })
        })
        .collect()
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

fn default_busy_timeout_milliseconds() -> u64 {
    5_000
}

fn default_journal_size_limit() -> i64 {
    64 * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn default_sqlite_pragmas_are_storage_friendly() {
        let config = Config::default();
        assert_eq!(config.sqlite.cache_size, 65_536);
        assert_eq!(config.cache_size(None), 65_536);
        assert_eq!(config.sqlite.busy_timeout_milliseconds, 5_000);
        assert_eq!(config.busy_timeout_milliseconds(None), 5_000);
        assert_eq!(config.sqlite.journal_size_limit, 64 * 1024 * 1024);
        assert_eq!(config.journal_size_limit(None), 64 * 1024 * 1024);
    }

    #[test]
    fn bucket_sqlite_pragmas_override_service_defaults() {
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
                    busy_timeout_milliseconds: Some(250),
                    journal_size_limit: Some(4096),
                }),
            },
        );

        assert_eq!(config.cache_size(Some("bucket")), 512);
        assert_eq!(config.busy_timeout_milliseconds(Some("bucket")), 250);
        assert_eq!(config.journal_size_limit(Some("bucket")), 4096);
    }

    #[test]
    fn sqlite_pragma_modes_parse_cli_and_yaml_names() -> TestResult {
        use clap::Parser as _;

        let lower: Config = yaml_serde::from_str(
            "
            journal_mode: wal
            synchronous: normal
            temp_store: memory
            busy_timeout_milliseconds: 1234
            journal_size_limit: 8192
            ",
        )?;
        assert_eq!(lower.sqlite.journal_mode, JournalMode::Wal);
        assert_eq!(lower.sqlite.synchronous, Synchronous::Normal);
        assert_eq!(lower.sqlite.temp_store, TempStore::Memory);
        assert_eq!(lower.sqlite.busy_timeout_milliseconds, 1234);
        assert_eq!(lower.sqlite.journal_size_limit, 8192);

        let upper: Config = yaml_serde::from_str(
            "
            journal_mode: DELETE
            synchronous: FULL
            temp_store: FILE
            ",
        )?;
        assert_eq!(upper.sqlite.journal_mode, JournalMode::Delete);
        assert_eq!(upper.sqlite.synchronous, Synchronous::Full);
        assert_eq!(upper.sqlite.temp_store, TempStore::File);

        let args = ConfigArgs::try_parse_from([
            "s3ite",
            "--journal-mode",
            "truncate",
            "--synchronous",
            "extra",
            "--temp-store",
            "default",
            "--busy-timeout-milliseconds",
            "5678",
            "--journal-size-limit",
            "16384",
        ])?;
        let cli = Config::from_config_args(args)?;
        assert_eq!(cli.sqlite.journal_mode, JournalMode::Truncate);
        assert_eq!(cli.sqlite.synchronous, Synchronous::Extra);
        assert_eq!(cli.sqlite.temp_store, TempStore::Default);
        assert_eq!(cli.sqlite.busy_timeout_milliseconds, 5678);
        assert_eq!(cli.sqlite.journal_size_limit, 16384);

        Ok(())
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

    #[test]
    fn cors_defaults_and_yaml_modes_are_parsed() -> TestResult {
        assert_eq!(Config::default().cors_mode(), CorsMode::Permissive);

        let config: Config = yaml_serde::from_str("permissive_cors: false\n")?;
        assert_eq!(config.cors_mode(), CorsMode::Disabled);

        let config: Config = yaml_serde::from_str(
            "
            cors:
              mode: disabled
            ",
        )?;
        assert_eq!(config.cors_mode(), CorsMode::Disabled);

        let config: Config = yaml_serde::from_str(
            "
            cors:
              mode: custom
              allow_origins:
                - https://app.example.test
              allow_credentials: false
              allow_private_network: true
              max_age_seconds: 600
            ",
        )?;
        let CorsMode::Custom(cors) = config.cors_mode() else {
            panic!("expected custom CORS mode");
        };
        assert_eq!(cors.allow_origins, ["https://app.example.test"]);
        assert_eq!(cors.allow_methods, default_cors_allow_methods());
        assert_eq!(cors.allow_headers, ["*"]);
        assert_eq!(cors.expose_headers, default_cors_expose_headers());
        assert!(!cors.allow_credentials);
        assert!(cors.allow_private_network);
        assert_eq!(cors.max_age_seconds, Some(600));
        assert!(config.cors_layer()?.is_some());

        Ok(())
    }

    #[test]
    fn custom_cors_rejects_invalid_combinations() -> TestResult {
        let config: Config = yaml_serde::from_str(
            "
            cors:
              mode: custom
              allow_origins:
                - '*'
                - https://app.example.test
            ",
        )?;
        assert!(config.validate().is_err());

        let config: Config = yaml_serde::from_str(
            "
            cors:
              mode: custom
              allow_origins:
                - '*'
              allow_credentials: true
            ",
        )?;
        assert!(config.validate().is_err());

        let custom_field_on_disabled = yaml_serde::from_str::<Config>(
            "
            cors:
              mode: disabled
              allow_origins:
                - https://app.example.test
            ",
        );
        assert!(custom_field_on_disabled.is_err());

        let config: Config = yaml_serde::from_str(
            "
            cors:
              mode: custom
              allow_origins:
                - https://app.example.test
              allow_methods:
                - 'BAD METHOD'
            ",
        )?;
        assert!(config.cors_layer().is_err());

        Ok(())
    }

    #[test]
    fn legacy_permissive_cors_aliases_override_explicit_mode() -> TestResult {
        use clap::Parser as _;

        let args = ConfigArgs::try_parse_from(["s3ite", "--permissive-cors", "true"])?;
        let config = Config::from_config_args(args)?;
        assert_eq!(config.cors_mode(), CorsMode::Permissive);

        let args = ConfigArgs::try_parse_from(["s3ite", "--permissive-cors", "false"])?;
        let config = Config::from_config_args(args)?;
        assert_eq!(config.cors_mode(), CorsMode::Disabled);

        let config = Config {
            cors: CorsMode::Disabled,
            permissive_cors: Some(true),
            ..Config::default()
        };
        assert_eq!(config.cors_mode(), CorsMode::Permissive);

        Ok(())
    }

    #[test]
    fn custom_cors_layers_cover_wildcard_and_explicit_rules() -> TestResult {
        let wildcard = Config {
            cors: CorsMode::Custom(CorsConfig {
                allow_origins: vec!["*".to_string()],
                allow_methods: vec!["*".to_string()],
                allow_headers: Vec::new(),
                expose_headers: vec!["*".to_string()],
                allow_credentials: false,
                allow_private_network: true,
                max_age_seconds: Some(30),
            }),
            ..Config::default()
        };
        assert!(wildcard.cors_layer()?.is_some());

        let explicit = Config {
            cors: CorsMode::Custom(CorsConfig {
                allow_origins: vec!["https://app.example.test".to_string()],
                allow_methods: vec!["GET".to_string(), "PUT".to_string()],
                allow_headers: vec!["x-amz-meta-test".to_string()],
                expose_headers: vec!["ETag".to_string()],
                allow_credentials: true,
                allow_private_network: false,
                max_age_seconds: None,
            }),
            ..Config::default()
        };
        assert!(explicit.cors_layer()?.is_some());

        let disabled = Config {
            cors: CorsMode::Disabled,
            ..Config::default()
        };
        assert!(disabled.cors_layer()?.is_none());

        let permissive = Config {
            cors: CorsMode::Permissive,
            ..Config::default()
        };
        assert!(permissive.cors_layer()?.is_some());

        Ok(())
    }

    #[test]
    fn custom_cors_validation_covers_required_fields_and_header_parsing() {
        let missing_origins = CorsConfig {
            allow_origins: Vec::new(),
            allow_methods: vec!["GET".to_string()],
            allow_headers: Vec::new(),
            expose_headers: Vec::new(),
            allow_credentials: false,
            allow_private_network: false,
            max_age_seconds: None,
        };
        assert!(missing_origins.validate().is_err());

        let missing_methods = CorsConfig {
            allow_origins: vec!["https://app.example.test".to_string()],
            allow_methods: Vec::new(),
            allow_headers: Vec::new(),
            expose_headers: Vec::new(),
            allow_credentials: false,
            allow_private_network: false,
            max_age_seconds: None,
        };
        assert!(missing_methods.validate().is_err());

        for config in [
            CorsConfig {
                allow_origins: vec!["https://app.example.test".to_string()],
                allow_methods: vec!["*".to_string()],
                allow_headers: vec!["x-amz-meta-test".to_string()],
                expose_headers: vec!["ETag".to_string()],
                allow_credentials: true,
                allow_private_network: false,
                max_age_seconds: None,
            },
            CorsConfig {
                allow_origins: vec!["https://app.example.test".to_string()],
                allow_methods: vec!["GET".to_string()],
                allow_headers: vec!["*".to_string()],
                expose_headers: vec!["ETag".to_string()],
                allow_credentials: true,
                allow_private_network: false,
                max_age_seconds: None,
            },
            CorsConfig {
                allow_origins: vec!["https://app.example.test".to_string()],
                allow_methods: vec!["GET".to_string()],
                allow_headers: vec!["x-amz-meta-test".to_string()],
                expose_headers: vec!["*".to_string()],
                allow_credentials: true,
                allow_private_network: false,
                max_age_seconds: None,
            },
        ] {
            assert!(config.validate().is_err());
        }

        for config in [
            CorsConfig {
                allow_origins: vec!["https://bad\n.example".to_string()],
                allow_methods: vec!["GET".to_string()],
                allow_headers: Vec::new(),
                expose_headers: Vec::new(),
                allow_credentials: false,
                allow_private_network: false,
                max_age_seconds: None,
            },
            CorsConfig {
                allow_origins: vec!["https://app.example.test".to_string()],
                allow_methods: vec!["GET".to_string()],
                allow_headers: vec!["bad header".to_string()],
                expose_headers: Vec::new(),
                allow_credentials: false,
                allow_private_network: false,
                max_age_seconds: None,
            },
            CorsConfig {
                allow_origins: vec!["https://app.example.test".to_string()],
                allow_methods: vec!["GET".to_string()],
                allow_headers: Vec::new(),
                expose_headers: vec!["bad header".to_string()],
                allow_credentials: false,
                allow_private_network: false,
                max_age_seconds: None,
            },
        ] {
            let config = Config {
                cors: CorsMode::Custom(config),
                ..Config::default()
            };
            assert!(matches!(
                config.cors_layer(),
                Err(ConfigError::Configuration(_))
            ));
        }
    }

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
            cors:
              mode: permissive
            ",
        )?;

        let path_arg = path.to_string_lossy().into_owned();
        let root = std::env::temp_dir().join(format!("s3ite-root-{}", uuid::Uuid::new_v4()));
        let root_arg = root.to_string_lossy().into_owned();

        let args = ConfigArgs::try_parse_from([
            "s3ite",
            "--config",
            &path_arg,
            "--root",
            &root_arg,
            "--host",
            "127.0.0.1",
            "--port",
            "9001",
            "--access-key",
            "access",
            "--secret-key",
            "secret",
            "--cors",
            "disabled",
            "--domain-name",
            "s3.example.test",
            "--concurrency-limit",
            "32",
            "--cache-size",
            "1024",
            "--readers",
            "3",
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
            "--read-only",
            "true",
            "--journal-mode",
            "delete",
            "--synchronous",
            "full",
            "--temp-store",
            "file",
        ])?;
        let config = Config::from_config_args(args)?;

        assert_eq!(config.root, root);
        assert_eq!(config.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(config.port, 9001);
        assert_eq!(config.access_key.as_deref(), Some("access"));
        assert_eq!(config.secret_key.as_deref(), Some("secret"));
        assert_eq!(config.cors_mode(), CorsMode::Disabled);
        assert_eq!(config.domain_name.as_deref(), Some("s3.example.test"));
        assert_eq!(config.concurrency_limit, 32);
        assert_eq!(config.readers, 3);
        assert_eq!(config.blob_readers, 8);
        assert_eq!(config.blob_read_timeout_seconds, 180);
        assert_eq!(config.maintenance_interval_seconds, 120);
        assert_eq!(config.maintenance_vacuum_pages, 75);
        assert_eq!(config.sqlite.cache_size, 1024);
        assert_eq!(config.multipart_upload_ttl_seconds, 300);
        assert_eq!(config.completed_multipart_upload_ttl_seconds, 400);
        assert!(config.read_only);
        assert_eq!(config.sqlite.journal_mode, JournalMode::Delete);
        assert_eq!(config.sqlite.synchronous, Synchronous::Full);
        assert_eq!(config.sqlite.temp_store, TempStore::File);

        std::fs::remove_file(path).ok();
        Ok(())
    }
}
