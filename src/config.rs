use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use clap::{
    ArgAction, ArgMatches, CommandFactory, FromArgMatches, Parser, ValueEnum, parser::ValueSource,
};
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
#[command(
    author,
    version,
    about,
    long_about = None,
    after_long_help = "Configuration precedence: built-in defaults, then optional YAML from `--config`, then explicit CLI flags. Values shown as defaults are built-in defaults and can be overridden by YAML when the corresponding CLI flag is omitted."
)]
struct ConfigArgs {
    /// Print an example YAML configuration to stdout and exit.
    #[arg(long, exclusive = true)]
    print_config_template: bool,

    /// The base path where the `.sqlite3` files will be created.
    /// All `.sqlite3` files at this path will be loaded at startup and exposed via this service.
    #[arg(long, default_value_os_t = default_root())]
    root: PathBuf,

    /// Path to a YAML configuration file.
    #[arg(long)]
    config: Option<PathBuf>,

    /// The IP address to listen on for this service. Use `0.0.0.0` to listen on all interfaces.
    #[arg(long, default_value_t = default_host())]
    host: IpAddr,

    /// The port to listen on for this service.
    #[arg(long, default_value_t = default_port())]
    port: u16,

    /// Address for the optional HTTP liveness and readiness listener.
    #[arg(long)]
    health_bind: Option<SocketAddr>,

    /// Configures Cross-Origin Resource Sharing (`CORS`) handling.
    #[arg(
        long,
        value_enum,
        default_value_t = default_cli_cors_mode(),
        conflicts_with = "permissive_cors"
    )]
    cors: CliCorsMode,

    /// Allow permissive Cross-Origin Resource Sharing (`CORS`) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    #[arg(long, conflicts_with = "cors", hide = true)]
    permissive_cors: Option<bool>,

    /// The access key ID that is used to authenticate for this service.
    #[arg(long, requires = "secret_key")]
    access_key: Option<String>,

    /// The secret access key that is used to authenticate for this service.
    #[arg(long, requires = "access_key")]
    secret_key: Option<String>,

    /// Base64-encoded 32-byte master key used for SSE-S3 object encryption.
    #[arg(long)]
    sse_s3_key: Option<String>,

    /// Enable SSE-KMS in this AWS region using the AWS SDK credential chain.
    #[arg(long)]
    kms_region: Option<String>,

    /// The domain to use to allow parsing virtual-hosted-style requests.
    #[arg(long)]
    domain_name: Option<String>,

    /// Full OTLP/HTTP traces endpoint, including its path.
    #[arg(long)]
    otlp_traces_endpoint: Option<String>,

    /// Full OTLP/HTTP audit logs endpoint.
    #[arg(long)]
    otlp_logs_endpoint: Option<String>,

    /// Full OTLP/HTTP operational metrics endpoint.
    #[arg(long)]
    otlp_metrics_endpoint: Option<String>,

    /// Include successful reads in the audit log.
    #[arg(long, action = ArgAction::Set)]
    audit_reads: Option<bool>,

    /// Enforces a limit on the concurrent number of requests the underlying service can handle.
    /// This can be tuned depending on infrastructure as `SSD`/`HDD` will deal with resource contention very differently.
    #[arg(long, default_value_t = default_concurrency_limit())]
    concurrency_limit: u16,

    /// Whether this service should be read-only.
    #[arg(long, action = ArgAction::Set, default_value_t = default_read_only())]
    read_only: bool,

    /// Controls the `SQLite` `journal_mode` flag pragma.
    #[arg(long, value_enum, default_value_t = default_journal_mode())]
    journal_mode: JournalMode,

    /// Controls the `SQLite` `synchronous` pragma.
    #[arg(long, value_enum, default_value_t = default_synchronous())]
    synchronous: Synchronous,

    /// Controls the `SQLite` `temp_store` pragma.
    #[arg(long, value_enum, default_value_t = default_temp_store())]
    temp_store: TempStore,

    /// Controls the `SQLite` `cache_size` pragma in kilobytes.
    #[arg(long, default_value_t = default_cache_size())]
    cache_size: u32,

    /// Controls the `SQLite` `busy_timeout` pragma in milliseconds.
    #[arg(long, default_value_t = default_busy_timeout_milliseconds())]
    busy_timeout_milliseconds: u64,

    /// Controls the `SQLite` `journal_size_limit` pragma in bytes.
    #[arg(long, default_value_t = default_journal_size_limit())]
    journal_size_limit: i64,

    /// Controls the number of reader connections to `SQLite`.
    #[arg(long, default_value_t = default_readers())]
    readers: usize,

    /// Controls the number of long-lived BLOB reader connections to `SQLite`.
    #[arg(long, default_value_t = default_blob_readers())]
    blob_readers: usize,

    /// Seconds a BLOB reader may stay blocked by an unconsumed response. Set to `0` to disable.
    #[arg(long, default_value_t = default_blob_read_timeout_seconds())]
    blob_read_timeout_seconds: u64,

    /// Size, in MiB, of each stored object chunk. Each upload buffers at most one chunk in memory.
    #[arg(long, default_value_t = default_blob_chunk_size_mib())]
    blob_chunk_size_mib: usize,

    /// Interval, in seconds, for background database maintenance. Set to `0` to disable.
    #[arg(long, default_value_t = default_maintenance_interval_seconds())]
    maintenance_interval_seconds: u64,

    /// Number of free pages to reclaim during each maintenance pass. Set to `0` to disable.
    #[arg(long, default_value_t = default_maintenance_vacuum_pages())]
    maintenance_vacuum_pages: u32,

    /// TTL, in seconds, for inactive incomplete multipart uploads. Set to `0` to disable.
    #[arg(long, default_value_t = default_multipart_upload_ttl_seconds())]
    multipart_upload_ttl_seconds: u64,

    /// TTL, in seconds, for completed multipart idempotency records. Set to `0` to disable.
    #[arg(long, default_value_t = default_completed_upload_ttl_seconds())]
    completed_multipart_upload_ttl_seconds: u64,
}

/// CLI-accessible `CORS` modes.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
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

/// Top-level action requested by command-line parsing.
#[derive(Debug)]
pub(crate) enum ConfigAction {
    /// Run the service with the resolved configuration.
    Run(Box<Config>),
    /// Print a starter YAML configuration and exit.
    PrintConfigTemplate,
}

impl ConfigAction {
    /// Loads the requested startup action from command-line arguments.
    ///
    /// # Errors
    ///
    /// Returns an error if a YAML config file requested by runtime startup
    /// cannot be read or parsed.
    pub(crate) fn from_args() -> ConfigResult<Self> {
        let matches = ConfigArgs::command().get_matches();
        Self::from_arg_matches(&matches)
    }

    /// Loads the requested startup action from parsed command-line matches.
    ///
    /// `--print-config-template` is handled before YAML loading so the template
    /// path is independent of user configuration and service validation.
    ///
    /// # Errors
    ///
    /// Returns an error if runtime startup requests a YAML file that cannot be
    /// read or parsed.
    fn from_arg_matches(matches: &ArgMatches) -> ConfigResult<Self> {
        let args = ConfigArgs::from_arg_matches(matches)
            .map_err(|err| ConfigError::Configuration(err.to_string()))?;
        if args.print_config_template {
            return Ok(Self::PrintConfigTemplate);
        }

        Ok(Self::Run(Box::new(Config::from_config_args(
            &args, matches,
        )?)))
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

    /// Address for the dedicated process health listener.
    pub health_bind: Option<SocketAddr>,

    /// The access key ID that is used to authenticate for this service.
    pub access_key: Option<String>,

    /// The secret access key that is used to authenticate for this service.
    pub secret_key: Option<String>,

    /// Base64-encoded 32-byte master key used for SSE-S3 object encryption.
    pub sse_s3_key: Option<String>,

    /// AWS region for the optional KMS client.
    pub kms_region: Option<String>,

    /// Shared client initialized at startup when SSE-KMS is configured.
    #[serde(skip)]
    pub(crate) kms_client: Option<aws_sdk_kms::Client>,

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

    /// Size, in MiB, of each stored object chunk.
    ///
    /// Uploads are staged one chunk at a time, so this caps the memory each
    /// in-flight upload holds: at most `concurrency_limit` times this size
    /// across the server (twice that while encrypting). A copy also holds one
    /// chunk of its source. Existing objects keep their stored chunk sizes when
    /// it changes, so that chunk can be larger than this setting.
    #[serde(default = "default_blob_chunk_size_mib")]
    pub blob_chunk_size_mib: usize,

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

    /// Full OTLP/HTTP traces endpoint, including its path.
    pub otlp_traces_endpoint: Option<String>,
    /// Full OTLP/HTTP audit logs endpoint.
    pub otlp_logs_endpoint: Option<String>,
    /// Full OTLP/HTTP operational metrics endpoint.
    pub otlp_metrics_endpoint: Option<String>,
    /// Includes successful read-only requests when audit export is enabled.
    #[serde(default)]
    pub audit_reads: bool,

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
    /// Loads service defaults with encryption providers disabled.
    fn default() -> Self {
        Self {
            root: default_root(),
            host: default_host(),
            port: default_port(),
            health_bind: None,
            access_key: None,
            secret_key: None,
            sse_s3_key: None,
            kms_region: None,
            kms_client: None,
            concurrency_limit: default_concurrency_limit(),
            readers: default_readers(),
            blob_readers: default_blob_readers(),
            blob_read_timeout_seconds: default_blob_read_timeout_seconds(),
            blob_chunk_size_mib: default_blob_chunk_size_mib(),
            maintenance_interval_seconds: default_maintenance_interval_seconds(),
            maintenance_vacuum_pages: default_maintenance_vacuum_pages(),
            multipart_upload_ttl_seconds: default_multipart_upload_ttl_seconds(),
            completed_multipart_upload_ttl_seconds: default_completed_upload_ttl_seconds(),
            cors: default_cors(),
            permissive_cors: None,
            read_only: default_read_only(),
            domain_name: None,
            otlp_traces_endpoint: None,
            otlp_logs_endpoint: None,
            otlp_metrics_endpoint: None,
            audit_reads: false,
            sqlite: default_pragmas(),
            buckets: HashMap::default(),
        }
    }
}

impl Config {
    /// Loads configuration from parsed command-line matches.
    ///
    /// If a YAML config path is provided, values from the file are loaded first
    /// and then overridden by any values explicitly supplied on the command
    /// line. Clap defaults are used for help text and bare runs, but they do
    /// not override YAML values.
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML file cannot be read or parsed.
    #[cfg(test)]
    fn from_arg_matches(matches: &ArgMatches) -> ConfigResult<Self> {
        let args = ConfigArgs::from_arg_matches(matches)
            .map_err(|err| ConfigError::Configuration(err.to_string()))?;
        Self::from_config_args(&args, matches)
    }

    /// Loads runtime configuration from parsed arguments and matches.
    ///
    /// Clap defaults are present in `args`, but only command-line sourced
    /// values from `matches` override YAML.
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML file cannot be read or parsed.
    fn from_config_args(args: &ConfigArgs, matches: &ArgMatches) -> ConfigResult<Self> {
        let mut config = args
            .config
            .as_ref()
            .map(|path| {
                let config = std::fs::read_to_string(path)?;
                Ok::<_, ConfigError>(yaml_serde::from_str::<Self>(&config)?)
            })
            .transpose()?
            .unwrap_or_default();

        config.apply_args(args, matches);
        config.validate()?;

        Ok(config)
    }

    /// Renders a starter YAML configuration using built-in defaults.
    #[must_use]
    pub(crate) fn config_template() -> String {
        render_config_template(&Self::default())
    }

    /// Applies explicit command-line values over the current configuration.
    fn apply_args(&mut self, args: &ConfigArgs, matches: &ArgMatches) {
        if cli_supplied(matches, "root") {
            self.root.clone_from(&args.root);
        }
        if cli_supplied(matches, "host") {
            self.host = args.host;
        }
        self.health_bind = args.health_bind.or(self.health_bind);
        if cli_supplied(matches, "port") {
            self.port = args.port;
        }
        if cli_supplied(matches, "access_key") {
            self.access_key.clone_from(&args.access_key);
        }
        if cli_supplied(matches, "secret_key") {
            self.secret_key.clone_from(&args.secret_key);
        }
        if cli_supplied(matches, "kms_region") {
            self.kms_region.clone_from(&args.kms_region);
        }
        if cli_supplied(matches, "sse_s3_key") {
            self.sse_s3_key.clone_from(&args.sse_s3_key);
        }
        if cli_supplied(matches, "cors") {
            self.cors = args.cors.into();
            self.permissive_cors = None;
        }
        if let Some(permissive_cors) = args
            .permissive_cors
            .filter(|_| cli_supplied(matches, "permissive_cors"))
        {
            self.cors = if permissive_cors {
                CorsMode::Permissive
            } else {
                CorsMode::Disabled
            };
            self.permissive_cors = None;
        }
        if let Some(endpoint) = &args.otlp_logs_endpoint {
            self.otlp_logs_endpoint = Some(endpoint.clone());
        }
        if let Some(endpoint) = &args.otlp_metrics_endpoint {
            self.otlp_metrics_endpoint = Some(endpoint.clone());
        }
        if let Some(enabled) = args.audit_reads {
            self.audit_reads = enabled;
        }
        if cli_supplied(matches, "otlp_traces_endpoint") {
            self.otlp_traces_endpoint
                .clone_from(&args.otlp_traces_endpoint);
        }
        if cli_supplied(matches, "domain_name") {
            self.domain_name.clone_from(&args.domain_name);
        }
        if cli_supplied(matches, "concurrency_limit") {
            self.concurrency_limit = args.concurrency_limit;
        }
        if cli_supplied(matches, "readers") {
            self.readers = args.readers;
        }
        if cli_supplied(matches, "blob_readers") {
            self.blob_readers = args.blob_readers;
        }
        if cli_supplied(matches, "blob_read_timeout_seconds") {
            self.blob_read_timeout_seconds = args.blob_read_timeout_seconds;
        }
        if cli_supplied(matches, "blob_chunk_size_mib") {
            self.blob_chunk_size_mib = args.blob_chunk_size_mib;
        }
        if cli_supplied(matches, "maintenance_interval_seconds") {
            self.maintenance_interval_seconds = args.maintenance_interval_seconds;
        }
        if cli_supplied(matches, "maintenance_vacuum_pages") {
            self.maintenance_vacuum_pages = args.maintenance_vacuum_pages;
        }
        if cli_supplied(matches, "multipart_upload_ttl_seconds") {
            self.multipart_upload_ttl_seconds = args.multipart_upload_ttl_seconds;
        }
        if cli_supplied(matches, "completed_multipart_upload_ttl_seconds") {
            self.completed_multipart_upload_ttl_seconds =
                args.completed_multipart_upload_ttl_seconds;
        }
        if cli_supplied(matches, "read_only") {
            self.read_only = args.read_only;
        }
        self.sqlite.apply_args(args, matches);
    }
}

impl Pragmas {
    /// Applies explicit command-line `SQLite` pragma values over the configured ones.
    fn apply_args(&mut self, args: &ConfigArgs, matches: &ArgMatches) {
        if cli_supplied(matches, "journal_mode") {
            self.journal_mode = args.journal_mode;
        }
        if cli_supplied(matches, "synchronous") {
            self.synchronous = args.synchronous;
        }
        if cli_supplied(matches, "temp_store") {
            self.temp_store = args.temp_store;
        }
        if cli_supplied(matches, "cache_size") {
            self.cache_size = args.cache_size;
        }
        if cli_supplied(matches, "busy_timeout_milliseconds") {
            self.busy_timeout_milliseconds = args.busy_timeout_milliseconds;
        }
        if cli_supplied(matches, "journal_size_limit") {
            self.journal_size_limit = args.journal_size_limit;
        }
    }
}

impl Config {
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

    /// Validates the merged configuration before the server opens any listeners.
    fn validate(&self) -> ConfigResult<()> {
        if self.access_key.is_some() != self.secret_key.is_some() {
            return Err(ConfigError::Configuration(
                "access_key and secret_key must be set together or both omitted".into(),
            ));
        }
        if self.concurrency_limit == 0 {
            return Err(ConfigError::Configuration(
                "concurrency_limit must be greater than zero".into(),
            ));
        }
        if self.blob_chunk_size_mib == 0 {
            return Err(ConfigError::Configuration(
                "blob_chunk_size_mib must be greater than zero".into(),
            ));
        }
        self.cors_mode()
            .validate()
            .map_err(ConfigError::Configuration)?;
        if let Some(key) = self.sse_s3_key.as_deref() {
            crate::s3::parse_master_key(key).map_err(ConfigError::Configuration)?;
        }
        self.cors_layer().map(|_| ())
    }

    /// Returns the decoded SSE-S3 master key when configured.
    pub(crate) fn sse_s3_master_key(&self) -> Option<[u8; 32]> {
        self.sse_s3_key
            .as_deref()
            .and_then(|key| crate::s3::parse_master_key(key).ok())
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

/// Returns whether an argument value came from the command line.
fn cli_supplied(matches: &ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

/// Renders a starter YAML configuration from a default config value.
fn render_config_template(config: &Config) -> String {
    format!(
        "\
root: {root}
host: {host}
port: {port}
# health_bind: 127.0.0.1:8015

# Set both credentials to enable authentication, or omit both to disable it.
# access_key: AKIAIOSFODNN7EXAMPLE
# secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# sse_s3_key: base64-encoded-32-byte-key
# kms_region: us-east-1
# domain_name: s3ite.local
# otlp_traces_endpoint: http://localhost:4318/v1/traces
# otlp_logs_endpoint: http://localhost:4318/v1/logs
# otlp_metrics_endpoint: http://localhost:4318/v1/metrics
audit_reads: false

concurrency_limit: {concurrency_limit}
read_only: {read_only}

readers: {readers}
blob_readers: {blob_readers}
blob_read_timeout_seconds: {blob_read_timeout_seconds}
# Caps the memory each in-flight upload buffers.
blob_chunk_size_mib: {blob_chunk_size_mib}

maintenance_interval_seconds: {maintenance_interval_seconds}
maintenance_vacuum_pages: {maintenance_vacuum_pages}
multipart_upload_ttl_seconds: {multipart_upload_ttl_seconds}
completed_multipart_upload_ttl_seconds: {completed_multipart_upload_ttl_seconds}

journal_mode: {journal_mode}
synchronous: {synchronous}
temp_store: {temp_store}
cache_size: {cache_size}
busy_timeout_milliseconds: {busy_timeout_milliseconds}
journal_size_limit: {journal_size_limit}

cors:
{cors}

# Bucket-specific overrides are optional.
# buckets:
#   example-bucket:
#     read_only: false
#     sqlite:
#       cache_size: 131072
#       synchronous: full
",
        root = config.root.display(),
        host = config.host,
        port = config.port,
        concurrency_limit = config.concurrency_limit,
        read_only = config.read_only,
        readers = config.readers,
        blob_readers = config.blob_readers,
        blob_read_timeout_seconds = config.blob_read_timeout_seconds,
        blob_chunk_size_mib = config.blob_chunk_size_mib,
        maintenance_interval_seconds = config.maintenance_interval_seconds,
        maintenance_vacuum_pages = config.maintenance_vacuum_pages,
        multipart_upload_ttl_seconds = config.multipart_upload_ttl_seconds,
        completed_multipart_upload_ttl_seconds = config.completed_multipart_upload_ttl_seconds,
        journal_mode = value_enum_name(&config.sqlite.journal_mode),
        synchronous = value_enum_name(&config.sqlite.synchronous),
        temp_store = value_enum_name(&config.sqlite.temp_store),
        cache_size = config.sqlite.cache_size,
        busy_timeout_milliseconds = config.sqlite.busy_timeout_milliseconds,
        journal_size_limit = config.sqlite.journal_size_limit,
        cors = render_cors_template(&config.cors_mode()),
    )
}

/// Renders the YAML body for the server-level CORS section.
fn render_cors_template(cors: &CorsMode) -> String {
    match cors {
        CorsMode::Disabled => "  mode: disabled".to_string(),
        CorsMode::Permissive => "  mode: permissive".to_string(),
        CorsMode::Custom(config) => {
            let mut output = "  mode: custom\n".to_string();
            render_yaml_list(&mut output, "  allow_origins", &config.allow_origins);
            render_yaml_list(&mut output, "  allow_methods", &config.allow_methods);
            render_yaml_list(&mut output, "  allow_headers", &config.allow_headers);
            render_yaml_list(&mut output, "  expose_headers", &config.expose_headers);
            output.push_str("  allow_credentials: ");
            output.push_str(&config.allow_credentials.to_string());
            output.push_str("\n  allow_private_network: ");
            output.push_str(&config.allow_private_network.to_string());
            output.push('\n');
            if let Some(max_age_seconds) = config.max_age_seconds {
                output.push_str("  max_age_seconds: ");
                output.push_str(&max_age_seconds.to_string());
            } else {
                output.push_str("  max_age_seconds: null");
            }
            output
        }
    }
}

/// Appends a YAML string list to an existing output buffer.
fn render_yaml_list(output: &mut String, key: &str, values: &[String]) {
    output.push_str(key);
    output.push_str(":\n");
    for value in values {
        output.push_str("    - ");
        output.push_str(&yaml_single_quoted(value));
        output.push('\n');
    }
}

/// Returns a single-quoted YAML scalar for arbitrary text.
fn yaml_single_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Returns the configured command/config spelling for a value enum variant.
fn value_enum_name<T: ValueEnum>(value: &T) -> String {
    let Some(possible) = value.to_possible_value() else {
        unreachable!("configuration enum variants must have clap values");
    };
    possible.get_name().to_string()
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
#[serde(deny_unknown_fields)]
pub struct Bucket {
    /// Whether this bucket should reject write operations.
    pub read_only: Option<bool>,

    /// Bucket-level `SQLite` pragma overrides.
    pub sqlite: Option<BucketPragmas>,
}

/// Optional `SQLite` PRAGMA overrides for one bucket.
#[derive(Clone, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
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

/// Default stored chunk size, in MiB, which also caps each upload's buffered bytes.
fn default_blob_chunk_size_mib() -> usize {
    16
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

/// Returns the CLI spelling for the default server-level `CORS` mode.
fn default_cli_cors_mode() -> CliCorsMode {
    match default_cors() {
        CorsMode::Disabled => CliCorsMode::Disabled,
        CorsMode::Permissive => CliCorsMode::Permissive,
        CorsMode::Custom(_) => unreachable!("custom CORS has no CLI default spelling"),
    }
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
    use std::{
        env::temp_dir,
        fs::{remove_file, write},
    };
    use tempfile::tempdir;
    use uuid::Uuid;
    use yaml_serde::from_str;

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

    /// Reports an unknown setting while loading bucket overrides for startup.
    #[test]
    fn yaml_rejects_unknown_bucket_keys() -> TestResult {
        let directory = tempdir()?;
        let path = directory.path().join("config.yaml");
        write(&path, "buckets:\n  example:\n    read_onyl: true\n")?;
        let matches = ConfigArgs::command().try_get_matches_from([
            std::ffi::OsStr::new("s3ite"),
            std::ffi::OsStr::new("--config"),
            path.as_os_str(),
        ])?;

        let Err(error) = Config::from_arg_matches(&matches) else {
            panic!("startup accepted an unknown bucket setting");
        };
        assert!(error.to_string().contains("unknown field `read_onyl`"));
        Ok(())
    }

    /// Reports typos in bucket-level SQLite pragmas.
    #[test]
    fn yaml_rejects_unknown_bucket_sqlite_keys() {
        let Err(error) =
            from_str::<Config>("buckets:\n  example:\n    sqlite:\n      cache_szie: 512\n")
        else {
            panic!("startup accepted an unknown bucket pragma");
        };
        assert!(error.to_string().contains("unknown field `cache_szie`"));
    }

    /// Reports typos in service settings, flattened SQLite pragmas, and CORS.
    #[test]
    fn yaml_rejects_unknown_service_keys() {
        for (yaml, key) in [
            ("read_onyl: true\n", "read_onyl"),
            ("cache_szie: 512\n", "cache_szie"),
            (
                "cors:\n  mode: disabled\n  allow_orgins: []\n",
                "allow_orgins",
            ),
        ] {
            let Err(error) = from_str::<Config>(yaml) else {
                panic!("startup accepted an unknown service setting {key}");
            };
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}`"))
            );
        }
    }

    #[test]
    fn sqlite_pragma_modes_parse_cli_and_yaml_names() -> TestResult {
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

        let matches = ConfigArgs::command().try_get_matches_from([
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
        let cli = Config::from_arg_matches(&matches)?;
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
        assert_eq!(Config::default().blob_chunk_size_mib, 16);

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

    /// Verifies that public help lists the runtime defaults and configuration precedence.
    #[test]
    fn help_shows_runtime_defaults_and_hides_deprecated_alias() {
        macro_rules! value_name {
            ($value:expr) => {{
                let Some(possible) = $value.to_possible_value() else {
                    panic!("configuration enum variants must have clap values");
                };
                possible.get_name().to_string()
            }};
        }

        let help = ConfigArgs::command().render_long_help().to_string();
        let expected_defaults = [
            format!("[default: {}]", default_root().display()),
            format!("[default: {}]", default_host()),
            format!("[default: {}]", default_port()),
            format!("[default: {}]", value_name!(default_cli_cors_mode())),
            format!("[default: {}]", default_concurrency_limit()),
            format!("[default: {}]", default_read_only()),
            format!("[default: {}]", value_name!(default_journal_mode())),
            format!("[default: {}]", value_name!(default_synchronous())),
            format!("[default: {}]", value_name!(default_temp_store())),
            format!("[default: {}]", default_cache_size()),
            format!("[default: {}]", default_busy_timeout_milliseconds()),
            format!("[default: {}]", default_journal_size_limit()),
            format!("[default: {}]", default_readers()),
            format!("[default: {}]", default_blob_readers()),
            format!("[default: {}]", default_blob_read_timeout_seconds()),
            format!("[default: {}]", default_blob_chunk_size_mib()),
            format!("[default: {}]", default_maintenance_interval_seconds()),
            format!("[default: {}]", default_maintenance_vacuum_pages()),
            format!("[default: {}]", default_multipart_upload_ttl_seconds()),
            format!("[default: {}]", default_completed_upload_ttl_seconds()),
        ];

        assert!(help.contains("--print-config-template"));
        assert!(help.contains("--cors <CORS>"));
        assert!(!help.contains("--permissive-cors"));
        assert!(help.contains("Configuration precedence: built-in defaults"));
        assert!(help.contains("can be overridden by YAML"));
        for expected in expected_defaults {
            assert!(help.contains(&expected), "help is missing `{expected}`");
        }
    }

    /// Verifies that omitted CLI options retain their default values and sources.
    #[test]
    fn omitted_cli_options_use_runtime_defaults() -> TestResult {
        let matches = ConfigArgs::command().try_get_matches_from(["s3ite"])?;
        for id in [
            "root",
            "print_config_template",
            "host",
            "port",
            "cors",
            "concurrency_limit",
            "read_only",
            "journal_mode",
            "synchronous",
            "temp_store",
            "cache_size",
            "busy_timeout_milliseconds",
            "journal_size_limit",
            "readers",
            "blob_readers",
            "blob_read_timeout_seconds",
            "blob_chunk_size_mib",
            "maintenance_interval_seconds",
            "maintenance_vacuum_pages",
            "multipart_upload_ttl_seconds",
            "completed_multipart_upload_ttl_seconds",
        ] {
            assert_eq!(matches.value_source(id), Some(ValueSource::DefaultValue));
        }
        for id in [
            "config",
            "permissive_cors",
            "access_key",
            "secret_key",
            "sse_s3_key",
            "domain_name",
        ] {
            assert_eq!(matches.value_source(id), None);
        }

        let args = ConfigArgs::from_arg_matches(&matches)?;
        assert!(!args.print_config_template);
        assert_eq!(args.root, default_root());
        assert!(args.config.is_none());
        assert_eq!(args.host, default_host());
        assert_eq!(args.port, default_port());
        assert_eq!(args.cors, default_cli_cors_mode());
        assert!(args.permissive_cors.is_none());
        assert!(args.access_key.is_none());
        assert!(args.secret_key.is_none());
        assert!(args.sse_s3_key.is_none());
        assert!(args.domain_name.is_none());
        assert_eq!(args.concurrency_limit, default_concurrency_limit());
        assert_eq!(args.read_only, default_read_only());
        assert_eq!(args.journal_mode, default_journal_mode());
        assert_eq!(args.synchronous, default_synchronous());
        assert_eq!(args.temp_store, default_temp_store());
        assert_eq!(args.cache_size, default_cache_size());
        assert_eq!(
            args.busy_timeout_milliseconds,
            default_busy_timeout_milliseconds()
        );
        assert_eq!(args.journal_size_limit, default_journal_size_limit());
        assert_eq!(args.readers, default_readers());
        assert_eq!(args.blob_readers, default_blob_readers());
        assert_eq!(
            args.blob_read_timeout_seconds,
            default_blob_read_timeout_seconds()
        );
        assert_eq!(args.blob_chunk_size_mib, default_blob_chunk_size_mib());
        assert_eq!(
            args.maintenance_interval_seconds,
            default_maintenance_interval_seconds()
        );
        assert_eq!(
            args.maintenance_vacuum_pages,
            default_maintenance_vacuum_pages()
        );
        assert_eq!(
            args.multipart_upload_ttl_seconds,
            default_multipart_upload_ttl_seconds()
        );
        assert_eq!(
            args.completed_multipart_upload_ttl_seconds,
            default_completed_upload_ttl_seconds()
        );

        Ok(())
    }

    /// Verifies the template flag selects the early-exit startup action.
    #[test]
    fn print_config_template_action_is_selected() -> TestResult {
        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--print-config-template"])?;

        assert!(matches!(
            ConfigAction::from_arg_matches(&matches)?,
            ConfigAction::PrintConfigTemplate
        ));

        Ok(())
    }

    /// Verifies the template flag cannot be combined with runtime configuration.
    #[test]
    fn print_config_template_rejects_other_options() {
        let result = ConfigArgs::command().try_get_matches_from([
            "s3ite",
            "--print-config-template",
            "--port",
            "9000",
        ]);

        assert!(result.is_err());
    }

    /// The command line can select a trace collector without a YAML file.
    #[test]
    fn cli_sets_otlp_signal_endpoints() -> TestResult {
        let endpoint = "http://collector:4318/v1/traces";
        let matches = ConfigArgs::command().try_get_matches_from([
            "s3ite",
            "--otlp-traces-endpoint",
            endpoint,
            "--otlp-metrics-endpoint",
            "http://metrics/v1/metrics",
            "--otlp-logs-endpoint",
            "http://logs/v1/logs",
            "--audit-reads",
            "true",
        ])?;
        let config = Config::from_arg_matches(&matches)?;
        assert_eq!(config.otlp_traces_endpoint.as_deref(), Some(endpoint));
        assert_eq!(
            config.otlp_metrics_endpoint.as_deref(),
            Some("http://metrics/v1/metrics")
        );
        assert_eq!(
            config.otlp_logs_endpoint.as_deref(),
            Some("http://logs/v1/logs")
        );
        assert!(config.audit_reads);
        Ok(())
    }

    /// Verifies the generated template parses back to the built-in defaults.
    #[test]
    fn config_template_parses_as_default_config() -> TestResult {
        let template = Config::config_template();
        let config: Config = from_str(&template)?;
        let default = Config::default();

        assert!(template.contains("# access_key:"));
        assert!(template.contains("# secret_key:"));
        assert!(template.contains("# sse_s3_key:"));
        assert!(template.contains("# domain_name:"));
        assert!(template.contains("# otlp_traces_endpoint:"));
        assert!(template.contains("# buckets:"));

        assert_eq!(config.root, default.root);
        assert_eq!(config.host, default.host);
        assert_eq!(config.port, default.port);
        assert_eq!(config.access_key, default.access_key);
        assert_eq!(config.secret_key, default.secret_key);
        assert_eq!(config.sse_s3_key, default.sse_s3_key);
        assert_eq!(config.domain_name, default.domain_name);
        assert!(config.otlp_traces_endpoint.is_none());
        assert_eq!(config.concurrency_limit, default.concurrency_limit);
        assert_eq!(config.read_only, default.read_only);
        assert_eq!(config.readers, default.readers);
        assert_eq!(config.blob_readers, default.blob_readers);
        assert_eq!(
            config.blob_read_timeout_seconds,
            default.blob_read_timeout_seconds
        );
        assert_eq!(config.blob_chunk_size_mib, default.blob_chunk_size_mib);
        assert_eq!(
            config.maintenance_interval_seconds,
            default.maintenance_interval_seconds
        );
        assert_eq!(
            config.maintenance_vacuum_pages,
            default.maintenance_vacuum_pages
        );
        assert_eq!(
            config.multipart_upload_ttl_seconds,
            default.multipart_upload_ttl_seconds
        );
        assert_eq!(
            config.completed_multipart_upload_ttl_seconds,
            default.completed_multipart_upload_ttl_seconds
        );
        assert_eq!(config.cors_mode(), default.cors_mode());
        assert_eq!(config.sqlite.journal_mode, default.sqlite.journal_mode);
        assert_eq!(config.sqlite.synchronous, default.sqlite.synchronous);
        assert_eq!(config.sqlite.temp_store, default.sqlite.temp_store);
        assert_eq!(config.sqlite.cache_size, default.sqlite.cache_size);
        assert_eq!(
            config.sqlite.busy_timeout_milliseconds,
            default.sqlite.busy_timeout_milliseconds
        );
        assert_eq!(
            config.sqlite.journal_size_limit,
            default.sqlite.journal_size_limit
        );
        assert!(config.buckets.is_empty());

        Ok(())
    }

    /// Startup accepts a complete or omitted YAML credential pair and rejects either missing half.
    #[test]
    fn yaml_requires_paired_owner_credentials() -> TestResult {
        let directory = tempdir()?;
        let path = directory.path().join("config.yaml");
        let matches = ConfigArgs::command().try_get_matches_from([
            "s3ite".as_ref(),
            "--config".as_ref(),
            path.as_os_str(),
        ])?;

        for yaml in ["access_key: example-id", "secret_key: example-secret"] {
            write(&path, yaml)?;
            assert!(matches!(
                ConfigAction::from_arg_matches(&matches),
                Err(ConfigError::Configuration(message))
                    if message == "access_key and secret_key must be set together or both omitted"
            ));
        }
        for yaml in ["{}", "access_key: example-id\nsecret_key: example-secret"] {
            write(&path, yaml)?;
            assert!(matches!(
                ConfigAction::from_arg_matches(&matches)?,
                ConfigAction::Run(_)
            ));
        }
        Ok(())
    }

    /// Startup rejects a zero request limit from the CLI or YAML, which would hang every request.
    #[test]
    fn rejects_zero_concurrency_limit() -> TestResult {
        let is_zero_limit_error = |result: ConfigResult<ConfigAction>| {
            matches!(
                result,
                Err(ConfigError::Configuration(message))
                    if message == "concurrency_limit must be greater than zero"
            )
        };
        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--concurrency-limit", "0"])?;
        assert!(is_zero_limit_error(ConfigAction::from_arg_matches(
            &matches
        )));

        let directory = tempdir()?;
        let path = directory.path().join("config.yaml");
        write(&path, "concurrency_limit: 0")?;
        let matches = ConfigArgs::command().try_get_matches_from([
            "s3ite".as_ref(),
            "--config".as_ref(),
            path.as_os_str(),
        ])?;
        assert!(is_zero_limit_error(ConfigAction::from_arg_matches(
            &matches
        )));
        Ok(())
    }

    /// Startup rejects a zero chunk size, which could never stage a byte.
    #[test]
    fn rejects_zero_blob_chunk_size() -> TestResult {
        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--blob-chunk-size-mib", "0"])?;
        assert!(matches!(
            ConfigAction::from_arg_matches(&matches),
            Err(ConfigError::Configuration(message))
                if message == "blob_chunk_size_mib must be greater than zero"
        ));
        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--blob-chunk-size-mib", "2"])?;
        let ConfigAction::Run(config) = ConfigAction::from_arg_matches(&matches)? else {
            panic!("a valid chunk size should run");
        };
        assert_eq!(config.blob_chunk_size_mib, 2);
        Ok(())
    }

    /// Verifies Clap defaults do not replace values loaded from a YAML file.
    #[test]
    fn clap_defaults_do_not_override_yaml_config() -> TestResult {
        let path = temp_dir().join(format!("s3ite-{}.yaml", Uuid::new_v4()));
        write(
            &path,
            "
            root: /tmp/s3ite-yaml-root
            host: 127.0.0.1
            port: 9000
            concurrency_limit: 7
            read_only: true
            readers: 2
            blob_readers: 6
            blob_read_timeout_seconds: 90
            blob_chunk_size_mib: 4
            maintenance_interval_seconds: 30
            maintenance_vacuum_pages: 5
            multipart_upload_ttl_seconds: 100
            completed_multipart_upload_ttl_seconds: 200
            cors:
              mode: disabled
            journal_mode: delete
            synchronous: full
            temp_store: file
            cache_size: 512
            busy_timeout_milliseconds: 250
            journal_size_limit: 4096
            ",
        )?;

        let path_arg = path.to_string_lossy().into_owned();
        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--config", &path_arg])?;
        let config = Config::from_arg_matches(&matches)?;

        assert_eq!(config.root, PathBuf::from("/tmp/s3ite-yaml-root"));
        assert_eq!(config.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(config.port, 9000);
        assert_eq!(config.concurrency_limit, 7);
        assert!(config.read_only);
        assert_eq!(config.readers, 2);
        assert_eq!(config.blob_readers, 6);
        assert_eq!(config.blob_read_timeout_seconds, 90);
        assert_eq!(config.blob_chunk_size_mib, 4);
        assert_eq!(config.maintenance_interval_seconds, 30);
        assert_eq!(config.maintenance_vacuum_pages, 5);
        assert_eq!(config.multipart_upload_ttl_seconds, 100);
        assert_eq!(config.completed_multipart_upload_ttl_seconds, 200);
        assert_eq!(config.cors_mode(), CorsMode::Disabled);
        assert_eq!(config.sqlite.journal_mode, JournalMode::Delete);
        assert_eq!(config.sqlite.synchronous, Synchronous::Full);
        assert_eq!(config.sqlite.temp_store, TempStore::File);
        assert_eq!(config.sqlite.cache_size, 512);
        assert_eq!(config.sqlite.busy_timeout_milliseconds, 250);
        assert_eq!(config.sqlite.journal_size_limit, 4096);

        remove_file(path).ok();
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
        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--permissive-cors", "true"])?;
        let config = Config::from_arg_matches(&matches)?;
        assert_eq!(config.cors_mode(), CorsMode::Permissive);

        let matches =
            ConfigArgs::command().try_get_matches_from(["s3ite", "--permissive-cors", "false"])?;
        let config = Config::from_arg_matches(&matches)?;
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

    /// Checks that explicitly supplied CLI settings override values loaded from YAML.
    #[test]
    fn cli_args_override_yaml_config() -> TestResult {
        let path = temp_dir().join(format!("s3ite-{}.yaml", Uuid::new_v4()));
        write(
            &path,
            "
            port: 9000
            health_bind: 127.0.0.1:9100
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
        let root = temp_dir().join(format!("s3ite-root-{}", Uuid::new_v4()));
        let root_arg = root.to_string_lossy().into_owned();

        let matches = ConfigArgs::command().try_get_matches_from([
            "s3ite",
            "--config",
            &path_arg,
            "--root",
            &root_arg,
            "--host",
            "127.0.0.1",
            "--port",
            "9001",
            "--health-bind",
            "127.0.0.1:9101",
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
        let config = Config::from_arg_matches(&matches)?;

        assert_eq!(config.root, root);
        assert_eq!(config.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(config.port, 9001);
        assert_eq!(config.health_bind, Some("127.0.0.1:9101".parse()?));
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

        remove_file(path).ok();
        Ok(())
    }
}
