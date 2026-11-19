#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::expect_used,
    clippy::pedantic,
    clippy::unwrap_used
)]

use std::{
    io::{Write, stdout},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_kms::Client as KmsClient;
use hyper::service::service_fn;
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto,
};
use s3s::{
    auth::SimpleAuth,
    config::{S3Config, StaticConfigProvider},
    host::MultiDomain,
    service::{S3Service, S3ServiceBuilder},
};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal as unix_signal};
use tokio::{
    net::{TcpListener, TcpStream},
    signal,
    time::sleep,
};
use tower::{ServiceBuilder, limit::GlobalConcurrencyLimitLayer};
use tracing::{info, warn};

mod compat;
mod config;
mod database;
mod health;
mod s3;
mod sqlite;
mod telemetry;

/// Routes Linux builds through jemalloc for steadier allocation behavior in the
/// static container binary.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use self::compat::S3CompatLayer;
use self::health::Readiness;
use self::telemetry::Observer;
pub(crate) use self::{
    config::{Config, ConfigAction, ConfigError},
    sqlite::{Sqlite, SqliteError},
};

#[derive(Clone, Copy, Debug)]
struct ClientAddress(SocketAddr);

type MainResult<T> = std::result::Result<T, MainError>;

#[derive(Debug, thiserror::Error)]
enum MainError {
    #[error("Config {0}")]
    Config(#[from] ConfigError),
    #[error("SQLite {0}")]
    Sqlite(#[from] SqliteError),
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration error: {0}")]
    Configuration(String),
}

/// Parses the startup action and runs the configured S3 server.
#[tokio::main]
async fn main() -> MainResult<()> {
    let config = match ConfigAction::from_args()? {
        ConfigAction::Run(config) => *config,
        ConfigAction::PrintConfigTemplate => {
            stdout()
                .lock()
                .write_all(Config::config_template()?.as_bytes())?;
            return Ok(());
        }
    };
    let telemetry = telemetry::init(&config)?;
    let readiness = Readiness::default();
    if let Some(address) = config.health_bind {
        let listener = TcpListener::bind(address).await?;
        info!(
            "health probes listening on http://{}",
            listener.local_addr()?
        );
        let probes = readiness.clone();
        tokio::spawn(async move { probes.serve(listener).await });
    }
    run_s3(config, telemetry.observer(), readiness).await
}

/// Opens bucket databases and binds the S3 socket before advertising readiness.
async fn run_s3(mut config: Config, observer: Observer, readiness: Readiness) -> MainResult<()> {
    if let Some(region) = &config.kms_region {
        let settings = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .load()
            .await;
        config.kms_client = Some(KmsClient::new(&settings));
    }
    let sqlite = Sqlite::new(&config).await?;
    let requests = sqlite.requests.clone();
    let cors_layer = s3::BucketCorsLayer::new(sqlite.clone(), &config);
    let svc = build_s3_service(&config, sqlite)?;
    let listener = bind_listener(&config).await?;

    serve(listener, svc, cors_layer, requests, observer, readiness).await
}

/// Builds the S3 service with the configured owner and addressing mode.
fn build_s3_service(config: &Config, sqlite: Sqlite) -> MainResult<S3Service> {
    let mut s3 = S3ServiceBuilder::new(sqlite);
    let mut protocol = S3Config::default();
    protocol.enable_sig_v2 = true;
    s3.set_config(Arc::new(StaticConfigProvider::new(protocol.into())));

    // Validation requires both credentials, so every request is authenticated.
    let (Some(access_key), Some(secret_key)) =
        (config.access_key.clone(), config.secret_key.clone())
    else {
        return Err(MainError::Configuration(
            "access_key and secret_key are required".into(),
        ));
    };
    s3.set_auth(SimpleAuth::from_single(access_key, secret_key));

    if let Some(domain_name) = &config.domain_name {
        let domains = [domain_name.as_str()];
        let domains =
            MultiDomain::new(&domains).map_err(|err| MainError::Configuration(err.to_string()))?;
        s3.set_host(domains);
    }

    Ok(s3.build())
}

async fn bind_listener(config: &Config) -> MainResult<TcpListener> {
    let addr = SocketAddr::new(config.host, config.port);
    Ok(TcpListener::bind(addr).await?)
}

/// Accepts the next connection, backing off if the listener runs out of resources.
async fn accept_connection(listener: &TcpListener) -> (TcpStream, SocketAddr) {
    loop {
        match listener.accept().await {
            Ok(connection) => return connection,
            Err(error) => {
                warn!(%error, "failed to accept connection");
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Waits for a console interrupt or a Unix termination signal before draining connections.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = unix_signal(SignalKind::terminate())?;
        tokio::select! {
            result = signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    signal::ctrl_c().await
}

/// Time an HTTP/1 client has to send a complete request head.
///
/// Matches the idle timeout on upload bodies, so a stalled client is cut off
/// at the same point whether it stalls in the headers or the body.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(20);

/// Serves S3 connections until shutdown and gives active requests time to finish.
///
/// `requests` caps in-flight S3 requests across every connection: one
/// semaphore, sized by `concurrency_limit`, is shared by all accepted
/// connections, so HTTP/1 keep-alive clients cannot bypass the limit by opening
/// more connections. Storage maintenance reads the same semaphore to detect an
/// idle server.
async fn serve(
    listener: TcpListener,
    svc: S3Service,
    cors_layer: s3::BucketCorsLayer,
    requests: Arc<tokio::sync::Semaphore>,
    observer: Observer,
    readiness: Readiness,
) -> MainResult<()> {
    let local_addr = listener.local_addr()?;
    let mut http_server = auto::Builder::new(TokioExecutor::new());
    // hyper only enforces its header-read timeout when a timer is set. Without
    // one, a client can hold a connection open by sending headers slowly.
    http_server
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());
    let request_limit = GlobalConcurrencyLimitLayer::with_semaphore(requests);

    readiness.set(true);
    info!("server is running at http://{local_addr}");

    loop {
        let (stream, remote_addr) = tokio::select! {
            conn = accept_connection(&listener) => conn,
            result = shutdown.as_mut() => {
                readiness.set(false);
                result?;
                break;
            }
        };

        let io = TokioIo::new(stream);
        let service = ServiceBuilder::new()
            .map_request(move |mut request: hyper::Request<hyper::body::Incoming>| {
                request.extensions_mut().insert(ClientAddress(remote_addr));
                request
            })
            .layer(request_limit.clone())
            .layer(cors_layer.clone())
            .layer(S3CompatLayer)
            .service(svc.clone());
        let conn = http_server.serve_connection(
            io,
            service_fn(move |req| observer.call(service.clone(), req, remote_addr.ip())),
        );
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            _ = conn.await;
        });
    }

    drop(listener);
    tokio::select! {
        () = graceful.shutdown() => {
             info!("graceful shutdown completed");
        },
        () = sleep(Duration::from_secs(10)) => {
             tracing::warn!("graceful shutdown timed out; aborting open connections");
        }
    }

    info!("server is stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn main_error_formats_configuration_messages() {
        let error = MainError::Configuration("bad domain".to_string());
        assert_eq!(error.to_string(), "configuration error: bad domain");
    }

    #[test]
    fn main_error_conversions_preserve_error_category() {
        let config = MainError::from(ConfigError::Io(std::io::Error::other("config")));
        assert!(config.to_string().contains("Config"));

        let sqlite = MainError::from(SqliteError::BucketNotFound);
        assert!(sqlite.to_string().contains("SQLite"));

        let io = MainError::from(std::io::Error::other("bind"));
        assert!(io.to_string().contains("Io"));
    }

    #[test]
    fn main_error_sources_match_wrapped_variants() {
        let config = MainError::from(ConfigError::Io(std::io::Error::other("config")));
        assert!(config.source().is_some());
        assert!(format!("{config:?}").contains("Config"));

        let sqlite = MainError::from(SqliteError::Configuration("sqlite".to_string()));
        assert!(sqlite.source().is_some());
        assert!(format!("{sqlite:?}").contains("Sqlite"));

        let io = MainError::from(std::io::Error::other("bind"));
        assert!(io.source().is_some());
        assert!(format!("{io:?}").contains("Io"));

        let configuration = MainError::Configuration("domain".to_string());
        assert!(configuration.source().is_none());
        assert!(format!("{configuration:?}").contains("Configuration"));
    }

    #[test]
    fn main_error_display_prefixes_are_stable() {
        let config = MainError::from(ConfigError::Io(std::io::Error::other("config")));
        assert!(config.to_string().starts_with("Config "));

        let sqlite = MainError::from(SqliteError::BucketNotFound);
        assert!(sqlite.to_string().starts_with("SQLite "));

        let io = MainError::from(std::io::Error::other("bind"));
        assert!(io.to_string().starts_with("Io "));

        let configuration = MainError::Configuration("bad domain".to_string());
        assert_eq!(configuration.to_string(), "configuration error: bad domain");
    }

    #[test]
    fn main_result_alias_uses_main_error() {
        fn configuration_failure() -> MainResult<()> {
            Err(MainError::Configuration("bad".to_string()))
        }

        let error = configuration_failure().err();
        assert!(matches!(error, Some(MainError::Configuration(message)) if message == "bad"));
    }
}
