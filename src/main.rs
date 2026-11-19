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
    // The s3s default leaves SigV2 disabled, which AWS S3 also rejects.
    s3.set_config(Arc::new(StaticConfigProvider::new(
        S3Config::default().into(),
    )));

    // Validation requires both credentials, so every request is authenticated.
    let (Some(access_key), Some(secret_key)) = (
        config.access_key.clone(),
        config
            .secret_key
            .as_ref()
            .map(|key| key.expose().to_owned()),
    ) else {
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

/// The signal that started a shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownSignal {
    /// Console Ctrl+C: stop accepting connections at once.
    Interrupt,
    /// SIGTERM from an orchestrator: keep serving through [`TERMINATE_GRACE`].
    Terminate,
}

/// Waits for a console interrupt or a Unix termination signal before draining connections.
async fn shutdown_signal() -> std::io::Result<ShutdownSignal> {
    #[cfg(unix)]
    {
        let mut terminate = unix_signal(SignalKind::terminate())?;
        tokio::select! {
            result = signal::ctrl_c() => result.map(|()| ShutdownSignal::Interrupt),
            _ = terminate.recv() => Ok(ShutdownSignal::Terminate),
        }
    }
    #[cfg(not(unix))]
    signal::ctrl_c().await.map(|()| ShutdownSignal::Interrupt)
}

/// Time between SIGTERM and closing the listener, while readiness reports 503.
///
/// Kubernetes removes a pod from its Service endpoints only after readiness
/// fails, so connections keep arriving for a few seconds after SIGTERM.
/// Serving them during this window avoids refused connections.
const TERMINATE_GRACE: Duration = Duration::from_secs(5);

/// Longest open connections may take to finish once the listener closes.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// What the accept loop does once a shutdown signal arrives.
#[derive(Debug)]
enum ShutdownStep {
    /// Keep accepting through [`TERMINATE_GRACE`], then drain.
    Grace,
    /// Close the listener and drain now, returning any signal-handler error after.
    Drain(Option<std::io::Error>),
}

/// Maps a shutdown signal, or a failure to wait for one, to the next step.
///
/// A signal-handler failure still drains open connections before the error is
/// returned, so a broken handler never drops in-flight requests.
fn shutdown_step(result: std::io::Result<ShutdownSignal>) -> ShutdownStep {
    match result {
        Ok(ShutdownSignal::Terminate) => {
            info!(
                grace_seconds = TERMINATE_GRACE.as_secs(),
                "SIGTERM received; draining after grace window"
            );
            ShutdownStep::Grace
        }
        Ok(ShutdownSignal::Interrupt) => ShutdownStep::Drain(None),
        Err(error) => {
            tracing::error!(%error, "shutdown signal handler failed; draining");
            ShutdownStep::Drain(Some(error))
        }
    }
}

/// Resolves when the SIGTERM grace window ends, or never before a SIGTERM.
async fn grace_elapsed(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
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

    // Readiness drops as soon as a signal arrives. After SIGTERM the listener
    // stays open through the grace window; a signal-handler failure still
    // drains before its error is returned.
    let mut grace_deadline = None;
    let mut signal_error = None;
    loop {
        let (stream, remote_addr) = tokio::select! {
            conn = accept_connection(&listener) => conn,
            result = shutdown.as_mut(), if grace_deadline.is_none() => {
                readiness.set(false);
                match shutdown_step(result) {
                    ShutdownStep::Grace => {
                        grace_deadline = Some(tokio::time::Instant::now() + TERMINATE_GRACE);
                        continue;
                    }
                    ShutdownStep::Drain(error) => {
                        signal_error = error;
                        break;
                    }
                }
            }
            () = grace_elapsed(grace_deadline) => break,
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
        () = sleep(DRAIN_TIMEOUT) => {
             tracing::warn!("graceful shutdown timed out; aborting open connections");
        }
    }

    info!("server is stopped");
    signal_error.map_or(Ok(()), |error| Err(error.into()))
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    /// Refuses to build an S3 service without both owner credentials.
    #[tokio::test]
    async fn s3_service_requires_both_credentials() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let config = Config {
            root: root.path().to_owned(),
            maintenance_interval_seconds: 0,
            access_key: Some("access".to_owned()),
            ..Default::default()
        };
        let sqlite = Sqlite::new(&config).await?;
        assert!(matches!(
            build_s3_service(&config, sqlite.clone()),
            Err(MainError::Configuration(_))
        ));
        let config = Config {
            secret_key: "secret".parse().ok(),
            ..config
        };
        assert!(build_s3_service(&config, sqlite).is_ok());
        Ok(())
    }

    /// SIGTERM opens the grace window; Ctrl+C and handler failures drain at once.
    #[test]
    fn shutdown_signals_choose_grace_or_drain() {
        assert!(matches!(
            shutdown_step(Ok(ShutdownSignal::Terminate)),
            ShutdownStep::Grace
        ));
        assert!(matches!(
            shutdown_step(Ok(ShutdownSignal::Interrupt)),
            ShutdownStep::Drain(None)
        ));
        assert!(matches!(
            shutdown_step(Err(std::io::Error::other("handler"))),
            ShutdownStep::Drain(Some(_))
        ));
    }

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
