#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::expect_used,
    clippy::pedantic,
    clippy::unwrap_used
)]

use std::net::SocketAddr;

use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
use s3ite::{Config, ConfigError, Sqlite, SqliteError};
use s3s::{auth::SimpleAuth, host::MultiDomain, service::S3ServiceBuilder};
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

mod compat;

use self::compat::{ChunkedBodyCompatLayer, HeaderAuthCompatLayer};

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

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> MainResult<()> {
    let env_filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let config = Config::from_args()?;
    let auth_compat = HeaderAuthCompatLayer::new(config.secret_key.clone());

    // Setup S3 provider
    let sqlite = Sqlite::new(&config).await?;

    // Setup S3 service
    let svc = {
        let mut s3 = S3ServiceBuilder::new(sqlite);

        // Enable authentication
        if let (Some(access_key), Some(secret_key)) = (config.access_key, config.secret_key) {
            s3.set_auth(SimpleAuth::from_single(access_key, secret_key));
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = &config.domain_name {
            let domains = [domain_name.as_str()];
            let domains = MultiDomain::new(&domains)
                .map_err(|err| MainError::Configuration(err.to_string()))?;
            s3.set_host(domains);
        }

        s3.build()
    };

    // Parse addr
    let addr = SocketAddr::new(config.host, config.port);
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let http_server = auto::Builder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("server is running at http://{local_addr}");

    loop {
        let (stream, _) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };

        let io = TokioIo::new(stream);

        // Add CorsLayer if defined
        if config.permissive_cors {
            let conn = http_server.serve_connection(
                io,
                TowerToHyperService::new(
                    tower::ServiceBuilder::new()
                        .layer(CorsLayer::very_permissive())
                        .layer(auth_compat.clone())
                        .layer(ChunkedBodyCompatLayer)
                        .layer(ConcurrencyLimitLayer::new(config.concurrency_limit.into()))
                        .service(svc.clone()),
                ),
            );
            let conn = graceful.watch(conn.into_owned());
            tokio::spawn(async move {
                _ = conn.await;
            });
        } else {
            let conn = http_server.serve_connection(
                io,
                TowerToHyperService::new(
                    tower::ServiceBuilder::new()
                        .layer(auth_compat.clone())
                        .layer(ChunkedBodyCompatLayer)
                        .layer(ConcurrencyLimitLayer::new(config.concurrency_limit.into()))
                        .service(svc.clone()),
                ),
            );
            let conn = graceful.watch(conn.into_owned());
            tokio::spawn(async move {
                _ = conn.await;
            });
        }
    }

    tokio::select! {
        () = graceful.shutdown() => {
             tracing::debug!("Gracefully shutdown!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
             tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
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
