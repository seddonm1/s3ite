#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use std::net::SocketAddr;

use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
use s3ite::{Config, Result, Sqlite};
use s3s::{auth::SimpleAuth, host::MultiDomain, service::S3ServiceBuilder};
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let config = Config::from_args()?;

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
            s3.set_host(MultiDomain::new(&[domain_name]).unwrap());
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
                        .layer(ConcurrencyLimitLayer::new(config.concurrency_limit.into()))
                        .service(svc.clone()),
                ),
            );
            let conn = graceful.watch(conn.into_owned());
            tokio::spawn(async move {
                let _ = conn.await;
            });
        } else {
            let conn = http_server.serve_connection(
                io,
                TowerToHyperService::new(
                    tower::ServiceBuilder::new()
                        .layer(ConcurrencyLimitLayer::new(config.concurrency_limit.into()))
                        .service(svc.clone()),
                ),
            );
            let conn = graceful.watch(conn.into_owned());
            tokio::spawn(async move {
                let _ = conn.await;
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
