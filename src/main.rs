#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use clap::Parser;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
use s3ite::{Config, JournalMode, Result, S3ite, Sqlite, Synchronous, TempStore};
use s3s::{auth::SimpleAuth, host::MultiDomain, service::S3ServiceBuilder};
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opt {
    /// The base path where the `.sqlite3` files will be created.
    /// All `.sqlite3` files at this path will be loaded at startup and exposed via this service.
    #[clap(long)]
    root: Option<PathBuf>,

    #[clap(long)]
    /// The domain to use to allow parsing virtual-hosted-style requests.
    config: Option<PathBuf>,

    #[clap(long)]
    /// The IP address to listen on for this service. Use `0.0.0.0` to listen on all interfaces.
    host: Option<IpAddr>,

    #[clap(long)]
    /// The port to listen on for this service.
    port: Option<u16>,

    #[clap(long)]
    /// Allow permissive Cross-Origin Resource Sharing (CORS) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    permissive_cors: Option<bool>,

    #[clap(long, requires = "secret_key")]
    /// The access key ID that is used to authenticate for this service.
    access_key: Option<String>,

    #[clap(long, requires = "access_key")]
    /// The secret access key that is used to authenticate for this service.
    secret_key: Option<String>,

    #[clap(long)]
    /// The domain to use to allow parsing virtual-hosted-style requests.
    domain_name: Option<String>,

    #[clap(long)]
    /// Enforces a limit on the concurrent number of requests the underlying service can handle.
    /// This can be tuned depending on infrastructure as SSD/HDD will deal with resource contention very differently.
    concurrency_limit: Option<u16>,

    #[clap(long)]
    /// If this service should be read-only
    read_only: Option<bool>,

    #[clap(long)]
    /// Controls the `SQLite` `journal_mode` flag pragma.
    journal_mode: Option<JournalMode>,

    #[clap(long)]
    /// Controls the `SQLite` `synchronous` pragma.
    synchronous: Option<Synchronous>,

    #[clap(long)]
    /// Controls the `SQLite` `temp_store` pragma.
    temp_store: Option<TempStore>,

    #[clap(long)]
    /// Controls the `SQLite` `cache_size` pragma in kilobytes.
    cache_size: Option<u32>,

    #[clap(long)]
    /// Controls the number of reader connections to `SQLite`
    readers: Option<usize>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let opt = Opt::parse();

    let mut config = opt
        .config
        .map(|config| {
            let config = fs::read(config)?;
            Ok::<_, S3ite>(serde_yaml::from_slice::<Config>(&config)?)
        })
        .transpose()?
        .unwrap_or_default();

    // cli arguments override config
    if let Some(root) = opt.root {
        config.root = root;
    }
    if let Some(host) = opt.host {
        config.host = host;
    }
    if let Some(port) = opt.port {
        config.port = port;
    }
    if let Some(access_key) = opt.access_key {
        config.access_key = Some(access_key);
    }
    if let Some(secret_key) = opt.secret_key {
        config.secret_key = Some(secret_key);
    }
    if let Some(permissive_cors) = opt.permissive_cors {
        config.permissive_cors = permissive_cors;
    }
    if let Some(domain_name) = opt.domain_name {
        config.domain_name = Some(domain_name);
    }
    if let Some(concurrency_limit) = opt.concurrency_limit {
        config.concurrency_limit = concurrency_limit;
    }
    if let Some(read_only) = opt.read_only {
        config.read_only = read_only;
    }
    if let Some(journal_mode) = opt.journal_mode {
        config.sqlite.journal_mode = journal_mode;
    }
    if let Some(synchronous) = opt.synchronous {
        config.sqlite.synchronous = synchronous;
    }
    if let Some(temp_store) = opt.temp_store {
        config.sqlite.temp_store = temp_store;
    }
    if let Some(cache_size) = opt.cache_size {
        config.sqlite.cache_size = cache_size;
    }

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

        s3.build().into_shared()
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
