#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use s3ite::{Config, Error, JournalMode, Result, Sqlite};
use s3ite::{Synchronous, TempStore};

use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

use std::fs;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::PathBuf;

use clap::Parser;
use hyper::server::Server;
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
    /// The ip address to listen on for this service. Use `0.0.0.0` to listen on all interfaces.
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
    /// Controls the SQLite `journal_mode` flag pragma.
    journal_mode: Option<JournalMode>,

    #[clap(long)]
    /// Controls the SQLite `synchronous` pragma.
    synchronous: Option<Synchronous>,

    #[clap(long)]
    /// Controls the SQLite `temp_store` pragma.
    temp_store: Option<TempStore>,

    #[clap(long)]
    /// Controls the SQLite `cache_size` pragma in kilobytes.
    cache_size: Option<u32>,
}

#[tokio::main]
async fn main() -> Result {
    let env_filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let opt = Opt::parse();

    let mut config = opt
        .config
        .map(|config| {
            let config = fs::read(config)?;
            Ok::<_, Error>(serde_yaml::from_slice::<Config>(&config)?)
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

    // Parse addr
    let addr = SocketAddr::new(config.host, config.port);
    let listener = TcpListener::bind(addr)?;

    // Setup S3 provider
    let sqlite = Sqlite::new(&config).await?;

    // Setup S3 service
    let s3_service = {
        let mut s3 = S3ServiceBuilder::new(sqlite);

        // Enable authentication
        if let (Some(access_key), Some(secret_key)) = (opt.access_key, opt.secret_key) {
            s3.set_auth(SimpleAuth::from_single(access_key, secret_key));
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = &config.domain_name {
            s3.set_base_domain(domain_name);
        }

        s3.build().into_shared()
    };

    // Run server
    // Add CorsLayer if defined
    if config.permissive_cors {
        let service = Shared::new(
            ServiceBuilder::new()
                .layer(CorsLayer::very_permissive())
                .layer(ConcurrencyLimitLayer::new(config.concurrency_limit.into()))
                .service(s3_service),
        );
        let server = Server::from_tcp(listener)?.serve(service);
        info!("server is running at http://{addr}");
        server.with_graceful_shutdown(shutdown_signal()).await?;
    } else {
        let service = Shared::new(
            ServiceBuilder::new()
                .layer(ConcurrencyLimitLayer::new(config.concurrency_limit.into()))
                .service(s3_service),
        );
        let server = Server::from_tcp(listener)?.serve(service);
        info!("server is running at http://{addr}");
        server.with_graceful_shutdown(shutdown_signal()).await?;
    };

    info!("server is stopped");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
