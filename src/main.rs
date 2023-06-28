#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use s3ite::{JournalMode, Pragmas, Result, Sqlite, Synchronous, TempStore};

use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

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
    #[clap(long, default_value = "127.0.0.1")]
    /// The ip address to listen on for this service. Use `0.0.0.0` to listen on all interfaces.
    host: IpAddr,

    #[clap(long, default_value = "8014")]
    /// The port to listen on for this service.
    port: u16,

    #[clap(long, default_value_t = true)]
    /// Allow permissive Cross-Origin Resource Sharing (CORS) requests.
    /// This can be enabled to allow users to access this service from a web service running on a different host.
    permissive_cors: bool,

    #[clap(long, requires = "secret_key")]
    /// The access key ID that is used to authenticate for this service.
    access_key: Option<String>,

    #[clap(long, requires = "access_key")]
    /// The secret access key that is used to authenticate for this service.
    secret_key: Option<String>,

    #[clap(long)]
    /// The domain to use to allow parsing virtual-hosted-style requests.
    domain_name: Option<String>,

    #[clap(long, default_value_t = 16)]
    /// Enforces a limit on the concurrent number of requests the underlying service can handle.
    /// This can be tuned depending on infrastructure as SSD/HDD will deal with resource contention very differently.
    concurrency_limit: usize,

    /// Controls the SQLite `journal_mode` flag pragma.
    #[clap(long, default_value = "wal")]
    sqlite_journal_mode: JournalMode,

    #[clap(long, default_value = "normal")]
    /// Controls the SQLite `synchronous` pragma.
    sqlite_synchronous: Synchronous,

    #[clap(long, default_value = "memory")]
    /// Controls the SQLite `temp_store` pragma.
    sqlite_temp_store: TempStore,

    #[clap(long, default_value_t = 67108864)]
    /// Controls the SQLite `cache_size` pragma in kilobytes.
    sqlite_cache_size: u32,

    #[clap(long, default_value_t = false)]
    /// Controls the SQLite `query_only` pragma. Can be used to prevent database mutation.
    sqlite_query_only: bool,

    /// The base path where the `.sqlite3` files will be created.
    /// All `.sqlite3` files at this path will be loaded at startup and exposed via this service.
    root: PathBuf,
}

impl From<&Opt> for Pragmas {
    fn from(val: &Opt) -> Self {
        Self {
            journal_mode: val.sqlite_journal_mode,
            synchronous: val.sqlite_synchronous,
            temp_store: val.sqlite_temp_store,
            cache_size: val.sqlite_cache_size,
            query_only: val.sqlite_query_only,
            foreign_keys: true,
        }
    }
}

#[tokio::main]
async fn main() -> Result {
    let env_filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let opt = Opt::parse();

    // Parse addr
    let addr = SocketAddr::new(opt.host, opt.port);
    let listener = TcpListener::bind(addr)?;

    // Setup S3 provider
    let sqlite = Sqlite::new(&opt.root, Pragmas::from(&opt)).await?;

    // Setup S3 service
    let s3_service = {
        let mut s3 = S3ServiceBuilder::new(sqlite);

        // Enable authentication
        if let (Some(access_key), Some(secret_key)) = (opt.access_key, opt.secret_key) {
            s3.set_auth(SimpleAuth::from_single(access_key, secret_key));
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = opt.domain_name {
            s3.set_base_domain(domain_name);
        }

        s3.build().into_shared()
    };

    // Run server
    // Add CorsLayer if defined
    if opt.permissive_cors {
        let service = Shared::new(
            ServiceBuilder::new()
                .layer(CorsLayer::very_permissive())
                .layer(ConcurrencyLimitLayer::new(opt.concurrency_limit))
                .service(s3_service),
        );
        let server = Server::from_tcp(listener)?.serve(service);
        info!("server is running at http://{addr}");
        server.with_graceful_shutdown(shutdown_signal()).await?;
    } else {
        let service = Shared::new(
            ServiceBuilder::new()
                .layer(ConcurrencyLimitLayer::new(opt.concurrency_limit))
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
