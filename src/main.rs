#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use s3ite::Result;
use s3ite::Sqlite;

use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
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

#[derive(Debug, Parser)]
struct Opt {
    #[clap(long, default_value = "localhost")]
    host: IpAddr,

    #[clap(long, default_value = "8014")]
    port: u16,

    #[clap(long, default_value_t = true)]
    permissive_cors: bool,

    #[clap(long, requires("secret-key"))]
    access_key: Option<String>,

    #[clap(long, requires("access-key"))]
    secret_key: Option<String>,

    #[clap(long)]
    domain_name: Option<String>,

    root: PathBuf,
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
    let sqlite = Sqlite::new(opt.root).await?;

    // Setup S3 service
    let s3_service = {
        let mut s3 = S3ServiceBuilder::new(sqlite);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            s3.set_auth(SimpleAuth::from_single(ak, sk));
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
                .service(s3_service),
        );
        let server = Server::from_tcp(listener)?.serve(service);
        info!("server is running at http://{addr}");
        server.with_graceful_shutdown(shutdown_signal()).await?;
    } else {
        let service = Shared::new(ServiceBuilder::new().service(s3_service));
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
