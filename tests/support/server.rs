use std::{
    fs::{self, File},
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

const ACCESS_KEY: &str = "s3ite-access-key";
const SECRET_KEY: &str = "s3ite-secret-key-for-tests";
const DEFAULT_RUST_LOG: &str = "s3ite=debug,s3s=warn";
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const COVERAGE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const COVERAGE_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug)]
pub struct TestServer {
    child: Child,
    root: Option<TempDir>,
    data_root: PathBuf,
    log_path: PathBuf,
    endpoint: String,
}

#[derive(Debug, Default)]
pub struct TestServerOptions {
    pub domain_name: Option<String>,
    pub permissive_cors: Option<bool>,
    pub rust_log: Option<String>,
}

impl TestServer {
    pub async fn spawn() -> Result<Self> {
        Self::spawn_with(TestServerOptions::default()).await
    }

    pub async fn spawn_with(options: TestServerOptions) -> Result<Self> {
        let root = TempDir::new().context("create temporary s3ite root")?;
        let data_root = root.path().join("data");
        fs::create_dir_all(&data_root).context("create temporary bucket root")?;

        let log_path = root.path().join("s3ite.log");
        let log = File::create(&log_path).context("create s3ite test log")?;
        let stderr = log.try_clone().context("clone s3ite test log")?;

        let port = allocate_port().context("allocate local s3ite port")?;
        let endpoint = format!("http://127.0.0.1:{port}");
        let binary = s3ite_binary()?;
        let rust_log = options.rust_log.unwrap_or_else(|| {
            std::env::var("S3ITE_TEST_LOG").unwrap_or_else(|_| DEFAULT_RUST_LOG.to_string())
        });

        let mut command = Command::new(&binary);
        command
            .arg("--root")
            .arg(&data_root)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--access-key")
            .arg(ACCESS_KEY)
            .arg("--secret-key")
            .arg(SECRET_KEY)
            .arg("--maintenance-interval-seconds")
            .arg("0");
        if let Some(domain_name) = options.domain_name {
            command.arg("--domain-name").arg(domain_name);
        }
        if let Some(permissive_cors) = options.permissive_cors {
            command
                .arg("--permissive-cors")
                .arg(permissive_cors.to_string());
        }

        let mut child = command
            .env("RUST_LOG", rust_log)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| format!("spawn s3ite binary {}", binary.display()))?;

        wait_for_ready(&mut child, port, &log_path).await?;

        Ok(Self {
            child,
            root: Some(root),
            data_root,
            log_path,
            endpoint,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn shutdown_and_read_log(mut self) -> Result<String> {
        request_graceful_shutdown(&self.child).context("request graceful s3ite shutdown")?;
        wait_for_child_exit(&mut self.child, COVERAGE_SHUTDOWN_TIMEOUT)
            .context("wait for graceful s3ite shutdown")?;
        fs::read_to_string(&self.log_path)
            .with_context(|| format!("read {}", self.log_path.display()))
    }

    pub fn access_key(&self) -> &str {
        ACCESS_KEY
    }

    pub fn secret_key(&self) -> &str {
        SECRET_KEY
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if coverage_run() && request_graceful_shutdown(&self.child).is_ok() {
            let _ = wait_for_child_exit(&mut self.child, COVERAGE_SHUTDOWN_TIMEOUT);
        }

        let _ = self.child.kill();
        let _ = self.child.wait();

        if std::env::var_os("S3ITE_TEST_KEEP_ROOT").is_some()
            && let Some(root) = self.root.take()
        {
            eprintln!(
                "kept s3ite test root at {} with data in {} and log at {}",
                root.path().display(),
                self.data_root.display(),
                self.log_path.display()
            );
            let _ = root.keep();
        }
    }
}

fn coverage_run() -> bool {
    std::env::var_os("CARGO_LLVM_COV").is_some() || std::env::var_os("LLVM_PROFILE_FILE").is_some()
}

#[cfg(unix)]
fn request_graceful_shutdown(child: &Child) -> io::Result<()> {
    let status = Command::new("kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "kill -INT exited with status {status}"
        )))
    }
}

#[cfg(not(unix))]
fn request_graceful_shutdown(_child: &Child) -> io::Result<()> {
    Err(io::Error::other(
        "graceful coverage shutdown is only supported on Unix",
    ))
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for s3ite process to exit",
            ));
        }
        std::thread::sleep(COVERAGE_SHUTDOWN_POLL_INTERVAL);
    }
}

fn s3ite_binary() -> Result<PathBuf> {
    if let Ok(binary) = std::env::var("S3ITE_TEST_BIN") {
        return Ok(PathBuf::from(binary));
    }

    if let Some(binary) = option_env!("CARGO_BIN_EXE_s3ite") {
        return Ok(PathBuf::from(binary));
    }

    if let Ok(binary) = std::env::var("CARGO_BIN_EXE_s3ite") {
        return Ok(PathBuf::from(binary));
    }

    bail!(
        "CARGO_BIN_EXE_s3ite is unavailable; run `cargo test --test conformance_bucket` or set S3ITE_TEST_BIN"
    );
}

fn allocate_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

async fn wait_for_ready(child: &mut Child, port: u16, log_path: &Path) -> Result<()> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let address = SocketAddr::from(([127, 0, 0, 1], port));

    loop {
        if let Some(status) = child.try_wait().context("poll s3ite process")? {
            bail!(
                "s3ite exited before accepting connections with {status}; log tail:\n{}",
                log_tail(log_path)
            );
        }

        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            return Ok(());
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for s3ite at {address}; log tail:\n{}",
                log_tail(log_path)
            );
        }

        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

fn log_tail(path: &Path) -> String {
    match fs::read_to_string(path) {
        Ok(log) => {
            let lines = log.lines().rev().take(80).collect::<Vec<_>>();
            lines.into_iter().rev().collect::<Vec<_>>().join("\n")
        }
        Err(error) => format!("failed to read {}: {error}", path.display()),
    }
}
