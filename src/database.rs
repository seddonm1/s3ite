#![forbid(unsafe_code)]
#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enums,
    clippy::enum_glob_use,
    clippy::inefficient_to_string,
    clippy::mem_forget,
    clippy::mutex_integer,
    clippy::needless_continue,
    clippy::todo,
    clippy::unimplemented,
    clippy::wildcard_imports,
    missing_docs,
    missing_debug_implementations
)]

use std::{
    fmt::{self, Debug},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    thread::JoinHandle,
};

use crossbeam_channel::Sender;
use rusqlite::{OpenFlags, TransactionBehavior};
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::{
    Config,
    error::{Result, S3ite},
};

static MESSAGE_BOUND: usize = 100;

const BUG_TEXT: &str = "bug in tokio-rusqlite, please report";

type CallFn = Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>;

pub enum Message {
    Execute(CallFn),
    Close,
}

impl Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execute(_arg0) => f.debug_tuple("Execute").finish(),
            Self::Close => write!(f, "Close"),
        }
    }
}

/// A handle to call functions in background thread.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    writer_sender: Sender<Message>,
    reader_sender: Sender<Message>,
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    reader_handles: Mutex<Vec<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.close_blocking().ok();
        }
    }
}

impl Connection {
    /// Open a new connection to a `SQLite` database.
    ///
    /// `Connection::open(path)` is equivalent to
    /// `Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE |
    /// OpenFlags::SQLITE_OPEN_CREATE)`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying `SQLite` open call fails.
    pub(crate) async fn open<P: AsRef<Path>>(
        path: P,
        config: &Config,
        bucket: &str,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let path_clone = path.clone();

        let config = config.to_owned();
        let bucket = bucket.to_string();
        let readers = config.readers;
        if readers == 0 {
            return Err(S3ite::Configuration(
                "readers must be greater than zero".to_string(),
            ));
        }

        connect(
            move || {
                let mut writer = rusqlite::Connection::open(path)?;
                writer.set_transaction_behavior(TransactionBehavior::Immediate);
                writer.execute_batch(&config.to_sql(Some(&bucket)))?;

                Ok(writer)
            },
            Arc::new(move || {
                let mut reader = rusqlite::Connection::open_with_flags(
                    path_clone.clone(),
                    OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                reader.set_transaction_behavior(TransactionBehavior::Deferred);

                Ok(reader)
            }),
            readers,
        )
        .await
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the database connection has been closed.
    pub(crate) async fn write<F, R>(&self, function: F) -> Result<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R> + 'static + Send,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<Result<R>>();

        if self.inner.closed.load(Ordering::Acquire) {
            return Err(S3ite::DatabaseQueueClosed);
        }

        self.inner
            .writer_sender
            .try_send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))?;

        if self.inner.closed.load(Ordering::Acquire) {
            return Err(S3ite::DatabaseQueueClosed);
        }

        receiver.await?
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the database connection has been closed.
    pub(crate) async fn read<F, R>(&self, function: F) -> Result<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R> + 'static + Send,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<Result<R>>();

        if self.inner.closed.load(Ordering::Acquire) {
            return Err(S3ite::DatabaseQueueClosed);
        }

        self.inner
            .reader_sender
            .try_send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))?;

        if self.inner.closed.load(Ordering::Acquire) {
            return Err(S3ite::DatabaseQueueClosed);
        }

        receiver.await?
    }

    /// Close the database connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for
    /// `Connection`. It consumes the `Connection`, but on error returns it
    /// to the caller for retry purposes.
    ///
    /// If successful, any following `close` operations performed
    /// on `Connection` copies will succeed immediately.
    ///
    /// On the other hand, any calls to [`Connection::call`] will return a [`Error::ConnectionClosed`],
    /// and any calls to [`Connection::call_unwrap`] will cause a `panic`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying `SQLite` close call fails.
    pub(crate) async fn close(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.close_blocking())
            .await
            .map_err(|_| S3ite::ThreadJoin)?
    }
}

impl ConnectionInner {
    fn close_blocking(&self) -> Result<()> {
        let should_signal = self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        if should_signal {
            let reader_count = self.reader_handles.lock().unwrap().len();
            for _ in 0..reader_count {
                self.reader_sender.send(Message::Close).ok();
            }

            self.writer_sender.send(Message::Close).ok();
        }

        let mut join_failed = false;

        {
            let mut reader_handles = self.reader_handles.lock().unwrap();
            for reader_handle in reader_handles.drain(..) {
                if reader_handle.join().is_err() {
                    join_failed = true;
                }
            }
        }

        {
            let mut writer_handle = self.writer_handle.lock().unwrap();
            if let Some(writer_handle) = writer_handle.take()
                && writer_handle.join().is_err()
            {
                join_failed = true;
            }
        }

        if join_failed {
            return Err(S3ite::ThreadJoin);
        }

        Ok(())
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

async fn connect<F, G>(open_writer: F, open_reader: Arc<G>, readers: usize) -> Result<Connection>
where
    F: FnOnce() -> rusqlite::Result<rusqlite::Connection> + Send + 'static,
    G: Fn() -> rusqlite::Result<rusqlite::Connection> + Send + Sync + 'static,
{
    let (writer_sender, writer_receiver) = crossbeam_channel::bounded::<Message>(MESSAGE_BOUND);
    let (writer_result_sender, writer_result_receiver) = oneshot::channel();

    let writer_handle = thread::spawn(move || {
        debug!(
            "spawn writer on thread id: {:?}",
            std::thread::current().id()
        );
        let mut conn = match open_writer() {
            Ok(c) => c,
            Err(e) => {
                let _ = writer_result_sender.send(Err(e));
                return;
            }
        };

        if let Err(_e) = writer_result_sender.send(Ok(())) {
            return;
        }

        while let Ok(message) = writer_receiver.recv() {
            match message {
                Message::Execute(f) => f(&mut conn),
                Message::Close => {
                    debug!(
                        "close writer on thread id: {:?}",
                        std::thread::current().id()
                    );

                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);", ()).ok();

                    match conn.close() {
                        Ok(()) => {
                            break;
                        }
                        Err((_c, e)) => {
                            error!("{:?}", e);
                            break;
                        }
                    }
                }
            }
        }
    });
    writer_result_receiver.await.expect(BUG_TEXT)?;

    let (reader_sender, reader_receiver) = crossbeam_channel::bounded::<Message>(MESSAGE_BOUND);
    let mut reader_handles = Vec::with_capacity(readers);
    for _ in 0..readers {
        let (reader_result_sender, reader_result_receiver) = oneshot::channel();
        let reader_receiver = reader_receiver.clone();
        let open_reader = open_reader.clone();
        reader_handles.push(thread::spawn(move || {
            debug!(
                "spawn reader on thread id: {:?}",
                std::thread::current().id()
            );

            let mut conn = match open_reader() {
                Ok(c) => c,
                Err(e) => {
                    error!("{:?}", e);
                    let _ = reader_result_sender.send(Err(e));
                    return;
                }
            };

            if let Err(_e) = reader_result_sender.send(Ok(())) {
                return;
            }

            while let Ok(message) = reader_receiver.recv() {
                match message {
                    Message::Execute(f) => f(&mut conn),
                    Message::Close => {
                        debug!(
                            "close reader on thread id: {:?}",
                            std::thread::current().id()
                        );

                        match conn.close() {
                            Ok(()) => {
                                break;
                            }
                            Err((_c, e)) => {
                                error!("{:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
        }));
        reader_result_receiver.await.expect(BUG_TEXT)?;
    }

    Ok(Connection {
        inner: Arc::new(ConnectionInner {
            writer_sender,
            reader_sender,
            writer_handle: Mutex::new(Some(writer_handle)),
            reader_handles: Mutex::new(reader_handles),
            closed: AtomicBool::new(false),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_is_idempotent_and_rejects_later_operations() {
        let path = std::env::temp_dir().join(format!("s3ite-{}.sqlite3", uuid::Uuid::new_v4()));
        std::fs::remove_file(&path).ok();

        let config = Config {
            readers: 1,
            ..Default::default()
        };
        let connection = Connection::open(&path, &config, "bucket").await.unwrap();

        connection.close().await.unwrap();
        connection.close().await.unwrap();

        let err = connection.read(|_| Ok(())).await.unwrap_err();
        assert!(matches!(err, S3ite::DatabaseQueueClosed));

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.to_string_lossy())).ok();
        std::fs::remove_file(format!("{}-shm", path.to_string_lossy())).ok();
    }
}
