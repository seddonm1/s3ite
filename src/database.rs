#![forbid(unsafe_code)]
#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enum,
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
    sync::Arc,
    thread,
    thread::JoinHandle,
};

use crossbeam_channel::Sender;
use rusqlite::{OpenFlags, TransactionBehavior};
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::{error::Result, Config};

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
    writer_sender: Sender<Message>,
    reader_sender: Sender<Message>,
    writer_handle: Arc<JoinHandle<()>>,
    reader_handles: Arc<Vec<JoinHandle<()>>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
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
        let readers = config.concurrency_limit as usize;

        Ok(connect(
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
        .await?)
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

        self.writer_sender
            .send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))?;

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

        self.reader_sender
            .send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))?;

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
    #[allow(dead_code)]
    pub(crate) fn close(&self) {
        // close readers
        for _ in 0..self.reader_handles.len() {
            self.reader_sender.send(Message::Close).ok();
        }
        while self
            .reader_handles
            .iter()
            .any(|reader_handle| !reader_handle.is_finished())
        {}

        // close writer
        self.writer_sender.send(Message::Close).ok();
        while !self.writer_handle.is_finished() {}
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

async fn connect<F, G>(
    open_writer: F,
    open_reader: Arc<G>,
    readers: usize,
) -> rusqlite::Result<Connection>
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
        writer_sender,
        reader_sender,
        writer_handle: Arc::new(writer_handle),
        reader_handles: Arc::new(reader_handles),
    })
}
