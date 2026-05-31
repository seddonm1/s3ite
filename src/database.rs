use std::{
    fmt::{self, Debug},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
    thread::JoinHandle,
};

use rusqlite::{OpenFlags, TransactionBehavior};
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::{
    Config,
    sqlite::{SqliteError, SqliteResult},
};

/// Maximum number of queued commands per worker queue.
const QUEUE_BOUND: usize = 100;

/// Unit of work executed by a worker-owned database connection.
type CallFn = Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>;

/// Shared receiving end for a worker queue.
type SharedReceiver = Arc<Mutex<Receiver<WorkerCommand>>>;

/// Command sent to a database worker thread.
enum WorkerCommand {
    /// Runs a closure against the worker-owned database connection.
    Execute(CallFn),
    /// Closes the worker-owned database connection and exits the worker loop.
    Close,
}

impl Debug for WorkerCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execute(_arg0) => f.debug_tuple("Execute").finish(),
            Self::Close => write!(f, "Close"),
        }
    }
}

/// Handle for scheduling work on a bucket's database worker threads.
#[derive(Clone)]
pub struct BucketConnection {
    inner: Arc<BucketConnectionInner>,
}

impl Debug for BucketConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketConnection")
            .field("closed", &self.inner.closed)
            .finish_non_exhaustive()
    }
}

/// Shared worker state behind cloned connection handles.
struct BucketConnectionInner {
    writer_sender: SyncSender<WorkerCommand>,
    reader_sender: SyncSender<WorkerCommand>,
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    reader_handles: Mutex<Vec<JoinHandle<()>>>,
    closed: AtomicBool,
}

impl Drop for BucketConnection {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.close_blocking().ok();
        }
    }
}

impl BucketConnection {
    /// Opens a bucket database with one writer and configured readers.
    ///
    /// The writer uses the bucket's configured pragmas, while readers open
    /// read-only connections with deferred transactions.
    ///
    /// # Errors
    ///
    /// Returns an error when no readers are configured, the database cannot open, or
    /// the configured pragmas fail.
    pub(crate) async fn open<P: AsRef<Path>>(
        path: P,
        config: &Config,
        bucket: &str,
    ) -> SqliteResult<Self> {
        let path = path.as_ref().to_owned();
        let path_clone = path.clone();

        let config = config.to_owned();
        let bucket = bucket.to_string();
        let readers = config.readers;
        if readers == 0 {
            return Err(SqliteError::Configuration(
                "readers must be greater than zero".to_string(),
            ));
        }

        let inner = BucketConnectionInner::connect(
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
        .await?;

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Runs a closure on the writer connection.
    ///
    /// The closure runs on the dedicated writer thread and its result is
    /// returned asynchronously to the caller.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed, the bounded work queue is
    /// full, the response channel closes, or the closure returns an error.
    pub(crate) async fn write<F, R>(&self, function: F) -> SqliteResult<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> SqliteResult<R> + 'static + Send,
        R: Send + 'static,
    {
        self.inner.call(&self.inner.writer_sender, function).await
    }

    /// Runs a closure on a reader connection.
    ///
    /// The closure runs on one of the dedicated reader threads and its result is
    /// returned asynchronously to the caller.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed, the bounded work queue is
    /// full, the response channel closes, or the closure returns an error.
    pub(crate) async fn read<F, R>(&self, function: F) -> SqliteResult<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> SqliteResult<R> + 'static + Send,
        R: Send + 'static,
    {
        self.inner.call(&self.inner.reader_sender, function).await
    }

    /// Closes all database worker threads.
    ///
    /// Closing is idempotent across cloned handles. After close begins, new
    /// read and write calls fail with a closed-queue error.
    ///
    /// # Errors
    ///
    /// Returns an error if the blocking close task or a worker thread fails to
    /// join.
    pub(crate) async fn close(&self) -> SqliteResult<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.close_blocking())
            .await
            .map_err(|_| SqliteError::ThreadJoin)?
    }
}

impl BucketConnectionInner {
    /// Spawns database worker threads and waits for each connection to open.
    async fn connect<F, G>(
        open_writer: F,
        open_reader: Arc<G>,
        readers: usize,
    ) -> SqliteResult<Self>
    where
        F: FnOnce() -> rusqlite::Result<rusqlite::Connection> + Send + 'static,
        G: Fn() -> rusqlite::Result<rusqlite::Connection> + Send + Sync + 'static,
    {
        let (writer_sender, writer_receiver) = sync_channel::<WorkerCommand>(QUEUE_BOUND);
        let (writer_handle, writer_started) =
            WorkerRole::Writer.spawn(Arc::new(Mutex::new(writer_receiver)), open_writer);
        Self::wait_for_worker(writer_started).await?;

        let (reader_sender, reader_receiver) = sync_channel::<WorkerCommand>(QUEUE_BOUND);
        let reader_receiver = Arc::new(Mutex::new(reader_receiver));
        let mut reader_handles = Vec::with_capacity(readers);
        for _ in 0..readers {
            let reader_receiver = reader_receiver.clone();
            let open_reader = open_reader.clone();
            let (reader_handle, reader_started) =
                WorkerRole::Reader.spawn(reader_receiver, move || open_reader());
            Self::wait_for_worker(reader_started).await?;
            reader_handles.push(reader_handle);
        }

        Ok(Self {
            writer_sender,
            reader_sender,
            writer_handle: Mutex::new(Some(writer_handle)),
            reader_handles: Mutex::new(reader_handles),
            closed: AtomicBool::new(false),
        })
    }

    /// Runs work on the selected worker queue and returns the closure result.
    async fn call<F, R>(&self, queue: &SyncSender<WorkerCommand>, function: F) -> SqliteResult<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> SqliteResult<R> + 'static + Send,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<SqliteResult<R>>();

        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::DatabaseQueueClosed);
        }

        queue
            .try_send(WorkerCommand::Execute(Box::new(move |conn| {
                let value = function(conn);
                _ = sender.send(value);
            })))
            .map_err(Self::queue_send_error)?;

        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::DatabaseQueueClosed);
        }

        receiver.await.map_err(|_| SqliteError::ThreadJoin)?
    }

    /// Waits for a worker thread to finish opening its database connection.
    async fn wait_for_worker(
        receiver: oneshot::Receiver<rusqlite::Result<()>>,
    ) -> SqliteResult<()> {
        receiver.await.map_err(|_| SqliteError::ThreadJoin)??;
        Ok(())
    }

    /// Converts bounded queue send failures into database queue errors.
    fn queue_send_error<T>(error: TrySendError<T>) -> SqliteError {
        match error {
            TrySendError::Full(value) => {
                drop(value);
                SqliteError::DatabaseQueueFull
            }
            TrySendError::Disconnected(value) => {
                drop(value);
                SqliteError::DatabaseQueueClosed
            }
        }
    }

    /// Signals workers to close and joins their threads.
    fn close_blocking(&self) -> SqliteResult<()> {
        let should_signal = self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        if should_signal {
            let reader_count = self
                .reader_handles
                .lock()
                .map_err(|_| SqliteError::ThreadJoin)?
                .len();
            for _ in 0..reader_count {
                self.reader_sender.send(WorkerCommand::Close).ok();
            }

            self.writer_sender.send(WorkerCommand::Close).ok();
        }

        let mut join_failed = false;

        {
            let mut reader_handles = self
                .reader_handles
                .lock()
                .map_err(|_| SqliteError::ThreadJoin)?;
            for reader_handle in reader_handles.drain(..) {
                if reader_handle.join().is_err() {
                    join_failed = true;
                }
            }
        }

        {
            let mut writer_handle = self
                .writer_handle
                .lock()
                .map_err(|_| SqliteError::ThreadJoin)?;
            if let Some(writer_handle) = writer_handle.take()
                && writer_handle.join().is_err()
            {
                join_failed = true;
            }
        }

        if join_failed {
            return Err(SqliteError::ThreadJoin);
        }

        Ok(())
    }
}

/// Identifies the kind of database worker being spawned or closed.
#[derive(Clone, Copy)]
enum WorkerRole {
    /// Worker role used for read-only database connections.
    Reader,
    /// Worker role used for the single writable database connection.
    Writer,
}

impl WorkerRole {
    /// Returns the role name used in tracing output.
    fn name(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }

    /// Spawns a worker thread and returns a startup notification receiver.
    fn spawn<F>(
        self,
        receiver: SharedReceiver,
        open_connection: F,
    ) -> (JoinHandle<()>, oneshot::Receiver<rusqlite::Result<()>>)
    where
        F: FnOnce() -> rusqlite::Result<rusqlite::Connection> + Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();
        let handle = thread::spawn(move || {
            debug!(
                "spawn {} on thread id: {:?}",
                self.name(),
                std::thread::current().id()
            );
            let conn = match open_connection() {
                Ok(connection) => connection,
                Err(error) => {
                    _ = result_sender.send(Err(error));
                    return;
                }
            };

            if result_sender.send(Ok(())).is_err() {
                return;
            }

            self.run(conn, &receiver);
        });

        (handle, result_receiver)
    }

    /// Runs the worker command loop until the queue closes or shutdown is requested.
    fn run(self, mut connection: rusqlite::Connection, receiver: &SharedReceiver) {
        loop {
            let message = {
                let Ok(receiver) = receiver.lock() else {
                    return;
                };
                receiver.recv()
            };

            let Ok(message) = message else {
                return;
            };

            match message {
                WorkerCommand::Execute(function) => function(&mut connection),
                WorkerCommand::Close => {
                    self.close_connection(connection);
                    return;
                }
            }
        }
    }

    /// Closes a worker-owned database connection.
    fn close_connection(self, connection: rusqlite::Connection) {
        debug!(
            "close {} on thread id: {:?}",
            self.name(),
            std::thread::current().id()
        );

        if matches!(self, Self::Writer) {
            connection
                .execute("PRAGMA wal_checkpoint(TRUNCATE);", ())
                .ok();
        }

        if let Err((_connection, error)) = connection.close() {
            error!("{:?}", error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_is_idempotent_and_rejects_later_operations() -> SqliteResult<()> {
        let path = std::env::temp_dir().join(format!("s3ite-{}.sqlite3", uuid::Uuid::new_v4()));
        std::fs::remove_file(&path).ok();

        let config = Config {
            readers: 1,
            ..Default::default()
        };
        let connection = BucketConnection::open(&path, &config, "bucket").await?;

        connection.close().await?;
        connection.close().await?;

        let Err(err) = connection.read(|_| Ok(())).await else {
            panic!("read after close should fail");
        };
        assert!(matches!(err, SqliteError::DatabaseQueueClosed));

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.to_string_lossy())).ok();
        std::fs::remove_file(format!("{}-shm", path.to_string_lossy())).ok();
        Ok(())
    }
}
