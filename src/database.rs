use std::{
    fmt::{self, Debug},
    io::{ErrorKind, Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use bytes::Bytes;
use rusqlite::{MAIN_DB, OpenFlags, Transaction, TransactionBehavior};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{debug, error};

use crate::{
    Config,
    sqlite::{SqliteError, SqliteResult},
};

/// Maximum number of queued commands per worker queue.
const QUEUE_BOUND: usize = 100;
/// Size of each chunk read from an incremental `SQLite` BLOB.
const BLOB_STREAM_CHUNK_SIZE: u64 = 128 * 1024;
/// Number of BLOB chunks that may wait for the HTTP body consumer.
const BLOB_STREAM_QUEUE_BOUND: usize = 4;
/// Sleep interval while waiting for BLOB response backpressure to clear.
const BLOB_STREAM_SEND_RETRY: Duration = Duration::from_millis(10);

/// Receiver side of a streamed `SQLite` BLOB read.
pub(crate) struct BlobReadReceiver {
    receiver: tokio_mpsc::Receiver<SqliteResult<Bytes>>,
    timed_out: Arc<AtomicBool>,
}

impl BlobReadReceiver {
    /// Receives the next streamed BLOB chunk.
    pub(crate) async fn recv(&mut self) -> Option<SqliteResult<Bytes>> {
        self.receiver.recv().await
    }

    /// Returns whether the worker stopped because output backpressure timed out.
    pub(crate) fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Acquire)
    }
}

/// Rowid-based `SQLite` BLOB span to stream from a worker connection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BlobReadRequest {
    table: &'static str,
    column: &'static str,
    rowid: i64,
    start: u64,
    length: u64,
    expected_length: u64,
}

impl BlobReadRequest {
    /// Creates a request for a byte span in a rowid-addressed `SQLite` BLOB.
    #[must_use]
    pub(crate) const fn new(
        table: &'static str,
        column: &'static str,
        rowid: i64,
        start: u64,
        length: u64,
        expected_length: u64,
    ) -> Self {
        Self {
            table,
            column,
            rowid,
            start,
            length,
            expected_length,
        }
    }
}

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
    blob_reader_sender: SyncSender<WorkerCommand>,
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    reader_handles: Mutex<Vec<JoinHandle<()>>>,
    blob_reader_handles: Mutex<Vec<JoinHandle<()>>>,
    blob_read_timeout: Option<Duration>,
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
    /// Opens a bucket database with one writer and configured reader pools.
    ///
    /// The writer uses the bucket's configured pragmas. Metadata readers and
    /// BLOB readers open read-only connections with deferred transactions.
    ///
    /// # Errors
    ///
    /// Returns an error when no readers are configured, the database cannot
    /// open, or the configured pragmas fail.
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
        let blob_readers = config.blob_readers;
        let blob_read_timeout = (config.blob_read_timeout_seconds > 0)
            .then(|| Duration::from_secs(config.blob_read_timeout_seconds));
        if readers == 0 {
            return Err(SqliteError::Configuration(
                "readers must be greater than zero".to_string(),
            ));
        }
        if blob_readers == 0 {
            return Err(SqliteError::Configuration(
                "blob_readers must be greater than zero".to_string(),
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
            blob_readers,
            blob_read_timeout,
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

    /// Opens a read transaction, returns metadata, and streams a selected BLOB span.
    ///
    /// The preparation closure runs on a reader thread. When it returns a BLOB
    /// request, that same worker opens the BLOB before this method resolves and
    /// then sends chunks over a bounded channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed, the bounded work queue is
    /// full, the metadata load fails, or the BLOB cannot be opened.
    pub(crate) async fn read_blob<F, R>(&self, function: F) -> SqliteResult<(R, BlobReadReceiver)>
    where
        F: FnOnce(&Transaction<'_>) -> SqliteResult<(R, Option<BlobReadRequest>)> + 'static + Send,
        R: Send + 'static,
    {
        self.inner
            .call_blob(&self.inner.blob_reader_sender, function)
            .await
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
        blob_readers: usize,
        blob_read_timeout: Option<Duration>,
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
        let reader_handles = Self::spawn_readers(
            WorkerRole::Reader,
            reader_receiver,
            open_reader.clone(),
            readers,
        )
        .await?;

        let (blob_reader_sender, blob_reader_receiver) = sync_channel::<WorkerCommand>(QUEUE_BOUND);
        let blob_reader_handles = Self::spawn_readers(
            WorkerRole::BlobReader,
            blob_reader_receiver,
            open_reader,
            blob_readers,
        )
        .await?;

        Ok(Self {
            writer_sender,
            reader_sender,
            blob_reader_sender,
            writer_handle: Mutex::new(Some(writer_handle)),
            reader_handles: Mutex::new(reader_handles),
            blob_reader_handles: Mutex::new(blob_reader_handles),
            blob_read_timeout,
            closed: AtomicBool::new(false),
        })
    }

    /// Spawns one read-only worker pool.
    async fn spawn_readers<G>(
        role: WorkerRole,
        receiver: Receiver<WorkerCommand>,
        open_reader: Arc<G>,
        readers: usize,
    ) -> SqliteResult<Vec<JoinHandle<()>>>
    where
        G: Fn() -> rusqlite::Result<rusqlite::Connection> + Send + Sync + 'static,
    {
        let receiver = Arc::new(Mutex::new(receiver));
        let mut handles = Vec::with_capacity(readers);
        for _ in 0..readers {
            let receiver = receiver.clone();
            let open_reader = open_reader.clone();
            let (handle, started) = role.spawn(receiver, move || open_reader());
            Self::wait_for_worker(started).await?;
            handles.push(handle);
        }
        Ok(handles)
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

    /// Runs metadata work and streams a BLOB from the selected worker queue.
    async fn call_blob<F, R>(
        &self,
        queue: &SyncSender<WorkerCommand>,
        function: F,
    ) -> SqliteResult<(R, BlobReadReceiver)>
    where
        F: FnOnce(&Transaction<'_>) -> SqliteResult<(R, Option<BlobReadRequest>)> + 'static + Send,
        R: Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel::<SqliteResult<R>>();
        let (chunk_sender, chunk_receiver) =
            tokio_mpsc::channel::<SqliteResult<Bytes>>(BLOB_STREAM_QUEUE_BOUND);
        let timed_out = Arc::new(AtomicBool::new(false));
        let worker_timed_out = timed_out.clone();
        let blob_read_timeout = self.blob_read_timeout;

        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::DatabaseQueueClosed);
        }

        queue
            .try_send(WorkerCommand::Execute(Box::new(move |conn| {
                Self::execute_blob_read(
                    conn,
                    function,
                    result_sender,
                    &chunk_sender,
                    blob_read_timeout,
                    &worker_timed_out,
                );
            })))
            .map_err(Self::queue_send_error)?;

        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::DatabaseQueueClosed);
        }

        let result = result_receiver
            .await
            .map_err(|_| SqliteError::ThreadJoin)??;
        Ok((
            result,
            BlobReadReceiver {
                receiver: chunk_receiver,
                timed_out,
            },
        ))
    }

    /// Executes BLOB metadata preparation and chunk streaming on a worker thread.
    fn execute_blob_read<F, R>(
        connection: &mut rusqlite::Connection,
        function: F,
        result_sender: oneshot::Sender<SqliteResult<R>>,
        chunk_sender: &tokio_mpsc::Sender<SqliteResult<Bytes>>,
        timeout: Option<Duration>,
        timed_out: &AtomicBool,
    ) where
        F: FnOnce(&Transaction<'_>) -> SqliteResult<(R, Option<BlobReadRequest>)>,
    {
        let transaction = match connection.transaction() {
            Ok(transaction) => transaction,
            Err(error) => {
                _ = result_sender.send(Err(error.into()));
                return;
            }
        };
        let (result, request) = match function(&transaction) {
            Ok(value) => value,
            Err(error) => {
                _ = result_sender.send(Err(error));
                return;
            }
        };
        let Some(request) = request else {
            _ = result_sender.send(Ok(result));
            return;
        };

        let mut blob = match transaction.blob_open(
            MAIN_DB,
            request.table,
            request.column,
            request.rowid,
            true,
        ) {
            Ok(blob) => blob,
            Err(error) => {
                _ = result_sender.send(Err(error.into()));
                return;
            }
        };

        if let Err(error) = Self::validate_blob_span(&blob, request) {
            _ = result_sender.send(Err(error));
            return;
        }

        if request.start > 0
            && let Err(error) = blob.seek(SeekFrom::Start(request.start))
        {
            _ = result_sender.send(Err(error.into()));
            return;
        }

        if result_sender.send(Ok(result)).is_err() {
            return;
        }

        Self::stream_blob_chunks(&mut blob, request.length, chunk_sender, timeout, timed_out);
    }

    /// Validates that the requested byte span fits inside the opened BLOB.
    fn validate_blob_span(
        blob: &rusqlite::blob::Blob<'_>,
        request: BlobReadRequest,
    ) -> SqliteResult<()> {
        let blob_len =
            u64::try_from(blob.len()).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        if blob_len != request.expected_length {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        let end = request
            .start
            .checked_add(request.length)
            .ok_or(SqliteError::ObjectBodyInconsistent)?;
        if end > blob_len {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        Ok(())
    }

    /// Sends BLOB bytes to the async response body until complete or dropped.
    fn stream_blob_chunks(
        blob: &mut rusqlite::blob::Blob<'_>,
        mut remaining: u64,
        chunk_sender: &tokio_mpsc::Sender<SqliteResult<Bytes>>,
        timeout: Option<Duration>,
        timed_out: &AtomicBool,
    ) {
        while remaining > 0 {
            let chunk_len = remaining.min(BLOB_STREAM_CHUNK_SIZE);
            let Ok(chunk_len) = usize::try_from(chunk_len) else {
                _ = chunk_sender.blocking_send(Err(SqliteError::ObjectBodyInconsistent));
                return;
            };
            let Ok(chunk_len_u64) = u64::try_from(chunk_len) else {
                _ = chunk_sender.blocking_send(Err(SqliteError::ObjectBodyInconsistent));
                return;
            };
            let mut chunk = vec![0; chunk_len];
            if let Err(error) = blob.read_exact(&mut chunk) {
                let error = if error.kind() == ErrorKind::UnexpectedEof {
                    SqliteError::ObjectBodyInconsistent
                } else {
                    SqliteError::Io(error)
                };
                _ = Self::send_blob_chunk(chunk_sender, Err(error), timeout, timed_out);
                return;
            }
            remaining -= chunk_len_u64;

            if Self::send_blob_chunk(chunk_sender, Ok(Bytes::from(chunk)), timeout, timed_out)
                .is_err()
            {
                return;
            }
        }
    }

    /// Sends one BLOB chunk, timing out if response backpressure stays full.
    fn send_blob_chunk(
        chunk_sender: &tokio_mpsc::Sender<SqliteResult<Bytes>>,
        mut chunk: SqliteResult<Bytes>,
        timeout: Option<Duration>,
        timed_out: &AtomicBool,
    ) -> Result<(), ()> {
        let started = Instant::now();
        loop {
            match chunk_sender.try_send(chunk) {
                Ok(()) => return Ok(()),
                Err(tokio_mpsc::error::TrySendError::Closed(_)) => return Err(()),
                Err(tokio_mpsc::error::TrySendError::Full(value)) => {
                    chunk = value;
                }
            }

            if let Some(timeout) = timeout {
                let elapsed = started.elapsed();
                if elapsed >= timeout {
                    timed_out.store(true, Ordering::Release);
                    return Err(());
                }
                let remaining = timeout.checked_sub(elapsed).unwrap_or_default();
                thread::sleep(remaining.min(BLOB_STREAM_SEND_RETRY));
            } else {
                thread::sleep(BLOB_STREAM_SEND_RETRY);
            }
        }
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
            let blob_reader_count = self
                .blob_reader_handles
                .lock()
                .map_err(|_| SqliteError::ThreadJoin)?
                .len();
            for _ in 0..reader_count {
                self.reader_sender.send(WorkerCommand::Close).ok();
            }
            for _ in 0..blob_reader_count {
                self.blob_reader_sender.send(WorkerCommand::Close).ok();
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
            let mut blob_reader_handles = self
                .blob_reader_handles
                .lock()
                .map_err(|_| SqliteError::ThreadJoin)?;
            for blob_reader_handle in blob_reader_handles.drain(..) {
                if blob_reader_handle.join().is_err() {
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
    /// Worker role used for read-only BLOB streaming database connections.
    BlobReader,
    /// Worker role used for the single writable database connection.
    Writer,
}

impl WorkerRole {
    /// Returns the role name used in tracing output.
    fn name(self) -> &'static str {
        match self {
            Self::BlobReader => "blob reader",
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

    #[tokio::test]
    async fn blob_reader_timeout_releases_worker() -> SqliteResult<()> {
        let path = std::env::temp_dir().join(format!("s3ite-{}.sqlite3", uuid::Uuid::new_v4()));
        std::fs::remove_file(&path).ok();

        let config = Config {
            readers: 1,
            blob_readers: 1,
            blob_read_timeout_seconds: 1,
            ..Default::default()
        };
        let connection = BucketConnection::open(&path, &config, "bucket").await?;
        connection
            .write(|connection| {
                connection.execute_batch(
                    "
                    CREATE TABLE blob_source (
                        sequence INTEGER PRIMARY KEY,
                        body BLOB NOT NULL
                    ) STRICT;
                    ",
                )?;
                connection.execute(
                    "INSERT INTO blob_source (sequence, body) VALUES (1, ?1);",
                    [vec![7; 1024 * 1024]],
                )?;
                Ok(())
            })
            .await?;

        let ((), stalled_receiver) = connection
            .read_blob(|_| {
                Ok((
                    (),
                    Some(BlobReadRequest::new(
                        "blob_source",
                        "body",
                        1,
                        0,
                        1024 * 1024,
                        1024 * 1024,
                    )),
                ))
            })
            .await?;

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stalled_receiver.timed_out());

        let ((), mut receiver) = connection
            .read_blob(|_| {
                Ok((
                    (),
                    Some(BlobReadRequest::new(
                        "blob_source",
                        "body",
                        1,
                        0,
                        1,
                        1024 * 1024,
                    )),
                ))
            })
            .await?;
        let chunk = receiver.recv().await.ok_or(SqliteError::ThreadJoin)??;
        assert_eq!(chunk.as_ref(), &[7]);

        connection.close().await?;
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(format!("{}-wal", path.to_string_lossy())).ok();
        std::fs::remove_file(format!("{}-shm", path.to_string_lossy())).ok();
        Ok(())
    }
}
