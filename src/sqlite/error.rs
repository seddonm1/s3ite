/// Result type returned by `SQLite` storage operations.
pub type SqliteResult<T> = std::result::Result<T, SqliteError>;

/// Storage-domain errors returned by the `SQLite` layer.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// An `SQLite` connection or query failed.
    #[error("Rusqlite {0}")]
    Rusqlite(#[from] rusqlite::Error),
    /// A filesystem operation failed.
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    /// A database worker queue has reached its bounded capacity.
    #[error("database queue is full")]
    DatabaseQueueFull,
    /// A database worker queue is closed.
    #[error("database queue is closed")]
    DatabaseQueueClosed,
    /// A requested bucket database is not open.
    #[error("bucket does not exist")]
    BucketNotFound,
    /// A requested bucket is configured as read-only.
    #[error("database is in read-only mode")]
    BucketReadOnly,
    /// A requested object does not exist.
    #[error("object does not exist")]
    ObjectNotFound,
    /// A requested object version does not exist.
    #[error("object version does not exist")]
    ObjectVersionNotFound,
    /// A requested object byte range is not satisfiable.
    #[error("object range is invalid")]
    InvalidObjectRange,
    /// Object write preconditions did not match current stored metadata.
    #[error("object precondition failed")]
    PreconditionFailed,
    /// A supplied checksum does not match stored or computed object data.
    #[error("checksum mismatch")]
    ChecksumMismatch,
    /// Storage configuration is invalid.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// A bucket database schema is not supported by this binary.
    #[error("unsupported bucket schema version {found}; expected {expected}")]
    UnsupportedSchemaVersion {
        /// Version found in `PRAGMA user_version`.
        found: i32,
        /// Version expected by this binary.
        expected: i32,
    },
    /// Random bytes could not be generated for an opaque version ID.
    #[error("random version id generation failed: {0}")]
    Random(String),
    /// A database worker thread panicked or could not be joined.
    #[error("database worker thread panicked")]
    ThreadJoin,
    /// A requested multipart upload does not exist.
    #[error("multipart upload does not exist")]
    MultipartUploadNotFound,
    /// Multipart upload credentials do not match the stored upload owner.
    #[error("multipart upload access denied")]
    MultipartAccessDenied,
    /// A requested multipart part is missing or does not match stored data.
    #[error("invalid multipart part")]
    InvalidMultipartPart,
    /// A non-final multipart part is smaller than the minimum part size.
    #[error("multipart part is too small")]
    MultipartPartTooSmall,
    /// A completed multipart object is too large to store.
    #[error("multipart object is too large")]
    MultipartObjectTooLarge,
}

#[derive(Debug, thiserror::Error)]
#[error("object size is negative")]
pub(super) struct NegativeObjectSize;

#[derive(Debug, thiserror::Error)]
#[error("SQL limit is negative")]
pub(super) struct NegativeSqlLimit;
