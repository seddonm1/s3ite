//! Streams S3 request bodies into staged `SQLite` blob chunks.
//!
//! `PutObject` and `UploadPart` bodies are cut into storage chunks as they
//! arrive. Each full chunk is encrypted when the target uses SSE and appended
//! to a staged body in its own short writer transaction. At most one chunk of
//! plaintext (plus its ciphertext while encrypting) is held in memory, and the
//! bucket writer is free between chunks, so a slow client cannot block other
//! writes. The caller claims the staged body in its final write transaction,
//! or discards it when validation fails.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use s3s::stream::aws_chunked_stream::AwsChunkedStreamError;
use uuid::Uuid;

use crate::{
    database::BucketConnection,
    sqlite::{ChunkPosition, STAGED_BODY_MAX_LIFETIME, Sqlite},
};

use super::{
    InternalError, S3Error, S3ErrorCode, S3Result, StreamingBlob, s3_error_from_sqlite,
    sse::{ActiveSse, encode_chunk},
};

/// A live request body, from its first staged chunk through the final claim.
///
/// Dropping it without calling [`StagedBody::claimed`] schedules deletion of
/// the staged chunks, which covers cancelled requests and rejected writes.
pub(super) struct StagedBody {
    connection: Arc<BucketConnection>,
    staging_id: Uuid,
    /// Plaintext byte count of the staged body.
    pub(super) size: i64,
    /// Whether the staged chunks no longer need deleting.
    settled: bool,
}

impl StagedBody {
    /// Returns the staging identifier to claim in the final write transaction.
    pub(super) fn staging_id(&self) -> Uuid {
        self.staging_id
    }

    /// Records that the final write transaction claimed the staged chunks.
    pub(super) fn claimed(mut self) {
        self.settled = true;
    }

    /// Schedules cleanup of rejected writes without waiting on a saturated writer.
    pub(super) fn discard(self) {
        drop(self);
    }
}

impl Drop for StagedBody {
    /// Hands any unclaimed chunks to a cleanup task that outlives the request.
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let connection = self.connection.clone();
        let staging_id = self.staging_id;
        runtime.spawn(async move { Sqlite::discard_staged_body(&connection, staging_id).await });
    }
}

/// Streams `body` into staged chunks of `chunk_size` plaintext bytes.
///
/// The body must be exactly `expected_size` bytes, the length the request
/// declared or the copy source's size. Staging stops at the first byte past
/// it (`InvalidRequest`), and a body that ends short is `IncompleteBody`, so
/// a staged body that is returned always has the declared size.
///
/// `observe_plaintext` sees every plaintext fragment in order, so callers can
/// compute MD5 and checksums without holding the body. Chunks are encrypted
/// with `encryption` when it is set. Any stream, length, or storage error
/// schedules deletion of any chunks staged so far. A stream error that is
/// already an [`S3Error`], such as `NoSuchKey` from a copy source deleted
/// mid-copy, is returned unchanged.
pub(super) async fn stage_streaming_body(
    connection: Arc<BucketConnection>,
    mut body: StreamingBlob,
    chunk_size: usize,
    expected_size: i64,
    part_number: i32,
    encryption: Option<&ActiveSse>,
    mut observe_plaintext: impl FnMut(&[u8]),
) -> S3Result<StagedBody> {
    if chunk_size == 0 {
        return Err(S3Error::with_message(
            InternalError,
            "object blob chunk size must be greater than zero",
        ));
    }

    let mut writer = ChunkWriter {
        staged: StagedBody {
            connection,
            staging_id: Uuid::new_v4(),
            size: 0,
            settled: false,
        },
        encryption,
        part_number,
        part_size: expected_size,
        next_chunk_number: 1,
    };
    let mut buffer = Vec::new();
    let mut received = 0_i64;
    let deadline = tokio::time::Instant::now() + STAGED_BODY_MAX_LIFETIME;

    let result = async {
        loop {
            let Ok(next) = tokio::time::timeout_at(deadline, body.next()).await else {
                return Err(request_timeout());
            };
            let Some(bytes) = next else {
                break;
            };
            let bytes = bytes.map_err(stream_error)?;
            received = i64::try_from(bytes.len())
                .ok()
                .and_then(|len| received.checked_add(len))
                .filter(|received| *received <= expected_size)
                .ok_or_else(|| {
                    S3Error::with_message(
                        S3ErrorCode::InvalidRequest,
                        "request body is longer than its declared length",
                    )
                })?;
            observe_plaintext(bytes.as_ref());
            let mut remaining = bytes.as_ref();
            while !remaining.is_empty() {
                let take = (chunk_size - buffer.len()).min(remaining.len());
                buffer.extend_from_slice(&remaining[..take]);
                remaining = &remaining[take..];
                if buffer.len() == chunk_size {
                    writer.flush(std::mem::take(&mut buffer)).await?;
                }
            }
        }
        if received != expected_size {
            return Err(S3Error::new(S3ErrorCode::IncompleteBody));
        }
        if !buffer.is_empty() {
            writer.flush(buffer).await?;
        }
        Ok(())
    }
    .await;

    match result {
        Ok(()) => Ok(writer.staged),
        Err(err) => {
            writer.staged.discard();
            Err(err)
        }
    }
}

/// Longest a client may leave an upload body without sending more bytes.
///
/// AWS closes idle uploads with `RequestTimeout`; it does not publish the
/// exact limit, and 20 seconds matches its commonly observed behaviour.
const CLIENT_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// Returns the AWS error for a body that stalled or outlived its staging deadline.
fn request_timeout() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::RequestTimeout,
        "Your socket connection to the server was not read from or written to within the timeout period.",
    )
}

/// Fails a client request body with `RequestTimeout` once it sends nothing for
/// [`CLIENT_BODY_IDLE_TIMEOUT`].
///
/// Only bodies read from the client are wrapped. Copy sources are read from
/// local storage and are bounded by the blob read timeout instead.
pub(super) fn with_client_idle_timeout(body: StreamingBlob) -> StreamingBlob {
    StreamingBlob::wrap(futures::stream::unfold(Some(body), |body| async move {
        let mut body = body?;
        match tokio::time::timeout(CLIENT_BODY_IDLE_TIMEOUT, body.next()).await {
            Ok(Some(Ok(bytes))) => Some((Ok(bytes), Some(body))),
            Ok(Some(Err(err))) => Some((Err(stream_error(err)), None)),
            Ok(None) => None,
            Err(_) => Some((Err(request_timeout()), None)),
        }
    }))
}

/// Converts a body stream error into an S3 error, keeping one that already is.
pub(super) fn stream_error(err: s3s::StdError) -> S3Error {
    match err.downcast::<S3Error>() {
        Ok(err) => *err,
        Err(err) => match err.downcast::<AwsChunkedStreamError>() {
            Ok(err) => aws_chunked_stream_error(*err),
            Err(err) => S3Error::with_message(InternalError, err.to_string()),
        },
    }
}

/// Maps AWS chunk decoding failures to a client error when the wire data is at fault.
fn aws_chunked_stream_error(err: AwsChunkedStreamError) -> S3Error {
    use AwsChunkedStreamError as ChunkError;

    let code = match err {
        ChunkError::Underlying(err) => return stream_error(err),
        // The AWS error table defines IncompleteBody for a mismatch between
        // the declared and delivered body length, in either direction here.
        ChunkError::Incomplete | ChunkError::LengthMismatch => S3ErrorCode::IncompleteBody,
        ChunkError::SignatureMismatch => S3ErrorCode::SignatureDoesNotMatch,
        ChunkError::FormatError
        | ChunkError::ChunkMetaTooLarge(..)
        | ChunkError::ChunkDataTooLarge(..)
        | ChunkError::TrailersTooLarge(..)
        | ChunkError::TooManyTrailerHeaders(..) => S3ErrorCode::InvalidRequest,
    };
    S3Error::with_message(code, err.to_string())
}

/// Encodes and appends staged chunks in body order.
struct ChunkWriter<'a> {
    staged: StagedBody,
    encryption: Option<&'a ActiveSse>,
    /// Part the body will be claimed as; staged chunks are encrypted for it.
    part_number: i32,
    /// Declared body size, which every chunk is bound to.
    part_size: i64,
    next_chunk_number: i32,
}

impl ChunkWriter<'_> {
    /// Encodes one plaintext chunk and appends it in its own writer transaction.
    async fn flush(&mut self, plaintext: Vec<u8>) -> S3Result<()> {
        let size = i64::try_from(plaintext.len()).map_err(S3Error::internal_error)?;
        let position = ChunkPosition {
            part_number: self.part_number,
            chunk_number: self.next_chunk_number,
            part_size: self.part_size,
        };
        let chunk = encode_chunk(self.encryption, position, self.staged.size, plaintext)?;
        let staging_id = self.staged.staging_id;
        self.staged
            .connection
            .write(move |transaction| {
                Sqlite::append_staged_chunks(transaction, staging_id, [chunk])
            })
            .await
            .map_err(s3_error_from_sqlite)?;

        self.next_chunk_number = self
            .next_chunk_number
            .checked_add(1)
            .ok_or_else(|| S3Error::with_message(InternalError, "upload chunk count overflow"))?;
        self.staged.size = self
            .staged
            .size
            .checked_add(size)
            .ok_or_else(|| S3Error::with_message(InternalError, "upload body size overflow"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use futures::stream;
    use s3s::{S3Error, S3ErrorCode, S3Result};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{StreamingBlob, stage_streaming_body};
    use crate::{
        Config,
        database::BucketConnection,
        s3::sse::{decrypt_stored_chunks, new_active_sse},
        sqlite::{Sqlite, SqliteResult, StoredBlobChunk},
    };

    /// Opens a writable bucket connection with the current schema in a temporary directory.
    async fn open_bucket() -> SqliteResult<(TempDir, Arc<BucketConnection>)> {
        let directory = TempDir::new()?;
        let connection = BucketConnection::open(
            directory.path().join("bucket.sqlite3"),
            &Config::default(),
            "bucket",
        )
        .await?;
        connection.write(Sqlite::create_tables).await?;
        Ok((directory, Arc::new(connection)))
    }

    /// Wraps static fragments, optionally ending with a stream error, as a request body.
    fn body(fragments: &[&'static [u8]], fail: bool) -> StreamingBlob {
        let mut items = fragments
            .iter()
            .map(|fragment| Ok::<_, S3Error>(Bytes::from_static(fragment)))
            .collect::<Vec<_>>();
        if fail {
            items.push(Err(S3Error::new(S3ErrorCode::IncompleteBody)));
        }
        StreamingBlob::wrap::<_, S3Error>(stream::iter(items))
    }

    /// Loads the chunks a staged body still owns, in body order.
    async fn staged_chunks(
        connection: &BucketConnection,
        staging_id: Option<Uuid>,
    ) -> SqliteResult<Vec<StoredBlobChunk>> {
        connection
            .read(move |transaction| {
                let mut stmt = transaction.prepare(
                    "SELECT offset, size, body, sse_nonce, sse_tag
                    FROM object_blob_chunk
                    WHERE staging_id IS NOT NULL AND (?1 IS NULL OR staging_id = ?1)
                    ORDER BY chunk_number;",
                )?;
                let chunks = stmt
                    .query_map((staging_id,), |row| {
                        Ok(StoredBlobChunk {
                            offset: row.get(0)?,
                            size: row.get(1)?,
                            body: row.get(2)?,
                            sse_nonce: row.get(3)?,
                            sse_tag: row.get(4)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(chunks)
            })
            .await
    }

    /// Checks that a plain body is cut into full chunks plus a remainder, in order.
    #[tokio::test]
    async fn stages_plain_body_in_chunk_order() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let mut observed = Vec::new();
        let staged = stage_streaming_body(
            connection.clone(),
            body(&[b"ab", b"cdef", b"ghi"], false),
            4,
            9,
            1,
            None,
            |bytes| observed.extend_from_slice(bytes),
        )
        .await?;

        assert_eq!(observed, b"abcdefghi");
        assert_eq!(staged.size, 9);
        let chunks = staged_chunks(&connection, Some(staged.staging_id()))
            .await
            .map_err(S3Error::internal_error)?;
        let sizes = chunks.iter().map(|chunk| chunk.size).collect::<Vec<_>>();
        assert_eq!(sizes, [4, 4, 1]);
        assert!(chunks.iter().all(|chunk| chunk.sse_nonce.is_none()));
        staged.discard();
        Ok(())
    }

    /// Checks that staged SSE chunks are ciphertext that decrypts back to the body.
    #[tokio::test]
    async fn stages_encrypted_chunks_that_decrypt() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let encryption = new_active_sse(&[7; 32], "bucket", "key")
            .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, err))?;
        let staged = stage_streaming_body(
            connection.clone(),
            body(&[b"ab", b"cdef", b"ghi"], false),
            3,
            9,
            1,
            Some(&encryption),
            |_| {},
        )
        .await?;

        let chunks = staged_chunks(&connection, Some(staged.staging_id()))
            .await
            .map_err(S3Error::internal_error)?;
        assert_eq!(chunks.len(), 3);
        assert_ne!(chunks[0].body, b"abc");
        let plaintext =
            decrypt_stored_chunks(&encryption, chunks).map_err(S3Error::internal_error)?;
        assert_eq!(plaintext, b"abcdefghi");
        staged.discard();
        Ok(())
    }

    /// Checks that a failed stream and a dropped unclaimed body both leave no staged chunks.
    #[tokio::test]
    async fn failed_or_dropped_bodies_leave_no_staged_chunks() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;

        let failed = stage_streaming_body(
            connection.clone(),
            body(&[b"abcd"], true),
            2,
            4,
            1,
            None,
            |_| {},
        )
        .await;
        assert!(matches!(
            failed.err().as_ref().map(S3Error::code),
            Some(&S3ErrorCode::IncompleteBody)
        ));
        wait_for_cleanup(&connection).await?;

        let dropped = stage_streaming_body(
            connection.clone(),
            body(&[b"abcd"], false),
            2,
            4,
            1,
            None,
            |_| {},
        )
        .await?;
        drop(dropped);
        wait_for_cleanup(&connection).await
    }

    /// Waits for the independent cleanup task to remove everything it owns.
    async fn wait_for_cleanup(connection: &BucketConnection) -> S3Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if staged_chunks(connection, None)
                    .await
                    .map_err(S3Error::internal_error)?
                    .is_empty()
                {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(S3Error::internal_error)?
    }

    /// Asserts staging fails with `code` and leaves no staged chunks behind.
    async fn assert_rejected_without_chunks(
        connection: &Arc<BucketConnection>,
        result: S3Result<super::StagedBody>,
        code: &S3ErrorCode,
    ) -> S3Result<()> {
        assert_eq!(result.err().as_ref().map(S3Error::code), Some(code));
        wait_for_cleanup(connection).await
    }

    /// Checks that overrun and underrun bodies are rejected and their staged chunks removed.
    #[tokio::test]
    async fn rejects_bodies_that_differ_from_the_declared_length() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;

        // Overrun is detected on the fragment that crosses the declared length,
        // after earlier full chunks were already staged.
        let longer = stage_streaming_body(
            connection.clone(),
            body(&[b"abcd", b"ef"], false),
            2,
            5,
            1,
            None,
            |_| {},
        )
        .await;
        assert_rejected_without_chunks(&connection, longer, &S3ErrorCode::InvalidRequest).await?;

        let shorter = stage_streaming_body(
            connection.clone(),
            body(&[b"abc"], false),
            2,
            4,
            1,
            None,
            |_| {},
        )
        .await;
        assert_rejected_without_chunks(&connection, shorter, &S3ErrorCode::IncompleteBody).await
    }

    /// Checks that a stream `S3Error` keeps its code and any other error becomes `InternalError`.
    #[tokio::test]
    async fn keeps_typed_stream_errors_and_wraps_others() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let missing = StreamingBlob::wrap::<_, S3Error>(stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Err(S3Error::new(S3ErrorCode::NoSuchKey)),
        ]));
        let result = stage_streaming_body(connection.clone(), missing, 1, 4, 1, None, |_| {}).await;
        assert_rejected_without_chunks(&connection, result, &S3ErrorCode::NoSuchKey).await?;

        let io = StreamingBlob::wrap::<_, std::io::Error>(stream::iter([Err(
            std::io::Error::other("reset"),
        )]));
        let result = stage_streaming_body(connection.clone(), io, 1, 4, 1, None, |_| {}).await;
        assert_rejected_without_chunks(&connection, result, &S3ErrorCode::InternalError).await
    }

    /// Leaves a request paused after one flush and one partial storage chunk.
    fn paused_body() -> (
        StreamingBlob,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<Result<Bytes, S3Error>>,
    ) {
        use futures::StreamExt as _;
        let (paused, at_pause) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let initial = stream::iter([
            Ok(Bytes::from_static(b"abcd")),
            Ok(Bytes::from_static(b"ef")),
        ]);
        let final_fragment = stream::once(async {
            let _ = paused.send(());
            resumed
                .await
                .unwrap_or_else(|_| Err(S3Error::new(S3ErrorCode::IncompleteBody)))
        });
        (
            StreamingBlob::wrap::<_, S3Error>(initial.chain(final_fragment)),
            at_pause,
            resume,
        )
    }

    /// Saturates the bounded writer queue until the returned sender releases its worker.
    async fn park_writer(
        connection: &Arc<BucketConnection>,
    ) -> S3Result<std::sync::mpsc::Sender<()>> {
        // Park the writer, then fill its queue so the first delete is rejected.
        let (release, parked) = std::sync::mpsc::channel::<()>();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let worker = connection.clone();
        tokio::spawn(async move {
            let _ = worker
                .write(move |_| {
                    let _ = started.send(());
                    let _ = parked.recv();
                    Ok(())
                })
                .await;
        });
        waiting.await.map_err(S3Error::internal_error)?;
        loop {
            let worker = connection.clone();
            let filler = tokio::spawn(async move { worker.write(|_| Ok(())).await });
            tokio::time::sleep(Duration::from_millis(1)).await;
            if filler.is_finished() {
                assert!(matches!(
                    filler.await.map_err(S3Error::internal_error)?,
                    Err(crate::sqlite::SqliteError::DatabaseQueueFull)
                ));
                return Ok(release);
            }
        }
    }

    /// A request can finish or be cancelled while a saturated writer delays cleanup.
    #[tokio::test]
    async fn rejected_body_cleanup_survives_request_cancellation() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let (body, paused, resume) = paused_body();
        let request_connection = connection.clone();
        let (answered, response) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            let result =
                stage_streaming_body(request_connection, body, 4, 7, 1, None, |_| {}).await;
            let _ = answered.send(result.err().map(|err| err.code().clone()));
            std::future::pending::<()>().await;
        });
        paused.await.map_err(S3Error::internal_error)?;
        let release = park_writer(&connection).await?;
        resume
            .send(Err(S3Error::new(S3ErrorCode::IncompleteBody)))
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
        let observed = tokio::time::timeout(Duration::from_millis(250), response).await;
        request.abort();
        assert!(request.await.is_err());
        assert_eq!(
            staged_chunks(&connection, None)
                .await
                .map_err(S3Error::internal_error)?
                .len(),
            1
        );
        release.send(()).map_err(S3Error::internal_error)?;
        wait_for_cleanup(&connection).await?;
        assert_eq!(
            observed
                .map_err(S3Error::internal_error)?
                .map_err(S3Error::internal_error)?,
            Some(S3ErrorCode::IncompleteBody)
        );
        Ok(())
    }

    /// A body still open at the staging deadline is rejected and its chunks removed.
    #[tokio::test]
    async fn staging_stops_at_the_lifetime_deadline() -> S3Result<()> {
        use futures::StreamExt as _;
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let stalled = StreamingBlob::wrap::<_, S3Error>(
            stream::iter([Ok(Bytes::from_static(b"ab"))]).chain(stream::pending()),
        );
        let result = stage_streaming_body(connection.clone(), stalled, 2, 4, 1, None, |_| {}).await;
        let lifetime = crate::sqlite::STAGED_BODY_MAX_LIFETIME;
        assert!((lifetime..lifetime + Duration::from_secs(1)).contains(&started.elapsed()));
        tokio::time::resume();
        assert_rejected_without_chunks(&connection, result, &S3ErrorCode::RequestTimeout).await
    }

    /// A client body may pause just under the idle limit but not reach it.
    #[tokio::test(start_paused = true)]
    async fn client_bodies_time_out_when_idle() {
        use futures::StreamExt as _;
        let idle = super::CLIENT_BODY_IDLE_TIMEOUT;
        let slow = stream::iter([b"a".as_slice(), b"b"]).then(move |fragment| async move {
            tokio::time::sleep(idle.saturating_sub(Duration::from_millis(1))).await;
            Ok::<_, S3Error>(Bytes::from_static(fragment))
        });
        let slow = super::with_client_idle_timeout(StreamingBlob::wrap(slow));
        let fragments = slow.collect::<Vec<_>>().await;
        assert!(fragments.iter().all(Result::is_ok));
        assert_eq!(fragments.len(), 2);

        let started = tokio::time::Instant::now();
        let stalled = StreamingBlob::wrap::<_, S3Error>(
            stream::iter([Ok(Bytes::from_static(b"a"))]).chain(stream::pending()),
        );
        let outcomes = super::with_client_idle_timeout(stalled)
            .map(|item| item.map_err(|err| super::stream_error(err).code().clone()))
            .collect::<Vec<_>>()
            .await;
        assert!((idle..idle + Duration::from_secs(1)).contains(&started.elapsed()));
        assert_eq!(
            outcomes,
            [
                Ok(Bytes::from_static(b"a")),
                Err(S3ErrorCode::RequestTimeout)
            ]
        );
    }

    /// Cleanup on a closed connection returns instead of retrying or panicking.
    #[tokio::test]
    async fn cleanup_gives_up_when_the_writer_is_closed() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        connection.close().await.map_err(S3Error::internal_error)?;
        tokio::time::timeout(
            Duration::from_secs(1),
            Sqlite::discard_staged_body(&connection, Uuid::new_v4()),
        )
        .await
        .map_err(S3Error::internal_error)?;
        Ok(())
    }

    /// Covers decoder syntax, bound, signature, and nested transport errors.
    #[test]
    fn maps_aws_chunked_decoder_errors() {
        use super::{AwsChunkedStreamError as Error, stream_error};
        for (error, expected) in [
            (Error::FormatError, S3ErrorCode::InvalidRequest),
            (Error::ChunkMetaTooLarge(2, 1), S3ErrorCode::InvalidRequest),
            (Error::ChunkDataTooLarge(2, 1), S3ErrorCode::InvalidRequest),
            (Error::TrailersTooLarge(2, 1), S3ErrorCode::InvalidRequest),
            (
                Error::TooManyTrailerHeaders(2, 1),
                S3ErrorCode::InvalidRequest,
            ),
            (Error::SignatureMismatch, S3ErrorCode::SignatureDoesNotMatch),
            (
                Error::Underlying(Box::new(Error::ChunkMetaTooLarge(2, 1))),
                S3ErrorCode::InvalidRequest,
            ),
        ] {
            assert_eq!(stream_error(Box::new(error)).code(), &expected);
        }
    }

    /// Checks that a zero chunk size is rejected instead of looping forever.
    #[tokio::test]
    async fn rejects_zero_chunk_size() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        assert!(
            stage_streaming_body(connection, body(&[b"a"], false), 0, 1, 1, None, |_| {})
                .await
                .is_err()
        );
        Ok(())
    }
}
