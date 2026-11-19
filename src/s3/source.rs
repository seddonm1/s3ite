//! Streams a stored object byte range one storage chunk at a time.
//!
//! Encrypted chunks must be decrypted whole, and copies need plaintext to
//! re-encrypt or re-chunk, so these reads cannot use the incremental BLOB
//! stream. Instead each chunk is fetched in its own short read transaction,
//! decrypted when the version is encrypted, and trimmed to the requested
//! range. At most one chunk is held in memory, and no read transaction stays
//! open while the consumer is slow. Object versions are immutable and their
//! sequences are never reused, so separate reads see one consistent body; a
//! version deleted mid-stream ends the stream with `NoSuchKey`.

use std::{collections::VecDeque, ops::Range as ByteRange, sync::Arc};

use bytes::Bytes;
use futures::stream;

use crate::{
    database::{BlobReadReceiver, BucketConnection},
    sqlite::{EncryptedChunkRead, ObjectChunkLocation, Sqlite, SqliteError},
};

use super::{
    S3Error, S3Result, StreamingBlob, s3_error_from_sqlite,
    sse::{ActiveSse, decrypt_chunk},
};

/// Builds a plaintext stream for `range` of the object version `sequence`.
///
/// `locations` must be the chunks overlapping `range`, as listed by
/// `Sqlite::object_chunk_locations`, and `encryption` must be set exactly when
/// the version is encrypted.
pub(super) fn object_range_stream(
    connection: Arc<BucketConnection>,
    sequence: i64,
    locations: Vec<ObjectChunkLocation>,
    range: ByteRange<u64>,
    encryption: Option<Arc<ActiveSse>>,
) -> StreamingBlob {
    let reader = ChunkReader {
        connection,
        sequence,
        locations: locations.into(),
        range,
        encryption,
    };
    StreamingBlob::wrap::<_, S3Error>(stream::unfold(reader, |mut reader| async move {
        let location = reader.locations.pop_front()?;
        match reader.read(location).await {
            Ok(bytes) => Some((Ok(bytes), reader)),
            Err(err) => {
                reader.locations.clear();
                Some((Err(err), reader))
            }
        }
    }))
}

/// Per-stream state for reading planned chunks in order.
struct ChunkReader {
    connection: Arc<BucketConnection>,
    sequence: i64,
    locations: VecDeque<ObjectChunkLocation>,
    range: ByteRange<u64>,
    encryption: Option<Arc<ActiveSse>>,
}

impl ChunkReader {
    /// Fetches, decrypts, and trims one chunk to the requested range.
    async fn read(&self, location: ObjectChunkLocation) -> S3Result<Bytes> {
        let sequence = self.sequence;
        let chunk = self
            .connection
            .read(move |transaction| {
                Sqlite::get_object_blob_chunk_at(transaction, sequence, location)
            })
            .await
            .map_err(s3_error_from_sqlite)?;
        // The lookup already filters on these, so a mismatch means the planned
        // identity and the row disagree; never serve bytes from another chunk.
        if chunk.offset != location.offset || chunk.size != location.size {
            return Err(inconsistent());
        }
        let plaintext = match (&self.encryption, chunk.sse_nonce, chunk.sse_tag) {
            (Some(encryption), Some(nonce), Some(tag)) => {
                decrypt_chunk(encryption, location.position, &nonce, &tag, &chunk.body)
                    .map_err(|_| inconsistent())?
            }
            (None, None, None) => chunk.body,
            _ => return Err(inconsistent()),
        };
        trim_to_range(plaintext, location.offset, location.size, &self.range)
    }
}

/// Builds a plaintext stream for `range` from a snapshot BLOB read of whole encrypted chunks.
///
/// `receiver` must stream the full ciphertext of each entry in `chunks`, in
/// order, from a single reader transaction (see
/// `Sqlite::encrypted_object_read_plan`). Pieces are reassembled per chunk,
/// authenticated and decrypted, then trimmed to `range`, so one chunk is held
/// in memory at a time and the response comes from one consistent snapshot.
pub(super) fn decrypting_snapshot_stream(
    receiver: BlobReadReceiver,
    chunks: Vec<EncryptedChunkRead>,
    range: ByteRange<u64>,
    encryption: Arc<ActiveSse>,
) -> StreamingBlob {
    let state = SnapshotDecrypter {
        receiver,
        chunks: chunks.into(),
        range,
        encryption,
    };
    StreamingBlob::wrap::<_, S3Error>(stream::unfold(state, |mut state| async move {
        let chunk = state.chunks.pop_front()?;
        match state.next(chunk).await {
            Ok(bytes) => Some((Ok(bytes), state)),
            Err(err) => {
                state.chunks.clear();
                Some((Err(err), state))
            }
        }
    }))
}

/// Per-stream state for decrypting snapshot chunks in order.
struct SnapshotDecrypter {
    receiver: BlobReadReceiver,
    chunks: VecDeque<EncryptedChunkRead>,
    range: ByteRange<u64>,
    encryption: Arc<ActiveSse>,
}

impl SnapshotDecrypter {
    /// Collects one chunk's ciphertext from the BLOB stream, decrypts it, and trims it.
    async fn next(&mut self, chunk: EncryptedChunkRead) -> S3Result<Bytes> {
        let size = usize::try_from(chunk.size).map_err(|_| inconsistent())?;
        let mut ciphertext = Vec::with_capacity(size);
        while ciphertext.len() < size {
            match self.receiver.recv().await {
                Some(Ok(piece)) => ciphertext.extend_from_slice(&piece),
                Some(Err(err)) => return Err(s3_error_from_sqlite(err)),
                None if self.receiver.timed_out() => {
                    return Err(s3_error_from_sqlite(SqliteError::ObjectBodyReadTimeout));
                }
                None => return Err(inconsistent()),
            }
        }
        if ciphertext.len() != size {
            return Err(inconsistent());
        }
        let plaintext = decrypt_chunk(
            &self.encryption,
            chunk.position,
            &chunk.sse_nonce,
            &chunk.sse_tag,
            &ciphertext,
        )
        .map_err(|_| inconsistent())?;
        trim_to_range(plaintext, chunk.offset, chunk.size, &self.range)
    }
}

/// Checks a decrypted chunk's length and returns the part that lies inside `range`.
fn trim_to_range(
    plaintext: Vec<u8>,
    offset: i64,
    size: i64,
    range: &ByteRange<u64>,
) -> S3Result<Bytes> {
    if i64::try_from(plaintext.len()).ok() != Some(size) {
        return Err(inconsistent());
    }
    let offset = u64::try_from(offset).map_err(|_| inconsistent())?;
    let size = u64::try_from(size).map_err(|_| inconsistent())?;
    let end = offset.checked_add(size).ok_or_else(inconsistent)?;
    if range.start >= end || range.end <= offset {
        return Err(inconsistent());
    }
    let start = usize::try_from(range.start.max(offset) - offset).map_err(|_| inconsistent())?;
    let stop = usize::try_from(range.end.min(end) - offset).map_err(|_| inconsistent())?;
    Ok(Bytes::from(plaintext).slice(start..stop))
}

/// Maps corrupt or mismatched stored chunks to the storage inconsistency error.
fn inconsistent() -> S3Error {
    s3_error_from_sqlite(SqliteError::ObjectBodyInconsistent)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use s3s::{S3Error, S3ErrorCode, S3Result};
    use tempfile::TempDir;
    use time::OffsetDateTime;

    use super::{decrypting_snapshot_stream, object_range_stream};
    use crate::s3::stage::stream_error;
    use crate::{
        Config,
        database::{BlobReadReceiver, BucketConnection},
        s3::sse::{ActiveSse, encrypt_value_chunks, new_active_sse},
        sqlite::{
            EncryptedChunkRead, FullObjectChecksum, KeyMetadata, KeyValue, Sqlite, SqliteResult,
            StoredChecksum, StoredObjectLock,
        },
    };

    /// Opens a writable bucket connection with the current schema.
    async fn open_bucket() -> SqliteResult<(TempDir, Arc<BucketConnection>)> {
        open_bucket_with(&Config::default()).await
    }

    /// Opens a writable bucket connection with the current schema and `config`.
    async fn open_bucket_with(config: &Config) -> SqliteResult<(TempDir, Arc<BucketConnection>)> {
        let directory = TempDir::new()?;
        let connection =
            BucketConnection::open(directory.path().join("bucket.sqlite3"), config, "bucket")
                .await?;
        connection.write(Sqlite::create_tables).await?;
        Ok((directory, Arc::new(connection)))
    }

    /// Builds unversioned object metadata for a body of `size` bytes.
    fn metadata(size: i64) -> KeyMetadata {
        KeyMetadata {
            version_id: String::new(),
            size,
            metadata: None,
            content_type: None,
            content_encoding: None,
            content_disposition: None,
            content_language: None,
            cache_control: None,
            expires: None,
            last_modified: OffsetDateTime::UNIX_EPOCH,
            md5: Some("md5".to_string()),
            checksum: Some(StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                "crc".to_string(),
            ))),
            stored_sse: None,
            object_lock: StoredObjectLock::default(),
            is_latest: true,
            is_delete_marker: false,
        }
    }

    /// Stores `body` under `key` in 3-byte chunks, encrypted when `encryption` is set.
    async fn store(
        connection: &BucketConnection,
        key: &'static str,
        body: &'static [u8],
        encryption: Option<&ActiveSse>,
    ) -> S3Result<i64> {
        let size = i64::try_from(body.len()).map_err(S3Error::internal_error)?;
        let chunks = encryption
            .map(|encryption| encrypt_value_chunks(body, 3, encryption))
            .transpose()?;
        connection
            .write(move |transaction| {
                let outcome = match chunks {
                    Some(chunks) => Sqlite::put_object_with_encoded_chunks(
                        transaction,
                        key,
                        metadata(size),
                        chunks,
                    )?,
                    None => Sqlite::put_object_with_chunk_size(
                        transaction,
                        KeyValue {
                            key: key.to_string(),
                            value: Some(body.to_vec()),
                            key_metadata: metadata(size),
                        },
                        3,
                    )?,
                };
                Ok(outcome.sequence)
            })
            .await
            .map_err(S3Error::internal_error)
    }

    /// Reads `range` of `sequence` through the per-chunk range reader.
    async fn read_range(
        connection: &Arc<BucketConnection>,
        sequence: i64,
        range: std::ops::Range<u64>,
        encryption: Option<Arc<ActiveSse>>,
    ) -> S3Result<Vec<u8>> {
        let planned = range.clone();
        let locations = connection
            .read(move |transaction| Sqlite::object_chunk_locations(transaction, sequence, planned))
            .await
            .map_err(S3Error::internal_error)?;
        let mut stream =
            object_range_stream(connection.clone(), sequence, locations, range, encryption);
        let mut body = Vec::new();
        while let Some(bytes) = stream.next().await {
            body.extend_from_slice(&bytes.map_err(stream_error)?);
        }
        Ok(body)
    }

    /// Checks that plain and encrypted ranges spanning several chunks stream the exact bytes.
    #[tokio::test]
    async fn streams_plain_and_encrypted_ranges_across_chunks() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let plain = store(&connection, "plain", b"abcdefgh", None).await?;
        assert_eq!(
            read_range(&connection, plain, 0..8, None).await?,
            b"abcdefgh"
        );
        assert_eq!(read_range(&connection, plain, 2..7, None).await?, b"cdefg");

        let encryption = Arc::new(
            new_active_sse(&[3; 32], "bucket", "key")
                .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, err))?,
        );
        let sealed = store(&connection, "sealed", b"abcdefgh", Some(&encryption)).await?;
        assert_eq!(
            read_range(&connection, sealed, 4..8, Some(encryption.clone())).await?,
            b"efgh"
        );
        assert!(read_range(&connection, sealed, 0..8, None).await.is_err());
        Ok(())
    }

    /// Checks that a replaced source version ends the stream with an error rather than serving the replacement.
    #[tokio::test]
    async fn replaced_version_ends_stream_instead_of_serving_new_bytes() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let original = store(&connection, "key", b"oldoldold", None).await?;
        let locations = connection
            .read(move |transaction| Sqlite::object_chunk_locations(transaction, original, 0..9))
            .await
            .map_err(S3Error::internal_error)?;
        let mut stream = object_range_stream(connection.clone(), original, locations, 0..9, None);
        assert_eq!(
            stream
                .next()
                .await
                .transpose()
                .map_err(stream_error)?
                .as_deref(),
            Some(&b"old"[..])
        );

        store(&connection, "key", b"newnewnew", None).await?;
        let next = stream.next().await.transpose().map_err(stream_error);
        assert!(matches!(
            next.as_ref().err().map(S3Error::code),
            Some(&S3ErrorCode::NoSuchKey)
        ));
        assert!(stream.next().await.is_none());
        Ok(())
    }

    /// Test key shared by the snapshot decrypter tests.
    fn snapshot_key() -> S3Result<Arc<ActiveSse>> {
        new_active_sse(&[5; 32], "bucket", "key")
            .map(Arc::new)
            .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, err))
    }

    /// Opens a snapshot BLOB read of `range` of an encrypted object.
    async fn snapshot(
        connection: &BucketConnection,
        sequence: i64,
        range: std::ops::Range<u64>,
    ) -> S3Result<(Vec<EncryptedChunkRead>, BlobReadReceiver)> {
        connection
            .read_blob(move |transaction| {
                Sqlite::encrypted_object_read_plan(transaction, sequence, range)
            })
            .await
            .map_err(S3Error::internal_error)
    }

    /// Drains a decrypted stream, returning its bytes or the first error it ends with.
    async fn drain(mut stream: crate::s3::StreamingBlob) -> S3Result<Vec<u8>> {
        let mut body = Vec::new();
        while let Some(bytes) = stream.next().await {
            body.extend_from_slice(&bytes.map_err(stream_error)?);
        }
        Ok(body)
    }

    /// Checks that a snapshot read decrypts and trims a range spanning several chunks.
    #[tokio::test]
    async fn snapshot_stream_decrypts_ranges_across_chunks() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let encryption = snapshot_key()?;
        let sequence = store(&connection, "key", b"abcdefgh", Some(&encryption)).await?;

        let (chunks, receiver) = snapshot(&connection, sequence, 2..7).await?;
        let stream = decrypting_snapshot_stream(receiver, chunks, 2..7, encryption);
        assert_eq!(drain(stream).await?, b"cdefg");
        Ok(())
    }

    /// Checks that a chunk failing authentication, or ending short, ends the stream with an error.
    #[tokio::test]
    async fn snapshot_stream_rejects_tampered_and_short_chunks() -> S3Result<()> {
        let (_directory, connection) = open_bucket().await.map_err(S3Error::internal_error)?;
        let encryption = snapshot_key()?;
        let sequence = store(&connection, "key", b"abcdefgh", Some(&encryption)).await?;

        let (mut chunks, receiver) = snapshot(&connection, sequence, 0..8).await?;
        chunks[1].sse_tag[0] ^= 1;
        let mut stream = decrypting_snapshot_stream(receiver, chunks, 0..8, encryption.clone());
        assert_eq!(
            stream
                .next()
                .await
                .transpose()
                .map_err(stream_error)?
                .as_deref(),
            Some(&b"abc"[..])
        );
        let tampered = stream.next().await.transpose().map_err(stream_error);
        assert_eq!(
            tampered.err().as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InternalError)
        );
        assert!(stream.next().await.is_none());

        // The plan claims one more ciphertext byte than the snapshot streams.
        let (mut chunks, receiver) = snapshot(&connection, sequence, 0..8).await?;
        if let Some(last) = chunks.last_mut() {
            last.size += 1;
        }
        let short = drain(decrypting_snapshot_stream(
            receiver,
            chunks,
            0..8,
            encryption,
        ))
        .await;
        assert_eq!(
            short.err().as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InternalError)
        );
        Ok(())
    }

    /// Checks that a reader that stalls past the BLOB timeout gets a timeout error, not a short body.
    #[tokio::test]
    async fn snapshot_stream_reports_backpressure_timeout() -> S3Result<()> {
        let config = Config {
            blob_read_timeout_seconds: 1,
            ..Config::default()
        };
        let (_directory, connection) = open_bucket_with(&config)
            .await
            .map_err(S3Error::internal_error)?;
        let encryption = snapshot_key()?;
        // Eight 3-byte chunks overflow the BLOB stream queue while the reader stalls.
        let sequence = store(
            &connection,
            "key",
            b"aaabbbcccdddeeefffggghhh",
            Some(&encryption),
        )
        .await?;

        let (chunks, receiver) = snapshot(&connection, sequence, 0..24).await?;
        let mut stream = decrypting_snapshot_stream(receiver, chunks, 0..24, encryption);
        assert!(stream.next().await.is_some_and(|bytes| bytes.is_ok()));
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        let result = drain(stream).await;
        assert!(result.err().is_some_and(|err| {
            err.message()
                .is_some_and(|message| message.contains("timed out"))
        }));
        Ok(())
    }
}
