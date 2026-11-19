//! Staged upload bodies.
//!
//! Large request bodies are streamed into `object_blob_chunk` rows owned by a
//! `blob_staging` row, one short writer transaction per chunk, so memory stays
//! bounded and the single writer is never held while a client is uploading.
//! The final write transaction claims the staged chunks for an object version
//! or multipart part. Unclaimed staging rows are abandoned uploads: they are
//! deleted when a request fails, at bucket startup, and by maintenance.

use rusqlite::{OptionalExtension, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{EncodedBlobChunk, Sqlite, SqliteError, SqliteResult, sql_size};

/// Destination that receives a staged body when its write transaction commits.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StagedChunkOwner {
    /// Part 1 of a newly inserted object version.
    Object { object_sequence: i64 },
    /// One part of an active multipart upload.
    UploadPart { upload_id: Uuid, part_number: i32 },
}

impl Sqlite {
    /// Appends encoded chunks to a staged body, creating its staging row on first use.
    ///
    /// Every append refreshes `last_appended`, so the idle sweep in
    /// [`Self::delete_abandoned_staged_bodies`] ages orphaned rows from the
    /// last persisted chunk.
    ///
    /// Chunks must continue the staged sequence: the first appended chunk number
    /// is one more than the chunks already staged, and offsets are contiguous
    /// from zero. Any gap or overlap is `ObjectBodyInconsistent`, so a retried or
    /// reordered append can never corrupt the body.
    pub(crate) fn append_staged_chunks<I>(
        transaction: &Transaction,
        staging_id: Uuid,
        chunks: I,
    ) -> SqliteResult<()>
    where
        I: IntoIterator<Item = EncodedBlobChunk>,
    {
        let mut create = transaction.prepare_cached(
            "
            INSERT INTO blob_staging (staging_id, last_appended)
            VALUES (?1, ?2)
            ON CONFLICT (staging_id) DO UPDATE SET last_appended = excluded.last_appended;",
        )?;
        create.execute((staging_id, OffsetDateTime::now_utc()))?;
        drop(create);

        let (mut next_chunk_number, mut next_offset) =
            Self::staged_chunk_totals(transaction, staging_id)?;
        let mut insert = transaction.prepare_cached(
            "
            INSERT INTO object_blob_chunk (
                staging_id,
                part_number,
                chunk_number,
                offset,
                size,
                body,
                sse_nonce,
                sse_tag
            )
            VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7);",
        )?;
        for chunk in chunks {
            next_chunk_number = next_chunk_number
                .checked_add(1)
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
            let body_size = i64::try_from(chunk.body.len())
                .map_err(|_| SqliteError::MultipartObjectTooLarge)?;
            if chunk.chunk_number != next_chunk_number
                || chunk.offset != next_offset
                || chunk.size != body_size
            {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            insert.execute((
                staging_id,
                chunk.chunk_number,
                sql_size(chunk.offset)?,
                sql_size(chunk.size)?,
                chunk.body,
                chunk.sse_nonce,
                chunk.sse_tag,
            ))?;
            next_offset = next_offset
                .checked_add(chunk.size)
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
        }
        Ok(())
    }

    /// Moves every staged chunk to its final owner and deletes the staging row.
    ///
    /// Must run in the same transaction that creates the owner's `object_part`
    /// row, which the deferred chunk foreign keys require at commit. The staged
    /// byte count must equal `expected_size`; a body with no chunks (an empty
    /// object, or a staging row that never existed) is valid only when
    /// `expected_size` is zero.
    pub(crate) fn claim_staged_chunks(
        transaction: &Transaction,
        staging_id: Uuid,
        owner: StagedChunkOwner,
        expected_size: i64,
    ) -> SqliteResult<()> {
        let (_, staged_size) = Self::staged_chunk_totals(transaction, staging_id)?;
        if staged_size != expected_size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        let (object_sequence, upload_id, part_number) = match owner {
            StagedChunkOwner::Object { object_sequence } => (Some(object_sequence), None, 1),
            StagedChunkOwner::UploadPart {
                upload_id,
                part_number,
            } => (None, Some(upload_id), part_number),
        };
        let mut claim = transaction.prepare_cached(
            "
            UPDATE object_blob_chunk
            SET object_sequence = ?2,
                upload_id = ?3,
                part_number = ?4,
                staging_id = NULL
            WHERE staging_id = ?1;",
        )?;
        claim.execute((staging_id, object_sequence, upload_id, part_number))?;
        drop(claim);
        Self::delete_staged_body(transaction, staging_id)
    }

    /// Deletes one staged body and any chunks it still owns.
    pub(crate) fn delete_staged_body(
        transaction: &Transaction,
        staging_id: Uuid,
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM blob_staging
            WHERE staging_id = ?1;",
        )?;
        stmt.execute((staging_id,))?;
        Ok(())
    }

    /// Removes all persisted staging rows before the process accepts requests.
    pub(crate) fn delete_all_staged_bodies(transaction: &Transaction) -> SqliteResult<usize> {
        Ok(transaction.execute("DELETE FROM blob_staging;", ())?)
    }

    /// Deletes staged bodies whose last chunk append is older than `idle_before`.
    ///
    /// Each append refreshes `last_appended`, so an upload that keeps making
    /// progress is never swept however long it runs. Deleting a staging row
    /// cascades to its unclaimed chunks.
    pub(crate) fn delete_abandoned_staged_bodies(
        transaction: &Transaction,
        idle_before: OffsetDateTime,
    ) -> SqliteResult<usize> {
        Ok(transaction.execute(
            "DELETE FROM blob_staging WHERE DATETIME(last_appended) < DATETIME(?1);",
            (idle_before,),
        )?)
    }

    /// Returns the number of staged chunks and their total plaintext size.
    ///
    /// Appends keep chunk numbers and offsets contiguous from the start, so the
    /// last chunk alone gives both: its number is the count and its end offset
    /// is the size. The lookup uses the `(staging_id, chunk_number)` index, so
    /// it stays constant-time however many chunks a body has.
    fn staged_chunk_totals(
        transaction: &Transaction,
        staging_id: Uuid,
    ) -> SqliteResult<(i32, i64)> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT chunk_number, offset + size
            FROM object_blob_chunk
            WHERE staging_id = ?1
            ORDER BY chunk_number DESC
            LIMIT 1;",
        )?;
        Ok(stmt
            .query_row((staging_id,), |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?
            .unwrap_or((0, 0)))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use time::{Duration, OffsetDateTime};
    use uuid::Uuid;

    use super::StagedChunkOwner;
    use crate::sqlite::{
        EncodedBlobChunk, FullObjectChecksum, KeyMetadata, MultipartPartWrite,
        MultipartUploadState, Sqlite, SqliteError, SqliteResult, StoredChecksum, StoredObjectLock,
    };

    /// Opens an in-memory bucket schema with foreign keys enforced.
    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    /// Builds an unencrypted chunk at the given position.
    fn chunk(chunk_number: i32, offset: i64, body: &[u8]) -> EncodedBlobChunk {
        EncodedBlobChunk {
            chunk_number,
            offset,
            size: i64::try_from(body.len()).unwrap_or_default(),
            body: body.to_vec(),
            sse_nonce: None,
            sse_tag: None,
        }
    }

    /// Counts staging rows and chunks still owned by any staged body.
    fn staged_rows(connection: &Connection) -> SqliteResult<(i64, i64)> {
        Ok(connection.query_row(
            "SELECT
                (SELECT COUNT(*) FROM blob_staging),
                (SELECT COUNT(*) FROM object_blob_chunk WHERE staging_id IS NOT NULL);",
            (),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    /// Builds object metadata for a staged body of `size` bytes.
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
            last_modified: OffsetDateTime::now_utc(),
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

    /// Checks that chunks appended over several transactions are claimed as one object body.
    #[test]
    fn staged_chunks_append_in_order_and_become_object_body() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let staging_id = Uuid::new_v4();
        let transaction = connection.transaction()?;
        Sqlite::append_staged_chunks(&transaction, staging_id, [chunk(1, 0, b"abc")])?;
        Sqlite::append_staged_chunks(&transaction, staging_id, [chunk(2, 3, b"de")])?;
        let outcome =
            Sqlite::put_object_with_staged_chunks(&transaction, "key", metadata(5), staging_id)?;
        transaction.commit()?;

        assert_eq!(staged_rows(&connection)?, (0, 0));
        let transaction = connection.transaction()?;
        let body = Sqlite::get_object_blob_chunks(&transaction, outcome.sequence, 5)?
            .into_iter()
            .flat_map(|chunk| chunk.body)
            .collect::<Vec<_>>();
        assert_eq!(body, b"abcde");
        Ok(())
    }

    /// Checks that out-of-sequence appends and wrong claim sizes are rejected.
    #[test]
    fn staged_chunks_reject_gaps_overlaps_and_size_mismatches() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let staging_id = Uuid::new_v4();
        let transaction = connection.transaction()?;
        Sqlite::append_staged_chunks(&transaction, staging_id, [chunk(1, 0, b"abc")])?;
        for bad in [
            chunk(3, 3, b"d"),
            chunk(2, 2, b"d"),
            EncodedBlobChunk {
                size: 9,
                ..chunk(2, 3, b"d")
            },
        ] {
            assert!(matches!(
                Sqlite::append_staged_chunks(&transaction, staging_id, [bad]),
                Err(SqliteError::ObjectBodyInconsistent)
            ));
        }
        assert!(matches!(
            Sqlite::claim_staged_chunks(
                &transaction,
                staging_id,
                StagedChunkOwner::Object { object_sequence: 1 },
                4,
            ),
            Err(SqliteError::ObjectBodyInconsistent)
        ));
        Ok(())
    }

    /// Checks that a staged body can be claimed as a multipart upload part.
    #[test]
    fn staged_chunks_become_multipart_part_body() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let upload_id = Uuid::new_v4();
        let staging_id = Uuid::new_v4();
        let transaction = connection.transaction()?;
        Sqlite::create_multipart_upload(
            &transaction,
            upload_id,
            "bucket",
            "key",
            MultipartUploadState {
                access_key: None,
                metadata: None,
                content_type: None,
                content_encoding: None,
                content_disposition: None,
                content_language: None,
                cache_control: None,
                expires: None,
                checksum: None,
                stored_sse: None,
                object_lock: StoredObjectLock::default(),
            },
        )?;
        Sqlite::append_staged_chunks(&transaction, staging_id, [chunk(1, 0, b"part")])?;
        Sqlite::put_multipart_with_staged_chunks(
            &transaction,
            &MultipartPartWrite {
                upload_id,
                part_number: 7,
                last_modified: OffsetDateTime::now_utc(),
                size: 4,
                md5: Some("md5".to_string()),
                checksum: None,
            },
            staging_id,
        )?;
        transaction.commit()?;

        assert_eq!(staged_rows(&connection)?, (0, 0));
        let owners: (i64, i32) = connection.query_row(
            "SELECT COUNT(*), MAX(part_number) FROM object_blob_chunk WHERE upload_id = ?1;",
            (upload_id,),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(owners, (1, 7));
        Ok(())
    }

    /// Checks per-body deletion, the idle cutoff, and the unconditional startup sweep.
    #[test]
    fn staged_bodies_are_deleted_individually_and_by_age() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let discarded = Uuid::new_v4();
        let live = Uuid::new_v4();
        Sqlite::append_staged_chunks(&transaction, discarded, [chunk(1, 0, b"x")])?;
        Sqlite::append_staged_chunks(&transaction, live, [chunk(1, 0, b"y")])?;
        Sqlite::delete_staged_body(&transaction, discarded)?;
        transaction.commit()?;
        assert_eq!(staged_rows(&connection)?, (1, 1));

        let transaction = connection.transaction()?;
        let an_hour_ago = OffsetDateTime::now_utc() - Duration::hours(1);
        assert_eq!(
            Sqlite::delete_abandoned_staged_bodies(&transaction, an_hour_ago)?,
            0
        );
        assert_eq!(Sqlite::delete_all_staged_bodies(&transaction)?, 1);
        transaction.commit()?;
        assert_eq!(staged_rows(&connection)?, (0, 0));
        Ok(())
    }

    /// A long upload that keeps appending is never treated as idle.
    #[test]
    fn appends_refresh_the_idle_sweep_cutoff() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let staging_id = Uuid::new_v4();
        let two_days_ago = OffsetDateTime::now_utc() - Duration::days(2);
        let a_day_ago = OffsetDateTime::now_utc() - Duration::days(1);

        let transaction = connection.transaction()?;
        Sqlite::append_staged_chunks(&transaction, staging_id, [chunk(1, 0, b"x")])?;
        transaction.execute(
            "UPDATE blob_staging SET last_appended = ?2 WHERE staging_id = ?1;",
            (staging_id, two_days_ago),
        )?;
        Sqlite::append_staged_chunks(&transaction, staging_id, [chunk(2, 1, b"y")])?;
        assert_eq!(
            Sqlite::delete_abandoned_staged_bodies(&transaction, a_day_ago)?,
            0
        );

        transaction.execute(
            "UPDATE blob_staging SET last_appended = ?2 WHERE staging_id = ?1;",
            (staging_id, two_days_ago),
        )?;
        assert_eq!(
            Sqlite::delete_abandoned_staged_bodies(&transaction, a_day_ago)?,
            1
        );
        transaction.commit()?;
        assert_eq!(staged_rows(&connection)?, (0, 0));
        Ok(())
    }

    /// Checks that an empty body claims successfully although no staging row was created.
    #[test]
    fn empty_staged_body_claims_without_staging_row() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        Sqlite::put_object_with_staged_chunks(&transaction, "empty", metadata(0), Uuid::new_v4())?;
        transaction.commit()?;
        assert_eq!(staged_rows(&connection)?, (0, 0));
        Ok(())
    }
}
