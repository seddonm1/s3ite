use std::ops::Not;

use md5::{Digest, Md5};
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, CompletedMultipartUploadOutcome,
    CompletedMultipartUploadRecord, CompletedMultipartUploadState, CompletedObjectPart,
    KeyMetadata, Multipart, MultipartChecksumConfig, MultipartCompletion,
    MultipartCompletionChecksum, MultipartCompletionPart, MultipartCompletionRequest,
    MultipartMetadata, MultipartPartWrite, MultipartUploadListEntry, MultipartUploadListPage,
    MultipartUploadState, MultipartUploadSummary, PartChecksum, PreparedMultipartUpload, Sqlite,
    SqliteError, SqliteResult, StoredBlobChunk, StoredChecksum, StoredSse, decode_base64,
    decode_md5_hex, encode_hex, optional_multipart_checksum_config, optional_part_checksum,
    optional_stored_object_lock, optional_stored_sse, required_stored_checksum, row_size,
    sql_limit, sql_size,
};

const MULTIPART_UPLOAD_ENTRY_KIND: i64 = 0;
const MULTIPART_COMMON_PREFIX_ENTRY_KIND: i64 = 1;

#[derive(Clone, Copy)]
struct CompletionPartMetadata<'a> {
    part_number: i32,
    size: i64,
    md5: Option<&'a str>,
    checksum: Option<&'a PartChecksum>,
}

impl<'a> From<&'a Multipart> for CompletionPartMetadata<'a> {
    /// Builds completion validation metadata from a stored multipart part.
    fn from(part: &'a Multipart) -> Self {
        Self {
            part_number: part.part_number,
            size: part.size,
            md5: part.md5.as_deref(),
            checksum: part.checksum.as_ref(),
        }
    }
}

impl<'a> From<&'a MultipartMetadata> for CompletionPartMetadata<'a> {
    /// Builds completion validation metadata from listed multipart part metadata.
    fn from(part: &'a MultipartMetadata) -> Self {
        Self {
            part_number: part.part_number,
            size: part.size,
            md5: part.md5.as_deref(),
            checksum: part.checksum.as_ref(),
        }
    }
}

impl MultipartUploadListEntry {
    fn marker(&self) -> (&str, Option<Uuid>) {
        match self {
            Self::Upload(summary) => (&summary.key, Some(summary.upload_id)),
            Self::CommonPrefix { prefix } => (prefix, None),
        }
    }
}

/// Builds a multipart upload list page from one more entry than the client limit.
fn multipart_upload_list_page(
    mut entries: Vec<MultipartUploadListEntry>,
    limit: i64,
) -> MultipartUploadListPage {
    if limit == 0 {
        return MultipartUploadListPage {
            entries: Vec::new(),
            next_key_marker: None,
            next_upload_id_marker: None,
            is_truncated: false,
        };
    }

    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let is_truncated = entries.len() > limit;
    if is_truncated {
        entries.truncate(limit);
    }
    let (next_key_marker, next_upload_id_marker) = if is_truncated {
        entries.last().map_or((None, None), |entry| {
            let (key, upload_id) = entry.marker();
            (
                Some(key.to_string()),
                upload_id.map(|upload_id| upload_id.to_string()),
            )
        })
    } else {
        (None, None)
    };

    MultipartUploadListPage {
        entries,
        next_key_marker,
        next_upload_id_marker,
        is_truncated,
    }
}

fn multipart_upload_entry_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MultipartUploadListEntry> {
    match row.get::<_, i64>(0)? {
        MULTIPART_UPLOAD_ENTRY_KIND => {
            Ok(MultipartUploadListEntry::Upload(MultipartUploadSummary {
                key: row.get(1)?,
                upload_id: row.get(2)?,
                initiated: row.get(3)?,
                checksum: optional_multipart_checksum_config(
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                )?,
            }))
        }
        MULTIPART_COMMON_PREFIX_ENTRY_KIND => Ok(MultipartUploadListEntry::CommonPrefix {
            prefix: row.get(7)?,
        }),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

/// Queries active multipart uploads and delimiter groups in emitted-entry order.
///
/// The CTE applies prefix and marker filters, derives common prefixes, emits
/// uploads outside delimiter groups, and emits one row for each grouped prefix.
fn query_multipart_upload_entries(
    transaction: &Transaction,
    prefix: &str,
    delimiter: &str,
    after_key: Option<&str>,
    after_upload_id: Option<Uuid>,
    fetch_limit: i64,
) -> SqliteResult<Vec<MultipartUploadListEntry>> {
    let mut stmt = transaction.prepare_cached(
        "
        WITH filtered AS (
            SELECT
                key, upload_id, initiated,
                checksum_algorithm, checksum_type, checksum_value,
                CASE
                    WHEN ?2 = '' THEN NULL
                    WHEN instr(substr(key, length(?1) + 1), ?2) = 0 THEN NULL
                    ELSE substr(
                        key, 1,
                        length(?1) + instr(substr(key, length(?1) + 1), ?2) + length(?2) - 1
                    )
                END AS common_prefix
            FROM multipart_upload
            WHERE
                (?1 = '' OR (key >= ?1 AND substr(key, 1, length(?1)) = ?1))
                AND (
                    ?3 IS NULL
                    OR (?4 IS NULL AND key > ?3)
                    OR (?4 IS NOT NULL AND (key > ?3 OR (key = ?3 AND upload_id > ?4)))
                )
        ),
        prefix_rows AS (
            SELECT
                *,
                row_number() OVER (PARTITION BY common_prefix ORDER BY key, upload_id)
                    AS prefix_rank
            FROM filtered
            WHERE common_prefix IS NOT NULL
        )
        SELECT
            entry_kind, key, upload_id, initiated, checksum_algorithm,
            checksum_type, checksum_value, common_prefix
        FROM (
            SELECT
                0 AS entry_kind, key, upload_id, initiated, checksum_algorithm,
                checksum_type, checksum_value, NULL AS common_prefix,
                key AS sort_key, upload_id AS sort_upload_id
            FROM filtered
            WHERE common_prefix IS NULL

            UNION ALL

            SELECT
                1 AS entry_kind, NULL AS key, NULL AS upload_id,
                NULL AS initiated, NULL AS checksum_algorithm, NULL AS checksum_type,
                NULL AS checksum_value, common_prefix,
                common_prefix AS sort_key, upload_id AS sort_upload_id
            FROM prefix_rows
            WHERE prefix_rank = 1 AND (?3 IS NULL OR common_prefix > ?3)
        )
        ORDER BY sort_key, sort_upload_id
        LIMIT ?5;",
    )?;
    let rows = stmt.query_map(
        (prefix, delimiter, after_key, after_upload_id, fetch_limit),
        multipart_upload_entry_from_row,
    )?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

struct MultipartObjectPreparation<'a> {
    upload_id: Uuid,
    requested_parts: &'a [MultipartCompletionPart],
    upload_checksum: Option<MultipartChecksumConfig>,
    upload_stored_sse: Option<StoredSse>,
    requested_checksum: &'a MultipartCompletionChecksum,
    default_checksum: MultipartChecksumConfig,
    precomputed_full_object_checksum: Option<&'a StoredChecksum>,
}

impl Sqlite {
    /// Validates and materializes multipart completion from a read snapshot.
    pub(crate) fn prepare_multipart_upload_completion(
        transaction: &Transaction,
        request: &MultipartCompletionRequest,
    ) -> SqliteResult<MultipartCompletion> {
        if request.requested_parts.is_empty() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let Some(upload_state) = Self::get_multipart_upload_state(
            transaction,
            request.upload_id,
            &request.bucket,
            &request.key,
        )?
        else {
            return Self::completed_multipart_upload_outcome(transaction, request)
                .map(MultipartCompletion::AlreadyCompleted);
        };
        Self::validate_multipart_access(
            upload_state.access_key.as_ref(),
            request.access_key.as_deref(),
        )?;

        Self::prepare_multipart_object(
            transaction,
            MultipartObjectPreparation {
                upload_id: request.upload_id,
                requested_parts: &request.requested_parts,
                upload_checksum: upload_state.checksum,
                upload_stored_sse: upload_state.stored_sse,
                requested_checksum: &request.requested_checksum,
                default_checksum: request.default_checksum,
                precomputed_full_object_checksum: request.precomputed_full_object_checksum.as_ref(),
            },
        )
        .map(MultipartCompletion::Prepared)
    }

    /// Commits a prepared multipart completion after revalidating cheap state.
    pub(crate) fn put_prepared_multipart_upload(
        transaction: &Transaction,
        request: &MultipartCompletionRequest,
        prepared: PreparedMultipartUpload,
    ) -> SqliteResult<CompletedMultipartUploadOutcome> {
        let Some(upload_state) = Self::get_multipart_upload_state(
            transaction,
            request.upload_id,
            &request.bucket,
            &request.key,
        )?
        else {
            return Self::completed_multipart_upload_outcome(transaction, request);
        };
        Self::validate_multipart_access(
            upload_state.access_key.as_ref(),
            request.access_key.as_deref(),
        )?;
        if upload_state.checksum != prepared.upload_checksum {
            return Err(SqliteError::ChecksumMismatch);
        }
        if upload_state.stored_sse != prepared.stored_sse {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        Self::validate_multipart_completion_metadata(transaction, request, upload_state.checksum)?;
        Self::check_object_write_conditions(transaction, &request.key, &request.write_conditions)?;
        let write_outcome = Self::put_object_version(
            transaction,
            &request.key,
            KeyMetadata {
                version_id: String::new(),
                size: prepared.size,
                metadata: upload_state.metadata,
                content_type: upload_state.content_type,
                content_encoding: upload_state.content_encoding,
                cache_control: upload_state.cache_control,
                expires: upload_state.expires,
                last_modified: OffsetDateTime::now_utc(),
                md5: Some(prepared.md5.clone()),
                checksum: Some(prepared.checksum.clone()),
                stored_sse: prepared.stored_sse.clone(),
                object_lock: upload_state.object_lock,
                is_latest: true,
                is_delete_marker: false,
            },
        )?;
        Self::reown_completed_multipart_rows(
            transaction,
            request.upload_id,
            write_outcome.sequence,
            &prepared.parts,
        )?;
        Self::copy_multipart_upload_tags_to_object(
            transaction,
            request.upload_id,
            write_outcome.sequence,
        )?;

        Self::delete_multipart(transaction, request.upload_id)?;
        Self::put_completed_multipart_upload(
            transaction,
            CompletedMultipartUploadRecord {
                upload_id: request.upload_id,
                bucket: &request.bucket,
                key: &request.key,
                access_key: request.access_key.clone(),
                md5: &prepared.md5,
                version_id: &write_outcome.version_id,
                returned_version_id: write_outcome.returned_version_id.as_deref(),
                checksum: &prepared.checksum,
            },
        )?;

        Ok(CompletedMultipartUploadOutcome {
            md5: prepared.md5,
            returned_version_id: write_outcome.returned_version_id,
            checksum: prepared.checksum,
            stored_sse: prepared.stored_sse,
        })
    }

    /// Loads a previously completed upload result for idempotent completion retry.
    fn completed_multipart_upload_outcome(
        transaction: &Transaction,
        request: &MultipartCompletionRequest,
    ) -> SqliteResult<CompletedMultipartUploadOutcome> {
        let completed = Self::get_completed_multipart_upload(
            transaction,
            request.upload_id,
            &request.bucket,
            &request.key,
        )?
        .ok_or(SqliteError::MultipartUploadNotFound)?;
        Self::validate_multipart_access(
            completed.access_key.as_ref(),
            request.access_key.as_deref(),
        )?;
        Ok(CompletedMultipartUploadOutcome {
            md5: completed.md5,
            returned_version_id: completed.returned_version_id,
            checksum: completed.checksum,
            stored_sse: None,
        })
    }

    /// Validates requested multipart parts and prepares final object metadata.
    fn prepare_multipart_object(
        transaction: &Transaction,
        request: MultipartObjectPreparation<'_>,
    ) -> SqliteResult<PreparedMultipartUpload> {
        let MultipartObjectPreparation {
            upload_id,
            requested_parts,
            upload_checksum,
            upload_stored_sse,
            requested_checksum,
            default_checksum,
            precomputed_full_object_checksum,
        } = request;
        let limit = i64::try_from(requested_parts.len())
            .map_err(|_| SqliteError::MultipartObjectTooLarge)?
            .checked_add(1)
            .ok_or(SqliteError::MultipartObjectTooLarge)?;
        let parts = Self::list_multipart(transaction, upload_id, 0, limit)?;
        if parts.len() != requested_parts.len() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let checksum_config = Self::completed_multipart_checksum_config(
            upload_checksum,
            requested_checksum,
            default_checksum,
        );
        let mut completed_parts = Vec::with_capacity(parts.len());
        let mut md5_hash = Md5::new();
        let mut full_object_checksum = (checksum_config.checksum_type
            == ChecksumTypeKind::FullObject
            && upload_stored_sse.is_none())
        .then(|| ChecksumAccumulator::new(checksum_config.algorithm));
        let final_part_number = requested_parts.len();
        let mut offset = 0;
        let algorithm = upload_checksum.map(|checksum| checksum.algorithm);
        let require_requested_part_checksums = requested_checksum.has_value();
        for (index, (requested_part, part)) in requested_parts.iter().zip(parts.iter()).enumerate()
        {
            Self::validate_completion_part(
                index,
                final_part_number,
                requested_part,
                CompletionPartMetadata::from(part),
                algorithm,
                require_requested_part_checksums,
            )?;
            let stored_md5 = part
                .md5
                .as_deref()
                .ok_or(SqliteError::InvalidMultipartPart)?;
            md5_hash.update(decode_md5_hex(stored_md5)?);
            completed_parts.push(CompletedObjectPart {
                part_number: part.part_number,
                start: offset,
                size: part.size,
                checksum: part.checksum.clone(),
            });
            offset = offset
                .checked_add(part.size)
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
            if let Some(full_object_checksum) = &mut full_object_checksum {
                Self::update_full_object_checksum_from_upload_chunks(
                    transaction,
                    upload_id,
                    part.part_number,
                    full_object_checksum,
                )?;
            }
        }

        let md5 = format!(
            "{}-{}",
            encode_hex(md5_hash.finalize()),
            requested_parts.len()
        );
        let size = Self::total_multipart_size(&parts)?;
        let checksum = Self::completed_multipart_checksum(
            checksum_config,
            requested_checksum,
            precomputed_full_object_checksum,
            full_object_checksum,
            &completed_parts,
        )?;
        Ok(PreparedMultipartUpload {
            size,
            md5,
            checksum,
            upload_checksum,
            stored_sse: upload_stored_sse,
            parts: completed_parts,
        })
    }

    /// Revalidates multipart completion metadata inside the writer transaction.
    fn validate_multipart_completion_metadata(
        transaction: &Transaction,
        request: &MultipartCompletionRequest,
        upload_checksum: Option<MultipartChecksumConfig>,
    ) -> SqliteResult<()> {
        let limit = i64::try_from(request.requested_parts.len())
            .map_err(|_| SqliteError::MultipartObjectTooLarge)?
            .checked_add(1)
            .ok_or(SqliteError::MultipartObjectTooLarge)?;
        let parts = Self::list_multipart(transaction, request.upload_id, 0, limit)?;
        if parts.len() != request.requested_parts.len() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let final_part_number = request.requested_parts.len();
        let algorithm = upload_checksum.map(|checksum| checksum.algorithm);
        let require_requested_part_checksums = request.requested_checksum.has_value();
        for (index, (requested_part, part)) in
            request.requested_parts.iter().zip(parts.iter()).enumerate()
        {
            Self::validate_completion_part(
                index,
                final_part_number,
                requested_part,
                CompletionPartMetadata::from(part),
                algorithm,
                require_requested_part_checksums,
            )?;
        }

        Ok(())
    }

    /// Resolves the final checksum algorithm and type for multipart completion.
    fn completed_multipart_checksum_config(
        upload_checksum: Option<MultipartChecksumConfig>,
        requested_checksum: &MultipartCompletionChecksum,
        default_checksum: MultipartChecksumConfig,
    ) -> MultipartChecksumConfig {
        let algorithm = upload_checksum
            .map(|checksum| checksum.algorithm)
            .or(requested_checksum.algorithm())
            .unwrap_or(default_checksum.algorithm);

        let checksum_type = requested_checksum
            .checksum_type()
            .or(upload_checksum.map(|checksum| checksum.checksum_type))
            .unwrap_or(default_checksum.checksum_type);

        MultipartChecksumConfig::new(algorithm, checksum_type)
    }

    /// Returns the total body size for uploaded multipart parts.
    fn total_multipart_size(parts: &[MultipartMetadata]) -> SqliteResult<i64> {
        parts.iter().try_fold(0i64, |total, part| {
            total
                .checked_add(part.size)
                .ok_or(SqliteError::MultipartObjectTooLarge)
        })
    }

    /// Feeds one upload part's chunks into a full-object checksum accumulator.
    fn update_full_object_checksum_from_upload_chunks(
        transaction: &Transaction,
        upload_id: Uuid,
        part_number: i32,
        accumulator: &mut ChecksumAccumulator,
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT body, sse_nonce, sse_tag
            FROM object_blob_chunk
            WHERE upload_id = ?1 AND part_number = ?2
            ORDER BY chunk_number;",
        )?;
        let mut rows = stmt.query((upload_id, part_number))?;
        while let Some(row) = rows.next()? {
            let body = row.get::<_, Vec<u8>>(0)?;
            let sse_nonce = row.get::<_, Option<Vec<u8>>>(1)?;
            let sse_tag = row.get::<_, Option<Vec<u8>>>(2)?;
            if sse_nonce.is_some() || sse_tag.is_some() {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            accumulator.update(&body);
        }
        Ok(())
    }

    /// Reads stored upload-part chunks without decrypting them.
    pub(crate) fn get_upload_part_blob_chunks(
        transaction: &Transaction,
        upload_id: Uuid,
        part_number: i32,
        expected_size: i64,
    ) -> SqliteResult<Vec<StoredBlobChunk>> {
        let expected_size =
            usize::try_from(expected_size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT chunk_number, offset, size, body, sse_nonce, sse_tag
            FROM object_blob_chunk
            WHERE upload_id = ?1 AND part_number = ?2
            ORDER BY chunk_number;",
        )?;
        let mut rows = stmt.query((upload_id, part_number))?;
        let mut chunks = Vec::new();
        let mut expected_chunk_number = 1;
        let mut expected_offset = 0usize;

        while let Some(row) = rows.next()? {
            let chunk_number = row.get::<_, i32>(0)?;
            let offset = usize::try_from(row_size(row, 1)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let size = usize::try_from(row_size(row, 2)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let body = row.get::<_, Vec<u8>>(3)?;
            let sse_nonce = row.get::<_, Option<Vec<u8>>>(4)?;
            let sse_tag = row.get::<_, Option<Vec<u8>>>(5)?;

            if chunk_number != expected_chunk_number || offset != expected_offset {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            if body.len() != size {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            chunks.push(StoredBlobChunk {
                offset: i64::try_from(offset).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
                size: i64::try_from(size).map_err(|_| SqliteError::ObjectBodyInconsistent)?,
                body,
                sse_nonce,
                sse_tag,
            });
            expected_chunk_number = expected_chunk_number
                .checked_add(1)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
            expected_offset = expected_offset
                .checked_add(size)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
        }

        if expected_offset != expected_size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        Ok(chunks)
    }

    /// Validates one requested completion part against the stored part row.
    fn validate_completion_part(
        index: usize,
        final_part_number: usize,
        requested_part: &MultipartCompletionPart,
        part: CompletionPartMetadata<'_>,
        algorithm: Option<ChecksumAlgorithmKind>,
        require_requested_checksum: bool,
    ) -> SqliteResult<()> {
        if part.part_number != requested_part.part_number {
            return Err(SqliteError::InvalidMultipartPart);
        }
        if index + 1 != final_part_number && part.size < 5 * 1024 * 1024 {
            return Err(SqliteError::MultipartPartTooSmall);
        }
        if part.md5 != Some(requested_part.md5.as_str()) {
            return Err(SqliteError::InvalidMultipartPart);
        }
        if let Some(algorithm) = algorithm {
            match requested_part
                .checksum
                .as_ref()
                .and_then(|checksum| checksum.value_for(algorithm))
            {
                Some(requested_checksum)
                    if part
                        .checksum
                        .and_then(|checksum| checksum.value_for(algorithm))
                        == Some(requested_checksum) => {}
                Some(_) => return Err(SqliteError::ChecksumMismatch),
                None if require_requested_checksum => return Err(SqliteError::ChecksumMismatch),
                None => {}
            }
        }
        Ok(())
    }

    /// Computes and validates the final checksum for a completed multipart object.
    fn completed_multipart_checksum(
        checksum_config: MultipartChecksumConfig,
        requested_checksum: &MultipartCompletionChecksum,
        precomputed_full_object_checksum: Option<&StoredChecksum>,
        full_object_checksum: Option<ChecksumAccumulator>,
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<StoredChecksum> {
        let checksum = match checksum_config.checksum_type {
            ChecksumTypeKind::FullObject => {
                if let Some(checksum) = precomputed_full_object_checksum {
                    if checksum.algorithm() != checksum_config.algorithm
                        || checksum.checksum_type() != ChecksumTypeKind::FullObject
                    {
                        return Err(SqliteError::ChecksumMismatch);
                    }
                    checksum.clone()
                } else {
                    full_object_checksum
                        .ok_or(SqliteError::ChecksumMismatch)?
                        .finish_stored(ChecksumTypeKind::FullObject)
                        .ok_or(SqliteError::ChecksumMismatch)?
                }
            }
            ChecksumTypeKind::Composite => {
                let mut accumulator = ChecksumAccumulator::new(checksum_config.algorithm);
                for part in parts {
                    let checksum = part
                        .checksum
                        .as_ref()
                        .and_then(|checksum| checksum.value_for(checksum_config.algorithm))
                        .ok_or(SqliteError::ChecksumMismatch)?;
                    let checksum = decode_base64(checksum).ok_or(SqliteError::ChecksumMismatch)?;
                    accumulator.update(&checksum);
                }
                accumulator
                    .finish_composite(parts.len())
                    .ok_or(SqliteError::ChecksumMismatch)?
            }
        };

        if let Some(requested) = requested_checksum.value_for(checksum_config.algorithm)
            && checksum.value_for(checksum_config.algorithm) != Some(requested)
        {
            return Err(SqliteError::ChecksumMismatch);
        }

        Ok(checksum)
    }
    /// Creates multipart upload state for an upload ID.
    pub(crate) fn create_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
        state: MultipartUploadState,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload (
                upload_id,
                initiated,
                last_modified,
                bucket,
                key,
                access_key,
                metadata,
                content_type,
                content_encoding,
                cache_control,
                expires,
                checksum_algorithm,
                checksum_type,
                checksum_value,
                sse_algorithm,
                sse_key_nonce,
                sse_key_ciphertext,
                sse_key_tag,
                object_lock_legal_hold,
                object_lock_retention_mode,
                object_lock_retain_until
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21);",
        )?;

        let now = OffsetDateTime::now_utc();
        Ok(stmt.execute(rusqlite::params![
            upload_id,
            now,
            now,
            bucket,
            key,
            state.access_key,
            state
                .metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|err| ToSqlConversionFailure(Box::new(err)))?,
            state.content_type,
            state.content_encoding,
            state.cache_control,
            state.expires,
            state.checksum.map(|checksum| checksum.algorithm),
            state.checksum.map(|checksum| checksum.checksum_type),
            Option::<String>::None,
            state
                .stored_sse
                .as_ref()
                .map(|_| crate::s3::SSE_S3_ALGORITHM_CODE),
            state
                .stored_sse
                .as_ref()
                .map(|stored_sse| stored_sse.nonce.as_slice()),
            state
                .stored_sse
                .as_ref()
                .map(|stored_sse| stored_sse.ciphertext.as_slice()),
            state
                .stored_sse
                .as_ref()
                .map(|stored_sse| stored_sse.tag.as_slice()),
            state.object_lock.legal_hold,
            state
                .object_lock
                .retention
                .as_ref()
                .map(|retention| retention.mode.as_str()),
            state
                .object_lock
                .retention
                .as_ref()
                .map(|retention| retention.retain_until),
        ])?)
    }

    /// Fetches multipart upload state for validation and completion.
    pub(crate) fn get_multipart_upload_state(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
    ) -> SqliteResult<Option<MultipartUploadState>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                access_key,
                metadata,
                content_type,
                content_encoding,
                cache_control,
                expires,
                checksum_algorithm,
                checksum_type,
                checksum_value,
                sse_algorithm,
                sse_key_nonce,
                sse_key_ciphertext,
                sse_key_tag,
                object_lock_legal_hold,
                object_lock_retention_mode,
                object_lock_retain_until
            FROM multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        Ok(stmt
            .query_row((upload_id, bucket, key), |row| {
                Ok(MultipartUploadState {
                    access_key: row.get(0)?,
                    metadata: row
                        .get::<_, Option<String>>(1)?
                        .map(|metadata| serde_json::from_str(&metadata))
                        .transpose()
                        .map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                    content_type: row.get(2)?,
                    content_encoding: row.get(3)?,
                    cache_control: row.get(4)?,
                    expires: row.get(5)?,
                    checksum: optional_multipart_checksum_config(
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    )?,
                    stored_sse: optional_stored_sse(
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                    )?,
                    object_lock: optional_stored_object_lock(
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                    )?,
                })
            })
            .optional()?)
    }

    /// Refreshes multipart upload activity after a successful part upload.
    pub(crate) fn touch_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            UPDATE multipart_upload
            SET last_modified = ?2
            WHERE upload_id = ?1;",
        )?;
        Ok(stmt.execute((upload_id, OffsetDateTime::now_utc()))?)
    }

    /// Records a completed multipart upload for idempotent retries.
    pub(crate) fn put_completed_multipart_upload(
        transaction: &Transaction,
        record: CompletedMultipartUploadRecord<'_>,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO completed_multipart_upload (
                upload_id,
                bucket,
                key,
                access_key,
                md5,
                version_id,
                returned_version_id,
                checksum_algorithm,
                checksum_type,
                checksum_value,
                last_modified
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(upload_id) DO UPDATE
            SET bucket=excluded.bucket, key=excluded.key, access_key=excluded.access_key, md5=excluded.md5, version_id=excluded.version_id, returned_version_id=excluded.returned_version_id, checksum_algorithm=excluded.checksum_algorithm, checksum_type=excluded.checksum_type, checksum_value=excluded.checksum_value, last_modified=excluded.last_modified;",
        )?;

        Ok(stmt.execute((
            record.upload_id,
            record.bucket,
            record.key,
            record.access_key,
            record.md5,
            record.version_id,
            record.returned_version_id,
            record.checksum.algorithm(),
            record.checksum.checksum_type(),
            record.checksum.as_str(),
            OffsetDateTime::now_utc(),
        ))?)
    }

    /// Fetches completed multipart upload state by upload ID.
    pub(crate) fn get_completed_multipart_upload(
        transaction: &Transaction,
        upload_id: Uuid,
        bucket: &str,
        key: &str,
    ) -> SqliteResult<Option<CompletedMultipartUploadState>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                access_key,
                md5,
                returned_version_id,
                checksum_algorithm,
                checksum_type,
                checksum_value
            FROM completed_multipart_upload
            WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3;",
        )?;

        Ok(stmt
            .query_row((upload_id, bucket, key), |row| {
                Ok(CompletedMultipartUploadState {
                    access_key: row.get(0)?,
                    md5: row.get(1)?,
                    returned_version_id: row.get(2)?,
                    checksum: required_stored_checksum(row.get(3)?, row.get(4)?, row.get(5)?)?,
                })
            })
            .optional()?)
    }

    /// Inserts or replaces a multipart upload part using the requested chunk size.
    pub(crate) fn put_multipart_with_chunk_size(
        transaction: &Transaction,
        multipart: &Multipart,
        chunk_size: usize,
    ) -> SqliteResult<usize> {
        let value_size = i64::try_from(multipart.value.len())
            .map_err(|_| SqliteError::MultipartObjectTooLarge)?;
        if value_size != multipart.size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        let mut delete_chunks = transaction.prepare_cached(
            "
            DELETE FROM object_blob_chunk
            WHERE upload_id = ?1 AND part_number = ?2;",
        )?;
        delete_chunks.execute((multipart.upload_id, multipart.part_number))?;
        drop(delete_chunks);

        let mut delete_part = transaction.prepare_cached(
            "
            DELETE FROM object_part
            WHERE upload_id = ?1 AND part_number = ?2;",
        )?;
        delete_part.execute((multipart.upload_id, multipart.part_number))?;
        drop(delete_part);

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (NULL, ?1, 1, ?2, ?3, NULL, ?4, ?5, ?6, ?7, ?8);",
        )?;

        let size = sql_size(multipart.size)?;
        let inserted = stmt.execute((
            multipart.upload_id,
            multipart.last_modified,
            multipart.part_number,
            size,
            multipart.md5.as_deref(),
            multipart.checksum.as_ref().map(PartChecksum::algorithm),
            Option::<ChecksumTypeKind>::None,
            multipart.checksum.as_ref().map(PartChecksum::as_str),
        ))?;
        drop(stmt);

        Self::put_object_blob_chunks(
            transaction,
            None,
            Some(multipart.upload_id),
            multipart.part_number,
            &multipart.value,
            0,
            chunk_size,
        )?;

        Ok(inserted)
    }

    /// Inserts or replaces a multipart upload part whose chunks were encoded before the transaction.
    pub(crate) fn put_multipart_with_encoded_chunks<I>(
        transaction: &Transaction,
        part: &MultipartPartWrite,
        chunks: I,
    ) -> SqliteResult<usize>
    where
        I: IntoIterator<Item = super::EncodedBlobChunk>,
    {
        let mut delete_chunks = transaction.prepare_cached(
            "
            DELETE FROM object_blob_chunk
            WHERE upload_id = ?1 AND part_number = ?2;",
        )?;
        delete_chunks.execute((part.upload_id, part.part_number))?;
        drop(delete_chunks);

        let mut delete_part = transaction.prepare_cached(
            "
            DELETE FROM object_part
            WHERE upload_id = ?1 AND part_number = ?2;",
        )?;
        delete_part.execute((part.upload_id, part.part_number))?;
        drop(delete_part);

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (NULL, ?1, 1, ?2, ?3, NULL, ?4, ?5, ?6, ?7, ?8);",
        )?;

        let inserted = stmt.execute((
            part.upload_id,
            part.last_modified,
            part.part_number,
            sql_size(part.size)?,
            part.md5.as_deref(),
            part.checksum.as_ref().map(PartChecksum::algorithm),
            Option::<ChecksumTypeKind>::None,
            part.checksum.as_ref().map(PartChecksum::as_str),
        ))?;
        drop(stmt);

        let written_size = Self::put_encoded_object_blob_chunks(
            transaction,
            None,
            Some(part.upload_id),
            part.part_number,
            0,
            chunks,
        )?;
        if written_size != part.size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }

        Ok(inserted)
    }

    /// Lists multipart upload part metadata ordered by part number.
    pub(crate) fn list_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
        after_part_number: i32,
        limit: i64,
    ) -> SqliteResult<Vec<MultipartMetadata>> {
        let limit = sql_limit(limit)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            FROM object_part
            WHERE upload_id = ?1
                AND part_number > ?2
            ORDER BY part_number
            LIMIT ?3;",
        )?;

        let objects = stmt.query_map((upload_id, after_part_number, limit), |row| {
            Ok(MultipartMetadata {
                last_modified: row.get(0)?,
                part_number: row.get(1)?,
                size: row_size(row, 2)?,
                md5: row.get(3)?,
                checksum: optional_part_checksum(row.get(4)?, row.get(5)?, row.get(6)?)?,
            })
        })?;

        Ok(objects.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Lists active multipart uploads and collapsed common prefixes.
    ///
    /// This is the storage-level `ListMultipartUploads` pager. It returns plain
    /// upload summaries and common-prefix entries; S3 DTO conversion, owner
    /// fields, and URL encoding stay in the S3 layer.
    pub(crate) fn list_multipart_upload_entries(
        transaction: &Transaction,
        prefix: &str,
        delimiter: Option<&str>,
        after_key: Option<&str>,
        after_upload_id: Option<Uuid>,
        limit: i64,
    ) -> SqliteResult<MultipartUploadListPage> {
        let limit = sql_limit(limit)?;
        if limit == 0 {
            return Ok(multipart_upload_list_page(Vec::new(), limit));
        }

        let delimiter = delimiter
            .filter(|delimiter| delimiter.is_empty().not())
            .unwrap_or("");
        let entries = query_multipart_upload_entries(
            transaction,
            prefix,
            delimiter,
            after_key,
            after_upload_id,
            limit.saturating_add(1),
        )?;

        Ok(multipart_upload_list_page(entries, limit))
    }

    /// Fetches one completed multipart object part boundary.
    pub(crate) fn get_completed_object_part(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        part_number: i32,
    ) -> SqliteResult<Option<CompletedObjectPart>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                op.part_number,
                op.start,
                op.size,
                op.checksum_algorithm,
                op.checksum_type,
                op.checksum_value
            FROM object_part AS op
            JOIN object_version AS ov ON ov.sequence = op.object_sequence
            WHERE ov.key = ?1
                AND ov.version_id = ?2
                AND op.is_multipart_part = 1
                AND op.part_number = ?3;",
        )?;

        Ok(stmt
            .query_row((key, version_id, part_number), |row| {
                Ok(CompletedObjectPart {
                    part_number: row.get(0)?,
                    start: row_size(row, 1)?,
                    size: row_size(row, 2)?,
                    checksum: optional_part_checksum(row.get(3)?, row.get(4)?, row.get(5)?)?,
                })
            })
            .optional()?)
    }

    /// Lists completed multipart object part boundaries ordered by part number.
    pub(crate) fn list_completed_object_parts(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        after_part_number: i32,
        limit: i64,
    ) -> SqliteResult<Vec<CompletedObjectPart>> {
        let limit = sql_limit(limit)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                op.part_number,
                op.start,
                op.size,
                op.checksum_algorithm,
                op.checksum_type,
                op.checksum_value
            FROM object_part AS op
            JOIN object_version AS ov ON ov.sequence = op.object_sequence
            WHERE ov.key = ?1
                AND ov.version_id = ?2
                AND op.is_multipart_part = 1
                AND op.part_number > ?3
            ORDER BY op.part_number
            LIMIT ?4;",
        )?;

        Ok(stmt
            .query_map((key, version_id, after_part_number, limit), |row| {
                Ok(CompletedObjectPart {
                    part_number: row.get(0)?,
                    start: row_size(row, 1)?,
                    size: row_size(row, 2)?,
                    checksum: optional_part_checksum(row.get(3)?, row.get(4)?, row.get(5)?)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Counts recorded completed multipart object part boundaries.
    pub(crate) fn count_completed_object_parts(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
    ) -> SqliteResult<i32> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT COUNT(*)
            FROM object_part AS op
            JOIN object_version AS ov ON ov.sequence = op.object_sequence
            WHERE ov.key = ?1 AND ov.version_id = ?2 AND op.is_multipart_part = 1;",
        )?;
        let count = stmt.query_row((key, version_id), |row| row_size(row, 0))?;
        i32::try_from(count).map_err(|_| SqliteError::MultipartObjectTooLarge)
    }

    /// Converts active upload part rows and chunks into completed object-owned rows.
    fn reown_completed_multipart_rows(
        transaction: &Transaction,
        upload_id: Uuid,
        object_sequence: i64,
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<()> {
        let mut update_part = transaction.prepare_cached(
            "
            UPDATE object_part
            SET object_sequence = ?1,
                upload_id = NULL,
                start = ?2
            WHERE upload_id = ?3 AND part_number = ?4;",
        )?;
        let mut update_chunks = transaction.prepare_cached(
            "
            UPDATE object_blob_chunk
            SET object_sequence = ?1,
                upload_id = NULL,
                offset = offset + ?2
            WHERE upload_id = ?3 AND part_number = ?4;",
        )?;

        for part in parts {
            let changed_parts = update_part.execute((
                object_sequence,
                sql_size(part.start)?,
                upload_id,
                part.part_number,
            ))?;
            if changed_parts != 1 {
                return Err(SqliteError::InvalidMultipartPart);
            }
            update_chunks.execute((
                object_sequence,
                sql_size(part.start)?,
                upload_id,
                part.part_number,
            ))?;
        }

        Ok(())
    }

    /// Deletes multipart upload state and cascading parts.
    pub(crate) fn delete_multipart(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            DELETE FROM multipart_upload
            WHERE upload_id = ?1;",
        )?;
        Ok(stmt.execute([upload_id])?)
    }

    /// Validates that stored multipart state belongs to a request access key.
    pub(crate) fn validate_multipart_access(
        stored_access_key: Option<&String>,
        request_access_key: Option<&str>,
    ) -> SqliteResult<()> {
        if stored_access_key.map(String::as_str) == request_access_key {
            Ok(())
        } else {
            Err(SqliteError::MultipartAccessDenied)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use rusqlite::Transaction;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::super::{
        ChecksumTypeKind, CompletedObjectPart, Multipart, PartChecksum, Sqlite, SqliteError,
        SqliteResult, optional_part_checksum, row_size, sql_size,
    };

    /// Inserts or replaces a multipart upload part.
    pub(crate) fn put_multipart(
        transaction: &Transaction,
        multipart: &Multipart,
    ) -> SqliteResult<usize> {
        Sqlite::put_multipart_with_chunk_size(
            transaction,
            multipart,
            super::super::DEFAULT_OBJECT_BLOB_CHUNK_SIZE,
        )
    }

    /// Inserts completed multipart object part boundaries.
    pub(crate) fn put_completed_object_parts(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO object_part (
                object_sequence,
                upload_id,
                is_multipart_part,
                last_modified,
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (
                (
                    SELECT sequence
                    FROM object_version
                    WHERE key = ?1 AND version_id = ?2
                ),
                NULL,
                1,
                ?3,
                ?4,
                ?5,
                ?6,
                ?7,
                ?8,
                ?9,
                ?10
            );",
        )?;

        for part in parts {
            stmt.execute((
                key,
                version_id,
                OffsetDateTime::now_utc(),
                part.part_number,
                sql_size(part.start)?,
                sql_size(part.size)?,
                Option::<String>::None,
                part.checksum.as_ref().map(PartChecksum::algorithm),
                Option::<ChecksumTypeKind>::None,
                part.checksum.as_ref().map(PartChecksum::as_str),
            ))?;
        }

        Ok(())
    }

    /// Fetches multipart upload parts with bodies ordered by part number.
    pub(crate) fn get_multiparts(
        transaction: &Transaction,
        upload_id: Uuid,
    ) -> SqliteResult<Vec<Multipart>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT
                last_modified,
                part_number,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            FROM object_part
            WHERE upload_id = ?1
            ORDER BY part_number;",
        )?;

        let mut rows = stmt.query([upload_id])?;
        let mut parts = Vec::new();
        while let Some(row) = rows.next()? {
            let part_number = row.get(1)?;
            let size = row_size(row, 2)?;
            parts.push(Multipart {
                upload_id,
                last_modified: row.get(0)?,
                part_number,
                value: get_upload_part_bytes(transaction, upload_id, part_number, size)?,
                size,
                md5: row.get(3)?,
                checksum: optional_part_checksum(row.get(4)?, row.get(5)?, row.get(6)?)?,
            });
        }

        Ok(parts)
    }

    /// Reassembles one active upload part from its storage chunks.
    fn get_upload_part_bytes(
        transaction: &Transaction,
        upload_id: Uuid,
        part_number: i32,
        expected_size: i64,
    ) -> SqliteResult<Vec<u8>> {
        let expected_size =
            usize::try_from(expected_size).map_err(|_| SqliteError::ObjectBodyInconsistent)?;
        let mut stmt = transaction.prepare_cached(
            "
            SELECT offset, size, body
            FROM object_blob_chunk
            WHERE upload_id = ?1 AND part_number = ?2
            ORDER BY chunk_number;",
        )?;
        let mut rows = stmt.query((upload_id, part_number))?;
        let mut value = Vec::with_capacity(expected_size);
        let mut expected_offset = 0usize;
        while let Some(row) = rows.next()? {
            let offset = usize::try_from(row_size(row, 0)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let size = usize::try_from(row_size(row, 1)?)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?;
            let body = row.get::<_, Vec<u8>>(2)?;
            if offset != expected_offset || body.len() != size {
                return Err(SqliteError::ObjectBodyInconsistent);
            }
            expected_offset = expected_offset
                .checked_add(size)
                .ok_or(SqliteError::ObjectBodyInconsistent)?;
            value.extend_from_slice(&body);
        }
        if expected_offset != expected_size || value.len() != expected_size {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rusqlite::Connection;

    use crate::{
        s3::{
            ChecksumAlgorithmKind, ChecksumTypeKind, FullObjectChecksum, MultipartChecksumConfig,
            MultipartCompletionChecksum, StoredChecksum, StoredObjectLock,
        },
        sqlite::{
            CompletedMultipartUploadRecord, CompletedObjectPart, EncodedBlobChunk,
            MultipartCompletion, MultipartCompletionPart, MultipartCompletionRequest,
            MultipartPartWrite, MultipartUploadState, NULL_VERSION_ID, ObjectWriteConditions,
            Sqlite, SqliteError, SqliteResult, get::test_support as get_test_support,
        },
    };

    use super::*;

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    fn upload_state() -> MultipartUploadState {
        MultipartUploadState {
            access_key: None,
            metadata: None,
            content_type: None,
            content_encoding: None,
            cache_control: None,
            expires: None,
            checksum: None,
            stored_sse: None,
            object_lock: StoredObjectLock::default(),
        }
    }

    fn completion_request(
        upload_id: Uuid,
        requested_parts: Vec<MultipartCompletionPart>,
    ) -> MultipartCompletionRequest {
        MultipartCompletionRequest {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            upload_id,
            requested_parts,
            requested_checksum: MultipartCompletionChecksum::default(),
            default_checksum: MultipartChecksumConfig::new(
                ChecksumAlgorithmKind::Crc64Nvme,
                ChecksumTypeKind::FullObject,
            ),
            access_key: None,
            write_conditions: ObjectWriteConditions::default(),
            precomputed_full_object_checksum: None,
        }
    }

    fn listed_uploads(page: MultipartUploadListPage) -> Vec<(String, Uuid)> {
        page.entries
            .into_iter()
            .filter_map(|entry| match entry {
                MultipartUploadListEntry::Upload(summary) => Some((summary.key, summary.upload_id)),
                MultipartUploadListEntry::CommonPrefix { .. } => None,
            })
            .collect()
    }

    fn listed_prefixes(page: MultipartUploadListPage) -> Vec<String> {
        page.entries
            .into_iter()
            .filter_map(|entry| match entry {
                MultipartUploadListEntry::Upload(_) => None,
                MultipartUploadListEntry::CommonPrefix { prefix } => Some(prefix),
            })
            .collect()
    }

    fn require_prepared(completion: MultipartCompletion) -> SqliteResult<PreparedMultipartUpload> {
        match completion {
            MultipartCompletion::Prepared(prepared) => Ok(prepared),
            MultipartCompletion::AlreadyCompleted(_) => Err(SqliteError::InvalidMultipartPart),
        }
    }

    #[test]
    fn completion_prepare_rejects_empty_requested_parts() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let request = completion_request(Uuid::new_v4(), Vec::new());

        assert!(matches!(
            Sqlite::prepare_multipart_upload_completion(&transaction, &request),
            Err(SqliteError::InvalidMultipartPart)
        ));
        Ok(())
    }

    #[test]
    fn completion_prepare_returns_existing_completed_upload() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let upload_id = Uuid::new_v4();
        let checksum = StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("crc".to_string()));
        Sqlite::put_completed_multipart_upload(
            &transaction,
            CompletedMultipartUploadRecord {
                upload_id,
                bucket: "bucket",
                key: "key",
                access_key: None,
                md5: "etag-1",
                version_id: NULL_VERSION_ID,
                returned_version_id: Some("returned"),
                checksum: &checksum,
            },
        )?;

        let completion = Sqlite::prepare_multipart_upload_completion(
            &transaction,
            &completion_request(
                upload_id,
                vec![MultipartCompletionPart {
                    part_number: 1,
                    md5: "etag".to_string(),
                    checksum: None,
                }],
            ),
        )?;
        let MultipartCompletion::AlreadyCompleted(outcome) = completion else {
            panic!("expected already-completed multipart outcome");
        };
        assert_eq!(outcome.md5, "etag-1");
        assert_eq!(outcome.returned_version_id.as_deref(), Some("returned"));
        assert_eq!(outcome.checksum, checksum);
        Ok(())
    }

    #[test]
    fn upload_state_rejects_malformed_metadata_json() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let upload_id = Uuid::new_v4();
        let mut metadata = HashMap::new();
        metadata.insert("color".to_string(), "blue".to_string());
        Sqlite::create_multipart_upload(
            &transaction,
            upload_id,
            "bucket",
            "key",
            MultipartUploadState {
                metadata: Some(metadata),
                ..upload_state()
            },
        )?;
        transaction.execute(
            "UPDATE multipart_upload SET metadata = '{not-json' WHERE upload_id = ?1;",
            [upload_id],
        )?;

        assert!(matches!(
            Sqlite::get_multipart_upload_state(&transaction, upload_id, "bucket", "key"),
            Err(SqliteError::Rusqlite(
                rusqlite::Error::FromSqlConversionFailure(..)
            ))
        ));
        Ok(())
    }

    #[test]
    fn put_multipart_with_encoded_chunks_replaces_part_rows() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let upload_id = Uuid::new_v4();
        Sqlite::create_multipart_upload(&transaction, upload_id, "bucket", "key", upload_state())?;
        Sqlite::put_multipart_with_chunk_size(
            &transaction,
            &Multipart {
                upload_id,
                part_number: 1,
                last_modified: time::OffsetDateTime::now_utc(),
                value: b"old".to_vec(),
                size: 3,
                md5: Some("old-md5".to_string()),
                checksum: None,
            },
            2,
        )?;

        let inserted = Sqlite::put_multipart_with_encoded_chunks(
            &transaction,
            &MultipartPartWrite {
                upload_id,
                part_number: 1,
                last_modified: time::OffsetDateTime::now_utc(),
                size: 5,
                md5: Some("new-md5".to_string()),
                checksum: None,
            },
            vec![
                EncodedBlobChunk {
                    chunk_number: 1,
                    offset: 0,
                    size: 2,
                    body: b"ab".to_vec(),
                    sse_nonce: None,
                    sse_tag: None,
                },
                EncodedBlobChunk {
                    chunk_number: 2,
                    offset: 2,
                    size: 3,
                    body: b"cde".to_vec(),
                    sse_nonce: None,
                    sse_tag: None,
                },
            ],
        )?;
        let chunks = {
            let mut stmt = transaction.prepare(
                "
                SELECT offset, size, body
                FROM object_blob_chunk
                WHERE upload_id = ?1 AND part_number = 1
                ORDER BY offset;",
            )?;
            stmt.query_map([upload_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };

        assert_eq!(inserted, 1);
        assert_eq!(
            chunks,
            vec![(0, 2, b"ab".to_vec()), (2, 3, b"cde".to_vec())]
        );
        assert_eq!(
            transaction.query_row(
                "SELECT size, md5 FROM object_part WHERE upload_id = ?1 AND part_number = 1;",
                [upload_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )?,
            (5, "new-md5".to_string())
        );
        Ok(())
    }

    #[test]
    fn completion_put_persists_object_parts_and_retries_from_completed_ledger() -> SqliteResult<()>
    {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let upload_id = Uuid::new_v4();
        let part_md5 = "900150983cd24fb0d6963f7d28e17f72";
        Sqlite::create_multipart_upload(
            &transaction,
            upload_id,
            "bucket",
            "key",
            MultipartUploadState {
                access_key: Some("owner".to_string()),
                ..upload_state()
            },
        )?;
        let multipart = Multipart {
            upload_id,
            part_number: 1,
            last_modified: time::OffsetDateTime::now_utc(),
            value: b"abc".to_vec(),
            size: 3,
            md5: Some(part_md5.to_string()),
            checksum: None,
        };
        Sqlite::put_multipart_with_chunk_size(&transaction, &multipart, 2)?;
        let mut request = completion_request(
            upload_id,
            vec![MultipartCompletionPart {
                part_number: 1,
                md5: part_md5.to_string(),
                checksum: None,
            }],
        );
        request.access_key = Some("owner".to_string());
        let prepared = require_prepared(Sqlite::prepare_multipart_upload_completion(
            &transaction,
            &request,
        )?)?;
        let retry_prepared = require_prepared(Sqlite::prepare_multipart_upload_completion(
            &transaction,
            &request,
        )?)?;

        let outcome = Sqlite::put_prepared_multipart_upload(&transaction, &request, prepared)?;
        assert!(
            Sqlite::get_multipart_upload_state(&transaction, upload_id, "bucket", "key")?.is_none()
        );
        assert!(test_support::get_multiparts(&transaction, upload_id)?.is_empty());
        let object = get_test_support::get_object(&transaction, "key")?
            .ok_or(SqliteError::ObjectNotFound)?;
        assert_eq!(object.value.as_deref(), Some(b"abc".as_slice()));
        let version_id = object.key_metadata.version_id;
        let completed_part =
            Sqlite::get_completed_object_part(&transaction, "key", &version_id, 1)?
                .ok_or(SqliteError::InvalidMultipartPart)?;
        assert_eq!(completed_part.start, 0);
        assert_eq!(completed_part.size, 3);
        let object_chunk_count: i64 = transaction.query_row(
            "
            SELECT COUNT(*)
            FROM object_blob_chunk
            WHERE object_sequence = (
                SELECT sequence
                FROM object_version
                WHERE key = 'key' AND version_id = ?1
            );",
            [&version_id],
            |row| row.get(0),
        )?;
        let upload_chunk_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM object_blob_chunk WHERE upload_id = ?1;",
            [upload_id],
            |row| row.get(0),
        )?;
        assert_eq!(object_chunk_count, 2);
        assert_eq!(upload_chunk_count, 0);

        let retry = Sqlite::put_prepared_multipart_upload(&transaction, &request, retry_prepared)?;
        assert_eq!(retry.md5, outcome.md5);
        assert_eq!(retry.returned_version_id, outcome.returned_version_id);
        assert_eq!(retry.checksum, outcome.checksum);

        request.access_key = Some("other".to_string());
        assert!(matches!(
            Sqlite::prepare_multipart_upload_completion(&transaction, &request),
            Err(SqliteError::MultipartAccessDenied)
        ));
        Ok(())
    }

    #[test]
    fn list_multipart_uploads_honors_marker_combinations() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        let alpha_first = Uuid::from_u128(1);
        let alpha_second = Uuid::from_u128(2);
        let beta = Uuid::from_u128(3);
        for (upload_id, key) in [
            (alpha_first, "alpha"),
            (alpha_second, "alpha"),
            (beta, "beta"),
        ] {
            Sqlite::create_multipart_upload(
                &transaction,
                upload_id,
                "bucket",
                key,
                upload_state(),
            )?;
        }

        let prefix_after_key = Sqlite::list_multipart_upload_entries(
            &transaction,
            "alpha",
            None,
            Some("alpha"),
            None,
            10,
        )?;
        assert_eq!(
            listed_uploads(prefix_after_key),
            Vec::<(String, Uuid)>::new()
        );

        let after_key_and_upload = Sqlite::list_multipart_upload_entries(
            &transaction,
            "",
            None,
            Some("alpha"),
            Some(alpha_first),
            10,
        )?;
        assert_eq!(
            listed_uploads(after_key_and_upload),
            vec![
                ("alpha".to_string(), alpha_second),
                ("beta".to_string(), beta)
            ]
        );

        let after_key_only =
            Sqlite::list_multipart_upload_entries(&transaction, "", None, Some("alpha"), None, 10)?;
        assert_eq!(
            listed_uploads(after_key_only),
            vec![("beta".to_string(), beta)]
        );
        Ok(())
    }

    #[test]
    fn list_multipart_upload_entries_groups_delimiter_prefixes() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;

        for (upload_id, key) in [
            (Uuid::from_u128(4), "group/a/one"),
            (Uuid::from_u128(5), "group/a/two"),
            (Uuid::from_u128(6), "group/b/one"),
            (Uuid::from_u128(7), "group/root"),
        ] {
            Sqlite::create_multipart_upload(
                &transaction,
                upload_id,
                "bucket",
                key,
                upload_state(),
            )?;
        }

        let grouped = Sqlite::list_multipart_upload_entries(
            &transaction,
            "group/",
            Some("/"),
            None,
            None,
            10,
        )?;
        assert_eq!(
            listed_prefixes(grouped),
            vec!["group/a/".to_string(), "group/b/".to_string()]
        );

        let truncated = Sqlite::list_multipart_upload_entries(
            &transaction,
            "group/",
            Some("/"),
            None,
            None,
            1,
        )?;
        assert!(truncated.is_truncated);
        assert_eq!(truncated.next_key_marker.as_deref(), Some("group/a/"));
        assert_eq!(truncated.next_upload_id_marker, None);

        let resumed = Sqlite::list_multipart_upload_entries(
            &transaction,
            "group/",
            Some("/"),
            truncated.next_key_marker.as_deref(),
            None,
            10,
        )?;
        assert_eq!(
            resumed
                .entries
                .into_iter()
                .map(|entry| match entry {
                    MultipartUploadListEntry::Upload(summary) => summary.key,
                    MultipartUploadListEntry::CommonPrefix { prefix } => prefix,
                })
                .collect::<Vec<_>>(),
            vec!["group/b/".to_string(), "group/root".to_string()]
        );
        Ok(())
    }

    #[test]
    fn completed_part_storage_rejects_negative_boundaries() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        assert!(matches!(
            test_support::put_completed_object_parts(
                &transaction,
                "key",
                "version",
                &[CompletedObjectPart {
                    part_number: 1,
                    start: -1,
                    size: 1,
                    checksum: None,
                }],
            ),
            Err(SqliteError::Rusqlite(
                rusqlite::Error::ToSqlConversionFailure(_)
            ))
        ));
        assert!(matches!(
            test_support::put_completed_object_parts(
                &transaction,
                "key",
                "version",
                &[CompletedObjectPart {
                    part_number: 1,
                    start: 0,
                    size: -1,
                    checksum: None,
                }],
            ),
            Err(SqliteError::Rusqlite(
                rusqlite::Error::ToSqlConversionFailure(_)
            ))
        ));
        Ok(())
    }

    #[test]
    fn multipart_access_requires_matching_owner_key() {
        let stored = "owner".to_string();
        assert!(Sqlite::validate_multipart_access(Some(&stored), Some("owner")).is_ok());
        assert!(matches!(
            Sqlite::validate_multipart_access(Some(&stored), Some("other")),
            Err(SqliteError::MultipartAccessDenied)
        ));
        assert!(matches!(
            Sqlite::validate_multipart_access(None, Some("owner")),
            Err(SqliteError::MultipartAccessDenied)
        ));
    }
}
