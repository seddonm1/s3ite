use std::ops::Not;

use md5::{Digest, Md5};
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::checksum::{ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind};

use super::{
    CompletedMultipartUploadOutcome, CompletedMultipartUploadRecord, CompletedMultipartUploadState,
    CompletedObjectPart, KeyMetadata, KeyValue, Multipart, MultipartChecksumConfig,
    MultipartCompletion, MultipartCompletionChecksum, MultipartCompletionPart,
    MultipartCompletionRequest, MultipartMetadata, MultipartUploadState, MultipartUploadSummary,
    PartChecksum, PreparedMultipartUpload, Sqlite, SqliteError, SqliteResult, decode_base64,
    decode_md5_hex, encode_hex, optional_multipart_checksum_config, optional_part_checksum,
    required_stored_checksum, row_size, sql_limit, sql_size,
};

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

        Self::materialize_multipart_object(
            transaction,
            request.upload_id,
            &request.requested_parts,
            upload_state.checksum,
            &request.requested_checksum,
            request.default_checksum,
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
        Self::validate_multipart_completion_metadata(transaction, request, upload_state.checksum)?;
        Self::check_object_write_conditions(transaction, &request.key, &request.write_conditions)?;
        let write_outcome = Self::put_object(
            transaction,
            KeyValue {
                key: request.key.clone(),
                value: Some(prepared.value),
                key_metadata: KeyMetadata {
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
                    is_latest: true,
                    is_delete_marker: false,
                },
            },
        )?;
        Self::put_completed_object_parts(
            transaction,
            &request.key,
            &write_outcome.version_id,
            &prepared.parts,
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
            version_id: write_outcome.version_id,
            returned_version_id: write_outcome.returned_version_id,
            checksum: prepared.checksum,
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
            version_id: completed.version_id,
            returned_version_id: completed.returned_version_id,
            checksum: completed.checksum,
        })
    }

    /// Validates requested multipart parts and concatenates their bytes.
    fn materialize_multipart_object(
        transaction: &Transaction,
        upload_id: Uuid,
        requested_parts: &[MultipartCompletionPart],
        upload_checksum: Option<MultipartChecksumConfig>,
        requested_checksum: &MultipartCompletionChecksum,
        default_checksum: MultipartChecksumConfig,
    ) -> SqliteResult<PreparedMultipartUpload> {
        let parts = Self::get_multiparts(transaction, upload_id)?;
        if parts.len() != requested_parts.len() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let checksum_config = Self::completed_multipart_checksum_config(
            upload_checksum,
            requested_checksum,
            default_checksum,
        );
        let mut value = Vec::with_capacity(Self::total_multipart_size(&parts)?);
        let mut completed_parts = Vec::with_capacity(parts.len());
        let mut md5_hash = Md5::new();
        let mut full_object_checksum = (checksum_config.checksum_type
            == ChecksumTypeKind::FullObject)
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
                md5: Some(stored_md5.to_string()),
                checksum: part.checksum.clone(),
            });
            offset = offset
                .checked_add(part.size)
                .ok_or(SqliteError::MultipartObjectTooLarge)?;
            if let Some(full_object_checksum) = &mut full_object_checksum {
                full_object_checksum.update(&part.value);
            }
            value.extend_from_slice(&part.value);
        }

        let md5 = format!(
            "{}-{}",
            encode_hex(md5_hash.finalize()),
            requested_parts.len()
        );
        let size = i64::try_from(value.len()).map_err(|_| SqliteError::MultipartObjectTooLarge)?;
        let checksum = Self::completed_multipart_checksum(
            checksum_config,
            requested_checksum,
            full_object_checksum,
            &completed_parts,
        )?;
        Ok(PreparedMultipartUpload {
            value,
            size,
            md5,
            checksum,
            upload_checksum,
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
    fn total_multipart_size(parts: &[Multipart]) -> SqliteResult<usize> {
        parts.iter().try_fold(0usize, |total, part| {
            let part_size =
                usize::try_from(part.size).map_err(|_| SqliteError::InvalidMultipartPart)?;
            total
                .checked_add(part_size)
                .ok_or(SqliteError::MultipartObjectTooLarge)
        })
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
        full_object_checksum: Option<ChecksumAccumulator>,
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<crate::checksum::StoredChecksum> {
        let checksum = match checksum_config.checksum_type {
            ChecksumTypeKind::FullObject => full_object_checksum
                .ok_or(SqliteError::ChecksumMismatch)?
                .finish_stored(ChecksumTypeKind::FullObject)
                .ok_or(SqliteError::ChecksumMismatch)?,
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
                checksum_value
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);",
        )?;

        let now = OffsetDateTime::now_utc();
        Ok(stmt.execute((
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
        ))?)
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
                checksum_value
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
                version_id,
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
                    version_id: row.get(2)?,
                    returned_version_id: row.get(3)?,
                    checksum: required_stored_checksum(row.get(4)?, row.get(5)?, row.get(6)?)?,
                })
            })
            .optional()?)
    }

    /// Inserts or replaces a multipart upload part.
    pub(crate) fn put_multipart(
        transaction: &Transaction,
        multipart: Multipart,
    ) -> SqliteResult<usize> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO multipart_upload_part (
                upload_id,
                last_modified,
                part_number,
                value,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(upload_id, part_number) DO UPDATE
            SET last_modified=excluded.last_modified, value=excluded.value, size=excluded.size, md5=excluded.md5, checksum_algorithm=excluded.checksum_algorithm, checksum_type=excluded.checksum_type, checksum_value=excluded.checksum_value;",
        )?;

        let size = sql_size(multipart.size)?;
        Ok(stmt.execute((
            multipart.upload_id,
            multipart.last_modified,
            multipart.part_number,
            multipart.value,
            size,
            multipart.md5,
            multipart.checksum.as_ref().map(PartChecksum::algorithm),
            Option::<ChecksumTypeKind>::None,
            multipart.checksum.as_ref().map(PartChecksum::as_str),
        ))?)
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
            FROM multipart_upload_part
            WHERE upload_id = ?1
                AND part_number > ?2
            ORDER BY part_number
            LIMIT ?3;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map((upload_id, after_part_number, limit), |row| {
                Ok(MultipartMetadata {
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    size: row_size(row, 2)?,
                    md5: row.get(3)?,
                    checksum: optional_part_checksum(row.get(4)?, row.get(5)?, row.get(6)?)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Lists active multipart uploads ordered by key and upload ID.
    pub(crate) fn list_multipart_uploads(
        transaction: &Transaction,
        prefix: Option<&str>,
        after_key: Option<&str>,
        after_upload_id: Option<Uuid>,
        limit: i64,
    ) -> SqliteResult<Vec<MultipartUploadSummary>> {
        let limit = sql_limit(limit)?;
        let prefix = prefix.and_then(|prefix| prefix.is_empty().not().then_some(prefix));
        let map_upload = |row: &rusqlite::Row<'_>| {
            Ok(MultipartUploadSummary {
                key: row.get(0)?,
                upload_id: row.get(1)?,
                initiated: row.get(2)?,
                checksum: optional_multipart_checksum_config(
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                )?,
            })
        };

        let uploads = match (prefix, after_key, after_upload_id) {
            (Some(prefix), Some(after_key), Some(after_upload_id)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value FROM multipart_upload WHERE key >= ?1 AND (key > ?2 OR (key = ?2 AND upload_id > ?3)) ORDER BY key, upload_id LIMIT ?4;",
                )?;
                stmt.query_map(
                    rusqlite::params![prefix, after_key, after_upload_id, limit],
                    map_upload,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value FROM multipart_upload WHERE key >= ?1 AND key > ?2 ORDER BY key, upload_id LIMIT ?3;",
                )?;
                stmt.query_map(rusqlite::params![prefix, after_key, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value FROM multipart_upload WHERE key >= ?1 ORDER BY key, upload_id LIMIT ?2;",
                )?;
                stmt.query_map(rusqlite::params![prefix, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), Some(after_upload_id)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value FROM multipart_upload WHERE key > ?1 OR (key = ?1 AND upload_id > ?2) ORDER BY key, upload_id LIMIT ?3;",
                )?;
                stmt.query_map(
                    rusqlite::params![after_key, after_upload_id, limit],
                    map_upload,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value FROM multipart_upload WHERE key > ?1 ORDER BY key, upload_id LIMIT ?2;",
                )?;
                stmt.query_map(rusqlite::params![after_key, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type, checksum_value FROM multipart_upload ORDER BY key, upload_id LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(uploads)
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
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            FROM completed_object_part
            WHERE key = ?1 AND version_id = ?2 AND part_number = ?3;",
        )?;

        Ok(stmt
            .query_row((key, version_id, part_number), |row| {
                Ok(CompletedObjectPart {
                    part_number: row.get(0)?,
                    start: row_size(row, 1)?,
                    size: row_size(row, 2)?,
                    md5: row.get(3)?,
                    checksum: optional_part_checksum(row.get(4)?, row.get(5)?, row.get(6)?)?,
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
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            FROM completed_object_part
            WHERE key = ?1
                AND version_id = ?2
                AND part_number > ?3
            ORDER BY part_number
            LIMIT ?4;",
        )?;

        Ok(stmt
            .query_map((key, version_id, after_part_number, limit), |row| {
                Ok(CompletedObjectPart {
                    part_number: row.get(0)?,
                    start: row_size(row, 1)?,
                    size: row_size(row, 2)?,
                    md5: row.get(3)?,
                    checksum: optional_part_checksum(row.get(4)?, row.get(5)?, row.get(6)?)?,
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
            "SELECT COUNT(*) FROM completed_object_part WHERE key = ?1 AND version_id = ?2;",
        )?;
        let count = stmt.query_row((key, version_id), |row| row_size(row, 0))?;
        i32::try_from(count).map_err(|_| SqliteError::MultipartObjectTooLarge)
    }

    /// Inserts completed multipart object part boundaries.
    fn put_completed_object_parts(
        transaction: &Transaction,
        key: &str,
        version_id: &str,
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO completed_object_part (
                key,
                version_id,
                part_number,
                start,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
        )?;

        for part in parts {
            stmt.execute((
                key,
                version_id,
                part.part_number,
                sql_size(part.start)?,
                sql_size(part.size)?,
                &part.md5,
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
                value,
                size,
                md5,
                checksum_algorithm,
                checksum_type,
                checksum_value
            FROM multipart_upload_part
            WHERE upload_id = ?1
            ORDER BY part_number;",
        )?;

        #[allow(clippy::let_and_return)]
        let objects = stmt
            .query_map([upload_id], |row| {
                Ok(Multipart {
                    upload_id,
                    last_modified: row.get(0)?,
                    part_number: row.get(1)?,
                    value: row.get(2)?,
                    size: row_size(row, 3)?,
                    md5: row.get(4)?,
                    checksum: optional_part_checksum(row.get(5)?, row.get(6)?, row.get(7)?)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
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
mod tests {
    use std::collections::HashMap;

    use rusqlite::Connection;

    use crate::{
        checksum::{ChecksumAlgorithmKind, ChecksumTypeKind, FullObjectChecksum},
        sqlite::{
            CompletedMultipartUploadRecord, CompletedObjectPart, MultipartChecksumConfig,
            MultipartCompletion, MultipartCompletionChecksum, MultipartCompletionPart,
            MultipartCompletionRequest, MultipartUploadState, NULL_VERSION_ID,
            ObjectWriteConditions, Sqlite, SqliteError, SqliteResult, StoredChecksum,
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
        assert_eq!(outcome.version_id, NULL_VERSION_ID);
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

        let prefix_after_key =
            Sqlite::list_multipart_uploads(&transaction, Some("alpha"), Some("alpha"), None, 10)?;
        assert_eq!(
            prefix_after_key
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["beta"]
        );

        let after_key_and_upload = Sqlite::list_multipart_uploads(
            &transaction,
            None,
            Some("alpha"),
            Some(alpha_first),
            10,
        )?;
        assert_eq!(
            after_key_and_upload
                .iter()
                .map(|summary| (summary.key.as_str(), summary.upload_id))
                .collect::<Vec<_>>(),
            vec![("alpha", alpha_second), ("beta", beta)]
        );

        let after_key_only =
            Sqlite::list_multipart_uploads(&transaction, None, Some("alpha"), None, 10)?;
        assert_eq!(
            after_key_only
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["beta"]
        );
        Ok(())
    }

    #[test]
    fn completed_part_storage_rejects_negative_boundaries() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;
        assert!(matches!(
            Sqlite::put_completed_object_parts(
                &transaction,
                "key",
                "version",
                &[CompletedObjectPart {
                    part_number: 1,
                    start: -1,
                    size: 1,
                    md5: None,
                    checksum: None,
                }],
            ),
            Err(SqliteError::Rusqlite(
                rusqlite::Error::ToSqlConversionFailure(_)
            ))
        ));
        assert!(matches!(
            Sqlite::put_completed_object_parts(
                &transaction,
                "key",
                "version",
                &[CompletedObjectPart {
                    part_number: 1,
                    start: 0,
                    size: -1,
                    md5: None,
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
