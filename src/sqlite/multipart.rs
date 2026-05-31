use std::ops::Not;

use md5::{Digest, Md5};
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT, ChecksumMetadata,
    CompletedMultipartUploadOutcome, CompletedMultipartUploadRecord, CompletedMultipartUploadState,
    CompletedObjectPart, KeyMetadata, KeyValue, Multipart, MultipartCompletionPart,
    MultipartCompletionRequest, MultipartMetadata, MultipartUploadState, MultipartUploadSummary,
    Sqlite, SqliteError, SqliteResult, checksum_value, decode_base64, decode_md5_hex, encode_hex,
    row_checksum, row_size, sql_limit, sql_size,
};

/// Buffered completed multipart object and its persisted part boundaries.
#[derive(Debug)]
struct CompletedMultipartObject {
    value: Vec<u8>,
    size: i64,
    md5: String,
    checksum: ChecksumMetadata,
    parts: Vec<CompletedObjectPart>,
}

impl Sqlite {
    /// Completes a multipart upload and records the completed object.
    pub(crate) async fn try_complete_multipart_upload(
        &self,
        request: MultipartCompletionRequest,
    ) -> SqliteResult<CompletedMultipartUploadOutcome> {
        self.validate_mutable_bucket(&request.bucket)?;

        let connection = self.try_get_connection(&request.bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let outcome =
                    Self::try_complete_multipart_upload_transaction(&transaction, &request)?;
                transaction.commit()?;
                Ok(outcome)
            })
            .await
    }

    /// Validates and materializes multipart completion inside one transaction.
    fn try_complete_multipart_upload_transaction(
        transaction: &Transaction,
        request: &MultipartCompletionRequest,
    ) -> SqliteResult<CompletedMultipartUploadOutcome> {
        if request.requested_parts.is_empty() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let Some(upload_state) = Self::try_get_multipart_upload_state(
            transaction,
            request.upload_id,
            &request.bucket,
            &request.key,
        )?
        else {
            let completed = Self::try_get_completed_multipart_upload(
                transaction,
                request.upload_id,
                &request.bucket,
                &request.key,
            )?
            .ok_or(SqliteError::MultipartUploadNotFound)?;
            Self::try_validate_multipart_access(
                completed.access_key.as_ref(),
                request.access_key.as_deref(),
            )?;
            return Ok(CompletedMultipartUploadOutcome {
                md5: completed.md5,
                version_id: completed.version_id,
                returned_version_id: completed.returned_version_id,
                checksum: completed.checksum,
            });
        };
        Self::try_validate_multipart_access(
            upload_state.access_key.as_ref(),
            request.access_key.as_deref(),
        )?;

        let completed_object = Self::try_materialize_multipart_object(
            transaction,
            request.upload_id,
            &request.requested_parts,
            &upload_state.checksum,
            &request.requested_checksum,
            request.default_checksum.as_ref(),
        )?;

        Self::try_check_object_write_conditions(
            transaction,
            &request.key,
            &request.write_conditions,
        )?;
        let write_outcome = Self::try_put_object(
            transaction,
            KeyValue {
                key: request.key.clone(),
                version_id: String::new(),
                value: Some(completed_object.value),
                key_metadata: KeyMetadata {
                    version_id: String::new(),
                    size: completed_object.size,
                    metadata: upload_state.metadata,
                    content_type: upload_state.content_type,
                    content_encoding: upload_state.content_encoding,
                    cache_control: upload_state.cache_control,
                    expires: upload_state.expires,
                    last_modified: OffsetDateTime::now_utc(),
                    md5: Some(completed_object.md5.clone()),
                    checksum: completed_object.checksum.clone(),
                    is_latest: true,
                    is_delete_marker: false,
                },
            },
        )?;
        Self::try_put_completed_object_parts(
            transaction,
            &request.key,
            &write_outcome.version_id,
            &completed_object.parts,
        )?;

        Self::try_delete_multipart(transaction, request.upload_id)?;
        Self::try_put_completed_multipart_upload(
            transaction,
            CompletedMultipartUploadRecord {
                upload_id: request.upload_id,
                bucket: &request.bucket,
                key: &request.key,
                access_key: request.access_key.clone(),
                md5: &completed_object.md5,
                version_id: &write_outcome.version_id,
                returned_version_id: write_outcome.returned_version_id.as_deref(),
                checksum: &completed_object.checksum,
            },
        )?;

        Ok(CompletedMultipartUploadOutcome {
            md5: completed_object.md5,
            version_id: write_outcome.version_id,
            returned_version_id: write_outcome.returned_version_id,
            checksum: completed_object.checksum,
        })
    }

    /// Validates requested multipart parts and concatenates their bytes.
    fn try_materialize_multipart_object(
        transaction: &Transaction,
        upload_id: Uuid,
        requested_parts: &[MultipartCompletionPart],
        upload_checksum: &ChecksumMetadata,
        requested_checksum: &ChecksumMetadata,
        default_checksum: Option<&ChecksumMetadata>,
    ) -> SqliteResult<CompletedMultipartObject> {
        let parts = Self::try_get_multiparts(transaction, upload_id)?;
        if parts.len() != requested_parts.len() {
            return Err(SqliteError::InvalidMultipartPart);
        }

        let mut value = Vec::with_capacity(Self::try_total_multipart_size(&parts)?);
        let mut completed_parts = Vec::with_capacity(parts.len());
        let mut part_md5s = Vec::with_capacity(parts.len() * 16);
        let final_part_number = requested_parts.len();
        let mut offset = 0;
        let algorithm = upload_checksum.algorithm.as_deref();
        let require_requested_part_checksums = requested_checksum.has_value();
        for (index, (requested_part, part)) in requested_parts.iter().zip(parts.iter()).enumerate()
        {
            Self::try_validate_completion_part(
                index,
                final_part_number,
                requested_part,
                part,
                algorithm,
                require_requested_part_checksums,
            )?;
            let stored_md5 = part
                .md5
                .as_deref()
                .ok_or(SqliteError::InvalidMultipartPart)?;
            part_md5s.extend_from_slice(&decode_md5_hex(stored_md5)?);
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
            value.extend_from_slice(&part.value);
        }

        let mut md5_hash = Md5::new();
        md5_hash.update(&part_md5s);
        let md5 = format!(
            "{}-{}",
            encode_hex(md5_hash.finalize()),
            requested_parts.len()
        );
        let size = i64::try_from(value.len()).map_err(|_| SqliteError::MultipartObjectTooLarge)?;
        let checksum = Self::try_completed_multipart_checksum(
            upload_checksum,
            requested_checksum,
            default_checksum,
            &value,
            &completed_parts,
        )?;
        Ok(CompletedMultipartObject {
            value,
            size,
            md5,
            checksum,
            parts: completed_parts,
        })
    }

    /// Returns the total body size for uploaded multipart parts.
    fn try_total_multipart_size(parts: &[Multipart]) -> SqliteResult<usize> {
        parts.iter().try_fold(0usize, |total, part| {
            let part_size =
                usize::try_from(part.size).map_err(|_| SqliteError::InvalidMultipartPart)?;
            total
                .checked_add(part_size)
                .ok_or(SqliteError::MultipartObjectTooLarge)
        })
    }

    /// Validates one requested completion part against the stored part row.
    fn try_validate_completion_part(
        index: usize,
        final_part_number: usize,
        requested_part: &MultipartCompletionPart,
        part: &Multipart,
        algorithm: Option<&str>,
        require_requested_checksum: bool,
    ) -> SqliteResult<()> {
        if part.part_number != requested_part.part_number {
            return Err(SqliteError::InvalidMultipartPart);
        }
        if index + 1 != final_part_number && part.size < 5 * 1024 * 1024 {
            return Err(SqliteError::MultipartPartTooSmall);
        }
        if part.md5.as_deref() != Some(requested_part.md5.as_str()) {
            return Err(SqliteError::InvalidMultipartPart);
        }
        if let Some(algorithm) = algorithm {
            match requested_part.checksum.value_for(algorithm) {
                Some(requested_checksum)
                    if part.checksum.value_for(algorithm) == Some(requested_checksum) => {}
                Some(_) => return Err(SqliteError::ChecksumMismatch),
                None if require_requested_checksum => return Err(SqliteError::ChecksumMismatch),
                None => {}
            }
        }
        Ok(())
    }

    /// Computes and validates the final checksum for a completed multipart object.
    fn try_completed_multipart_checksum(
        upload_checksum: &ChecksumMetadata,
        requested_checksum: &ChecksumMetadata,
        default_checksum: Option<&ChecksumMetadata>,
        value: &[u8],
        parts: &[CompletedObjectPart],
    ) -> SqliteResult<ChecksumMetadata> {
        let empty_default_checksum = ChecksumMetadata::default();
        let default_checksum = default_checksum.unwrap_or(&empty_default_checksum);
        let algorithm = upload_checksum
            .algorithm
            .as_deref()
            .or(requested_checksum.algorithm.as_deref())
            .or(default_checksum.algorithm.as_deref());
        let Some(algorithm) = algorithm else {
            if requested_checksum.has_value() {
                return Err(SqliteError::ChecksumMismatch);
            }
            return Ok(ChecksumMetadata::default());
        };

        let checksum_type =
            if upload_checksum.algorithm.is_some() || requested_checksum.algorithm.is_some() {
                requested_checksum
                    .checksum_type
                    .as_deref()
                    .or(upload_checksum.checksum_type.as_deref())
                    .or(default_checksum.checksum_type.as_deref())
                    .unwrap_or(CHECKSUM_TYPE_COMPOSITE)
            } else {
                default_checksum
                    .checksum_type
                    .as_deref()
                    .or(requested_checksum.checksum_type.as_deref())
                    .unwrap_or(CHECKSUM_TYPE_COMPOSITE)
            };
        let computed = match checksum_type {
            CHECKSUM_TYPE_FULL_OBJECT => checksum_value(algorithm, value)?,
            CHECKSUM_TYPE_COMPOSITE => {
                let mut combined = Vec::new();
                for part in parts {
                    let checksum = part
                        .checksum
                        .value_for(algorithm)
                        .ok_or(SqliteError::ChecksumMismatch)?;
                    let checksum = decode_base64(checksum).ok_or(SqliteError::ChecksumMismatch)?;
                    combined.extend_from_slice(&checksum);
                }
                format!("{}-{}", checksum_value(algorithm, &combined)?, parts.len())
            }
            _ => return Err(SqliteError::ChecksumMismatch),
        };

        let mut checksum = ChecksumMetadata {
            algorithm: Some(algorithm.to_string()),
            checksum_type: Some(checksum_type.to_string()),
            ..ChecksumMetadata::default()
        };
        checksum.set_value(algorithm, computed)?;

        if let Some(requested) = requested_checksum.value_for(algorithm)
            && checksum.value_for(algorithm) != Some(requested)
        {
            return Err(SqliteError::ChecksumMismatch);
        }

        Ok(checksum)
    }
    /// Creates multipart upload state for an upload ID.
    pub(crate) fn try_create_multipart_upload(
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
                checksum_type
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13);",
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
            state.checksum.algorithm.as_deref(),
            state.checksum.checksum_type.as_deref(),
        ))?)
    }

    /// Fetches multipart upload state for validation and completion.
    pub(crate) fn try_get_multipart_upload_state(
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
                checksum_type
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
                    checksum: ChecksumMetadata {
                        algorithm: row.get(6)?,
                        checksum_type: row.get(7)?,
                        ..ChecksumMetadata::default()
                    },
                })
            })
            .optional()?)
    }

    /// Refreshes multipart upload activity after a successful part upload.
    pub(crate) fn try_touch_multipart_upload(
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
    pub(crate) fn try_put_completed_multipart_upload(
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
                checksum_type,
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256,
                last_modified
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ON CONFLICT(upload_id) DO UPDATE
            SET bucket=excluded.bucket, key=excluded.key, access_key=excluded.access_key, md5=excluded.md5, version_id=excluded.version_id, returned_version_id=excluded.returned_version_id, checksum_type=excluded.checksum_type, checksum_crc32=excluded.checksum_crc32, checksum_crc32c=excluded.checksum_crc32c, checksum_crc64nvme=excluded.checksum_crc64nvme, checksum_sha1=excluded.checksum_sha1, checksum_sha256=excluded.checksum_sha256, last_modified=excluded.last_modified;",
        )?;

        Ok(stmt.execute((
            record.upload_id,
            record.bucket,
            record.key,
            record.access_key,
            record.md5,
            record.version_id,
            record.returned_version_id,
            record.checksum.checksum_type.as_deref(),
            record.checksum.crc32.as_deref(),
            record.checksum.crc32c.as_deref(),
            record.checksum.crc64nvme.as_deref(),
            record.checksum.sha1.as_deref(),
            record.checksum.sha256.as_deref(),
            OffsetDateTime::now_utc(),
        ))?)
    }

    /// Fetches completed multipart upload state by upload ID.
    pub(crate) fn try_get_completed_multipart_upload(
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
                checksum_type,
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
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
                    checksum: row_checksum(row, None, Some(4), 5)?,
                })
            })
            .optional()?)
    }

    /// Inserts or replaces a multipart upload part.
    pub(crate) fn try_put_multipart(
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
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(upload_id, part_number) DO UPDATE
            SET last_modified=excluded.last_modified, value=excluded.value, size=excluded.size, md5=excluded.md5, checksum_crc32=excluded.checksum_crc32, checksum_crc32c=excluded.checksum_crc32c, checksum_crc64nvme=excluded.checksum_crc64nvme, checksum_sha1=excluded.checksum_sha1, checksum_sha256=excluded.checksum_sha256;",
        )?;

        let size = sql_size(multipart.size)?;
        Ok(stmt.execute((
            multipart.upload_id,
            multipart.last_modified,
            multipart.part_number,
            multipart.value,
            size,
            multipart.md5,
            multipart.checksum.crc32.as_deref(),
            multipart.checksum.crc32c.as_deref(),
            multipart.checksum.crc64nvme.as_deref(),
            multipart.checksum.sha1.as_deref(),
            multipart.checksum.sha256.as_deref(),
        ))?)
    }

    /// Lists multipart upload part metadata ordered by part number.
    pub(crate) fn try_list_multipart(
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
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
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
                    checksum: row_checksum(row, None, None, 4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Lists active multipart uploads ordered by key and upload ID.
    pub(crate) fn try_list_multipart_uploads(
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
                checksum: ChecksumMetadata {
                    algorithm: row.get(3)?,
                    checksum_type: row.get(4)?,
                    ..ChecksumMetadata::default()
                },
            })
        };

        let uploads = match (prefix, after_key, after_upload_id) {
            (Some(prefix), Some(after_key), Some(after_upload_id)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type FROM multipart_upload WHERE key >= ?1 AND (key > ?2 OR (key = ?2 AND upload_id > ?3)) ORDER BY key, upload_id LIMIT ?4;",
                )?;
                stmt.query_map(
                    rusqlite::params![prefix, after_key, after_upload_id, limit],
                    map_upload,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type FROM multipart_upload WHERE key >= ?1 AND key > ?2 ORDER BY key, upload_id LIMIT ?3;",
                )?;
                stmt.query_map(rusqlite::params![prefix, after_key, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (Some(prefix), None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type FROM multipart_upload WHERE key >= ?1 ORDER BY key, upload_id LIMIT ?2;",
                )?;
                stmt.query_map(rusqlite::params![prefix, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), Some(after_upload_id)) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type FROM multipart_upload WHERE key > ?1 OR (key = ?1 AND upload_id > ?2) ORDER BY key, upload_id LIMIT ?3;",
                )?;
                stmt.query_map(
                    rusqlite::params![after_key, after_upload_id, limit],
                    map_upload,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, Some(after_key), None) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type FROM multipart_upload WHERE key > ?1 ORDER BY key, upload_id LIMIT ?2;",
                )?;
                stmt.query_map(rusqlite::params![after_key, limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
            (None, None, _) => {
                let mut stmt = transaction.prepare_cached(
                    "SELECT key, upload_id, initiated, checksum_algorithm, checksum_type FROM multipart_upload ORDER BY key, upload_id LIMIT ?1;",
                )?;
                stmt.query_map([limit], map_upload)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            }
        };

        Ok(uploads)
    }

    /// Fetches one completed multipart object part boundary.
    pub(crate) fn try_get_completed_object_part(
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
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
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
                    checksum: row_checksum(row, None, None, 4)?,
                })
            })
            .optional()?)
    }

    /// Lists completed multipart object part boundaries ordered by part number.
    pub(crate) fn try_list_completed_object_parts(
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
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
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
                    checksum: row_checksum(row, None, None, 4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Counts recorded completed multipart object part boundaries.
    pub(crate) fn try_count_completed_object_parts(
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
    fn try_put_completed_object_parts(
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
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11);",
        )?;

        for part in parts {
            stmt.execute((
                key,
                version_id,
                part.part_number,
                sql_size(part.start)?,
                sql_size(part.size)?,
                &part.md5,
                part.checksum.crc32.as_deref(),
                part.checksum.crc32c.as_deref(),
                part.checksum.crc64nvme.as_deref(),
                part.checksum.sha1.as_deref(),
                part.checksum.sha256.as_deref(),
            ))?;
        }

        Ok(())
    }

    /// Fetches multipart upload parts with bodies ordered by part number.
    pub(crate) fn try_get_multiparts(
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
                checksum_crc32,
                checksum_crc32c,
                checksum_crc64nvme,
                checksum_sha1,
                checksum_sha256
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
                    checksum: row_checksum(row, None, None, 5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(objects)
    }

    /// Deletes multipart upload state and cascading parts.
    pub(crate) fn try_delete_multipart(
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
    pub(crate) fn try_validate_multipart_access(
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
