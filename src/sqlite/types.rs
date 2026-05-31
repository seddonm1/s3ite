use std::collections::HashMap;

use rusqlite::{self, Error::ToSqlConversionFailure};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::sqlite::error::{NegativeObjectSize, NegativeSqlLimit};

use super::ChecksumMetadata;

pub(super) fn row_size(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<i64> {
    let value = row.get::<_, i64>(idx)?;
    if value < 0 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Integer,
            Box::new(NegativeObjectSize),
        ));
    }
    Ok(value)
}

pub(super) fn sql_size(value: i64) -> rusqlite::Result<i64> {
    if value < 0 {
        return Err(ToSqlConversionFailure(Box::new(NegativeObjectSize)));
    }
    Ok(value)
}

pub(super) fn sql_limit(value: i64) -> rusqlite::Result<i64> {
    if value < 0 {
        return Err(ToSqlConversionFailure(Box::new(NegativeSqlLimit)));
    }
    Ok(value)
}

pub(super) fn row_bool(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<bool> {
    Ok(row.get::<_, i32>(idx)? != 0)
}

/// Bucket-level S3 object versioning state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BucketVersioningState {
    /// Versioning has never been configured on this bucket.
    NeverConfigured,
    /// New object writes create generated version IDs.
    Enabled,
    /// New object writes replace the special `null` version.
    Suspended,
}

impl BucketVersioningState {
    /// Returns the serialized database representation.
    #[must_use]
    pub(super) fn as_sql(self) -> &'static str {
        match self {
            Self::NeverConfigured => "never_configured",
            Self::Enabled => "enabled",
            Self::Suspended => "suspended",
        }
    }

    /// Parses a database representation.
    pub(super) fn from_sql(value: &str) -> rusqlite::Result<Self> {
        match value {
            "never_configured" => Ok(Self::NeverConfigured),
            "enabled" => Ok(Self::Enabled),
            "suspended" => Ok(Self::Suspended),
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }

    /// Returns whether write responses expose the stored version ID.
    #[must_use]
    pub(super) fn returns_write_version_id(self) -> bool {
        self == Self::Enabled
    }
}

/// Object body and metadata loaded from the bucket database.
#[derive(Debug)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) version_id: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) key_metadata: KeyMetadata,
}

/// Minimal object metadata returned by list queries.
#[derive(Debug)]
pub(crate) struct ObjectSummary {
    pub(crate) key: String,
    pub(crate) version_id: String,
    pub(crate) size: i64,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
    pub(crate) is_latest: bool,
    pub(crate) is_delete_marker: bool,
}

/// ETag-like object identity condition evaluated by the storage layer.
#[derive(Clone, Debug)]
pub(crate) enum ObjectTagCondition {
    Any,
    Md5(String),
    Never,
}

/// Preconditions that must hold before replacing an object.
#[derive(Clone, Debug, Default)]
pub(crate) struct ObjectWriteConditions {
    pub(crate) if_match: Option<ObjectTagCondition>,
    pub(crate) if_none_match: Option<ObjectTagCondition>,
}

/// Initiated multipart upload state shared by all uploaded parts.
#[derive(Debug)]
pub(crate) struct MultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) content_type: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) checksum: ChecksumMetadata,
}

/// Active multipart upload row returned by multipart upload listing.
#[derive(Debug)]
pub(crate) struct MultipartUploadSummary {
    pub(crate) key: String,
    pub(crate) upload_id: Uuid,
    pub(crate) initiated: OffsetDateTime,
    pub(crate) checksum: ChecksumMetadata,
}

/// Completion ledger entry used to make multipart completion idempotent.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadState {
    pub(crate) access_key: Option<String>,
    pub(crate) md5: String,
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) checksum: ChecksumMetadata,
}

/// Values inserted into the multipart completion ledger.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadRecord<'a> {
    pub(crate) upload_id: Uuid,
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) access_key: Option<String>,
    pub(crate) md5: &'a str,
    pub(crate) version_id: &'a str,
    pub(crate) returned_version_id: Option<&'a str>,
    pub(crate) checksum: &'a ChecksumMetadata,
}

/// Result of materializing multipart upload parts into an object version.
#[derive(Debug)]
pub(crate) struct CompletedMultipartUploadOutcome {
    pub(crate) md5: String,
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) checksum: ChecksumMetadata,
}

/// Object metadata loaded without the object body.
#[derive(Debug)]
pub(crate) struct KeyMetadata {
    pub(crate) version_id: String,
    pub(crate) size: i64,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) content_type: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires: Option<OffsetDateTime>,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: ChecksumMetadata,
    pub(crate) is_latest: bool,
    pub(crate) is_delete_marker: bool,
}

/// Result of storing an object version.
#[derive(Debug)]
pub(crate) struct ObjectWriteOutcome {
    pub(crate) version_id: String,
    pub(crate) returned_version_id: Option<String>,
    pub(crate) md5: Option<String>,
    pub(crate) last_modified: OffsetDateTime,
}

/// Result of deleting an object or object version.
#[derive(Debug)]
pub(crate) struct ObjectDeleteOutcome {
    pub(crate) version_id: Option<String>,
    pub(crate) is_delete_marker: bool,
    pub(crate) delete_marker_version_id: Option<String>,
}

/// Stored multipart upload part with its body and checksum metadata.
#[derive(Debug)]
pub(crate) struct Multipart {
    pub(crate) upload_id: Uuid,
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) value: Vec<u8>,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: ChecksumMetadata,
}

/// Multipart upload part metadata returned by list-parts queries.
#[derive(Debug)]
pub(crate) struct MultipartMetadata {
    pub(crate) part_number: i32,
    pub(crate) last_modified: OffsetDateTime,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: ChecksumMetadata,
}

/// Stored part boundary for a completed multipart object.
#[derive(Debug)]
pub(crate) struct CompletedObjectPart {
    pub(crate) part_number: i32,
    pub(crate) start: i64,
    pub(crate) size: i64,
    pub(crate) md5: Option<String>,
    pub(crate) checksum: ChecksumMetadata,
}

/// Client-requested multipart part checksum used during completion.
#[derive(Debug)]
pub(crate) struct MultipartCompletionPart {
    pub(crate) part_number: i32,
    pub(crate) md5: String,
    pub(crate) checksum: ChecksumMetadata,
}

/// Request data needed to complete a multipart upload transactionally.
#[derive(Debug)]
pub(crate) struct MultipartCompletionRequest {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) upload_id: Uuid,
    pub(crate) requested_parts: Vec<MultipartCompletionPart>,
    pub(crate) requested_checksum: ChecksumMetadata,
    pub(crate) default_checksum: Option<ChecksumMetadata>,
    pub(crate) access_key: Option<String>,
    pub(crate) write_conditions: ObjectWriteConditions,
}
