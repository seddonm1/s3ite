use crate::checksum::{
    CHECKSUM_CRC32, CHECKSUM_CRC32C, CHECKSUM_CRC64NVME, CHECKSUM_SHA1, CHECKSUM_SHA256,
    ChecksumAlgorithmKind, checksum_value as storage_checksum_value,
    decode_md5_hex as decode_storage_md5_hex,
};

use super::{SqliteError, SqliteResult};

pub(super) use crate::checksum::{
    CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT, decode_base64, encode_hex,
};

pub(super) fn decode_md5_hex(value: &str) -> SqliteResult<[u8; 16]> {
    decode_storage_md5_hex(value).ok_or(SqliteError::InvalidMultipartPart)
}

pub(super) fn row_checksum(
    row: &rusqlite::Row<'_>,
    algorithm_idx: Option<usize>,
    checksum_type_idx: Option<usize>,
    value_idx: usize,
) -> rusqlite::Result<ChecksumMetadata> {
    let algorithm = if let Some(index) = algorithm_idx {
        row.get(index)?
    } else {
        None
    };
    let checksum_type = if let Some(index) = checksum_type_idx {
        row.get(index)?
    } else {
        None
    };

    Ok(ChecksumMetadata {
        algorithm,
        checksum_type,
        crc32: row.get(value_idx)?,
        crc32c: row.get(value_idx + 1)?,
        crc64nvme: row.get(value_idx + 2)?,
        sha1: row.get(value_idx + 3)?,
        sha256: row.get(value_idx + 4)?,
    })
}

pub(super) fn checksum_value(algorithm: &str, value: &[u8]) -> SqliteResult<String> {
    let kind = ChecksumAlgorithmKind::from_name(algorithm).ok_or(SqliteError::ChecksumMismatch)?;
    Ok(storage_checksum_value(kind, value))
}

/// Stored checksum values for an object, multipart upload, or part.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ChecksumMetadata {
    pub(crate) algorithm: Option<String>,
    pub(crate) checksum_type: Option<String>,
    pub(crate) crc32: Option<String>,
    pub(crate) crc32c: Option<String>,
    pub(crate) crc64nvme: Option<String>,
    pub(crate) sha1: Option<String>,
    pub(crate) sha256: Option<String>,
}

impl ChecksumMetadata {
    /// Returns whether no checksum value is stored.
    #[must_use]
    pub(crate) fn has_value(&self) -> bool {
        self.crc32.is_some()
            || self.crc32c.is_some()
            || self.crc64nvme.is_some()
            || self.sha1.is_some()
            || self.sha256.is_some()
    }

    /// Returns the checksum value for an algorithm name.
    #[must_use]
    pub(crate) fn value_for(&self, algorithm: &str) -> Option<&str> {
        match algorithm {
            CHECKSUM_CRC32 => self.crc32.as_deref(),
            CHECKSUM_CRC32C => self.crc32c.as_deref(),
            CHECKSUM_CRC64NVME => self.crc64nvme.as_deref(),
            CHECKSUM_SHA1 => self.sha1.as_deref(),
            CHECKSUM_SHA256 => self.sha256.as_deref(),
            _ => None,
        }
    }

    /// Stores the checksum value for an algorithm name.
    pub(crate) fn set_value(&mut self, algorithm: &str, value: String) -> SqliteResult<()> {
        match algorithm {
            CHECKSUM_CRC32 => self.crc32 = Some(value),
            CHECKSUM_CRC32C => self.crc32c = Some(value),
            CHECKSUM_CRC64NVME => self.crc64nvme = Some(value),
            CHECKSUM_SHA1 => self.sha1 = Some(value),
            CHECKSUM_SHA256 => self.sha256 = Some(value),
            _ => return Err(SqliteError::ChecksumMismatch),
        }
        Ok(())
    }
}
