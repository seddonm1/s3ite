use std::ops::Not;

use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};

use super::{SqliteError, SqliteResult};

pub(super) const CHECKSUM_CRC32: &str = "CRC32";
pub(super) const CHECKSUM_CRC32C: &str = "CRC32C";
pub(super) const CHECKSUM_CRC64NVME: &str = "CRC64NVME";
pub(super) const CHECKSUM_SHA1: &str = "SHA1";
pub(super) const CHECKSUM_SHA256: &str = "SHA256";
pub(super) const CHECKSUM_TYPE_COMPOSITE: &str = "COMPOSITE";
pub(super) const CHECKSUM_TYPE_FULL_OBJECT: &str = "FULL_OBJECT";

pub(super) fn encode_hex(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let input = input.as_ref();
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(super) fn decode_md5_hex(value: &str) -> SqliteResult<[u8; 16]> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    if bytes.len() != 32 {
        return Err(SqliteError::InvalidMultipartPart);
    }

    let mut decoded = [0; 16];
    for (idx, pair) in bytes.chunks_exact(2).enumerate() {
        let upper = nibble(pair[0]).ok_or(SqliteError::InvalidMultipartPart)?;
        let lower = nibble(pair[1]).ok_or(SqliteError::InvalidMultipartPart)?;
        decoded[idx] = (upper << 4) | lower;
    }
    Ok(decoded)
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

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(char::from(TABLE[usize::from(first >> 2)]));
        output.push(char::from(
            TABLE[usize::from(((first & 0b0000_0011) << 4) | (second >> 4))],
        ));
        if chunk.len() > 1 {
            output.push(char::from(
                TABLE[usize::from(((second & 0b0000_1111) << 2) | (third >> 6))],
            ));
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(char::from(TABLE[usize::from(third & 0b0011_1111)]));
        } else {
            output.push('=');
        }
    }
    output
}

pub(super) fn decode_base64(value: &str) -> Option<Vec<u8>> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len().is_multiple_of(4).not() {
        return None;
    }

    let chunk_count = bytes.len() / 4;
    let mut decoded = Vec::with_capacity(chunk_count * 3);
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let is_last = index + 1 == chunk_count;
        let padding = match (chunk[2], chunk[3]) {
            (b'=', b'=') => 2,
            (b'=', _) => return None,
            (_, b'=') => 1,
            _ => 0,
        };

        if padding > 0 && is_last.not() {
            return None;
        }

        let b0 = sextet(chunk[0])?;
        let b1 = sextet(chunk[1])?;
        let b2 = if padding == 2 { 0 } else { sextet(chunk[2])? };
        let b3 = if padding > 0 { 0 } else { sextet(chunk[3])? };

        if (padding == 2 && (b1 & 0x0f) != 0) || (padding == 1 && (b2 & 0x03) != 0) {
            return None;
        }

        decoded.push((b0 << 2) | (b1 >> 4));
        if padding < 2 {
            decoded.push((b1 << 4) | (b2 >> 2));
        }
        if padding == 0 {
            decoded.push((b2 << 6) | b3);
        }
    }

    Some(decoded)
}

pub(super) fn checksum_value(algorithm: &str, value: &[u8]) -> SqliteResult<String> {
    let checksum = match algorithm {
        CHECKSUM_CRC32 => base64_encode(Crc32::checksum(value).as_ref()),
        CHECKSUM_CRC32C => base64_encode(Crc32c::checksum(value).as_ref()),
        CHECKSUM_CRC64NVME => base64_encode(Crc64Nvme::checksum(value).as_ref()),
        CHECKSUM_SHA1 => base64_encode(Sha1::checksum(value).as_ref()),
        CHECKSUM_SHA256 => base64_encode(Sha256::checksum(value).as_ref()),
        _ => return Err(SqliteError::ChecksumMismatch),
    };
    Ok(checksum)
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
