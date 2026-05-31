//! Shared checksum and wire-encoding primitives.
//!
//! This module intentionally contains only storage/protocol-neutral helpers.
//! S3 request validation and response shaping live in `s3::checksum`; `SQLite` row
//! conversion and metadata storage live in `sqlite::checksum`.

use std::ops::Not;

use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};

/// S3 algorithm name for CRC32 checksums.
pub(crate) const CHECKSUM_CRC32: &str = "CRC32";
/// S3 algorithm name for CRC32C checksums.
pub(crate) const CHECKSUM_CRC32C: &str = "CRC32C";
/// S3 algorithm name for CRC64NVME checksums.
pub(crate) const CHECKSUM_CRC64NVME: &str = "CRC64NVME";
/// S3 algorithm name for SHA1 checksums.
pub(crate) const CHECKSUM_SHA1: &str = "SHA1";
/// S3 algorithm name for SHA256 checksums.
pub(crate) const CHECKSUM_SHA256: &str = "SHA256";
/// S3 checksum type name for checksums composed from per-part checksums.
pub(crate) const CHECKSUM_TYPE_COMPOSITE: &str = "COMPOSITE";
/// S3 checksum type name for checksums calculated over the complete object body.
pub(crate) const CHECKSUM_TYPE_FULL_OBJECT: &str = "FULL_OBJECT";

/// Checksum algorithms supported by S3-compatible checksum headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChecksumAlgorithmKind {
    /// ISO-HDLC CRC32.
    Crc32,
    /// iSCSI CRC32C.
    Crc32c,
    /// `NVMe` CRC64.
    Crc64Nvme,
    /// SHA1 digest.
    Sha1,
    /// SHA256 digest.
    Sha256,
}

impl ChecksumAlgorithmKind {
    /// Parses an S3 checksum algorithm name.
    #[must_use]
    pub(crate) fn from_name(algorithm: &str) -> Option<Self> {
        let kind = match algorithm {
            CHECKSUM_CRC32 => Self::Crc32,
            CHECKSUM_CRC32C => Self::Crc32c,
            CHECKSUM_CRC64NVME => Self::Crc64Nvme,
            CHECKSUM_SHA1 => Self::Sha1,
            CHECKSUM_SHA256 => Self::Sha256,
            _ => return None,
        };
        Some(kind)
    }

    /// Returns the S3 checksum algorithm name.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => CHECKSUM_CRC32,
            Self::Crc32c => CHECKSUM_CRC32C,
            Self::Crc64Nvme => CHECKSUM_CRC64NVME,
            Self::Sha1 => CHECKSUM_SHA1,
            Self::Sha256 => CHECKSUM_SHA256,
        }
    }
}

/// Checksum aggregation modes supported by S3 object checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChecksumTypeKind {
    /// Multipart checksum built from per-part checksum bytes.
    Composite,
    /// Checksum calculated over the full object body.
    FullObject,
}

impl ChecksumTypeKind {
    /// Parses an S3 checksum type name.
    #[must_use]
    pub(crate) fn from_name(checksum_type: &str) -> Option<Self> {
        let kind = match checksum_type {
            CHECKSUM_TYPE_COMPOSITE => Self::Composite,
            CHECKSUM_TYPE_FULL_OBJECT => Self::FullObject,
            _ => return None,
        };
        Some(kind)
    }

    /// Returns the S3 checksum type name.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Composite => CHECKSUM_TYPE_COMPOSITE,
            Self::FullObject => CHECKSUM_TYPE_FULL_OBJECT,
        }
    }
}

/// Part-level checksum value, tagged by its algorithm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PartChecksum {
    /// Base64 `CRC32` checksum value.
    Crc32(String),
    /// Base64 `CRC32C` checksum value.
    Crc32c(String),
    /// Base64 `CRC64NVME` checksum value.
    Crc64Nvme(String),
    /// Base64 `SHA1` checksum value.
    Sha1(String),
    /// Base64 `SHA256` checksum value.
    Sha256(String),
}

impl PartChecksum {
    /// Builds a checksum value variant for an algorithm.
    #[must_use]
    pub(crate) fn new(kind: ChecksumAlgorithmKind, value: String) -> Self {
        match kind {
            ChecksumAlgorithmKind::Crc32 => Self::Crc32(value),
            ChecksumAlgorithmKind::Crc32c => Self::Crc32c(value),
            ChecksumAlgorithmKind::Crc64Nvme => Self::Crc64Nvme(value),
            ChecksumAlgorithmKind::Sha1 => Self::Sha1(value),
            ChecksumAlgorithmKind::Sha256 => Self::Sha256(value),
        }
    }

    /// Returns the algorithm for this checksum value.
    #[must_use]
    pub(crate) fn algorithm(&self) -> ChecksumAlgorithmKind {
        match self {
            Self::Crc32(_) => ChecksumAlgorithmKind::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithmKind::Crc32c,
            Self::Crc64Nvme(_) => ChecksumAlgorithmKind::Crc64Nvme,
            Self::Sha1(_) => ChecksumAlgorithmKind::Sha1,
            Self::Sha256(_) => ChecksumAlgorithmKind::Sha256,
        }
    }

    /// Returns the base64 checksum text.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Crc32(value)
            | Self::Crc32c(value)
            | Self::Crc64Nvme(value)
            | Self::Sha1(value)
            | Self::Sha256(value) => value,
        }
    }

    /// Returns the checksum value for an algorithm when it matches this variant.
    #[must_use]
    pub(crate) fn value_for(&self, kind: ChecksumAlgorithmKind) -> Option<&str> {
        (self.algorithm() == kind).then(|| self.as_str())
    }
}

/// Full-object checksum value, tagged by its algorithm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FullObjectChecksum {
    /// Base64 `CRC32` checksum value.
    Crc32(String),
    /// Base64 `CRC32C` checksum value.
    Crc32c(String),
    /// Base64 `CRC64NVME` checksum value.
    Crc64Nvme(String),
    /// Base64 `SHA1` checksum value.
    Sha1(String),
    /// Base64 `SHA256` checksum value.
    Sha256(String),
}

impl FullObjectChecksum {
    /// Builds a full-object checksum value variant for an algorithm.
    #[must_use]
    pub(crate) fn new(algorithm: ChecksumAlgorithmKind, value: String) -> Self {
        match algorithm {
            ChecksumAlgorithmKind::Crc32 => Self::Crc32(value),
            ChecksumAlgorithmKind::Crc32c => Self::Crc32c(value),
            ChecksumAlgorithmKind::Crc64Nvme => Self::Crc64Nvme(value),
            ChecksumAlgorithmKind::Sha1 => Self::Sha1(value),
            ChecksumAlgorithmKind::Sha256 => Self::Sha256(value),
        }
    }

    /// Returns the checksum algorithm.
    #[must_use]
    pub(crate) fn algorithm(&self) -> ChecksumAlgorithmKind {
        match self {
            Self::Crc32(_) => ChecksumAlgorithmKind::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithmKind::Crc32c,
            Self::Crc64Nvme(_) => ChecksumAlgorithmKind::Crc64Nvme,
            Self::Sha1(_) => ChecksumAlgorithmKind::Sha1,
            Self::Sha256(_) => ChecksumAlgorithmKind::Sha256,
        }
    }

    /// Returns the base64 checksum text.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Crc32(value)
            | Self::Crc32c(value)
            | Self::Crc64Nvme(value)
            | Self::Sha1(value)
            | Self::Sha256(value) => value,
        }
    }
}

/// Composite checksum value, tagged by its algorithm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompositeChecksum {
    /// Base64 `CRC32` checksum value.
    Crc32(String),
    /// Base64 `CRC32C` checksum value.
    Crc32c(String),
    /// Base64 `SHA1` checksum value.
    Sha1(String),
    /// Base64 `SHA256` checksum value.
    Sha256(String),
}

impl CompositeChecksum {
    /// Builds a composite checksum value variant for a supported algorithm.
    #[must_use]
    pub(crate) fn new(algorithm: ChecksumAlgorithmKind, value: String) -> Option<Self> {
        let checksum = match algorithm {
            ChecksumAlgorithmKind::Crc32 => Self::Crc32(value),
            ChecksumAlgorithmKind::Crc32c => Self::Crc32c(value),
            ChecksumAlgorithmKind::Crc64Nvme => return None,
            ChecksumAlgorithmKind::Sha1 => Self::Sha1(value),
            ChecksumAlgorithmKind::Sha256 => Self::Sha256(value),
        };
        Some(checksum)
    }

    /// Returns the checksum algorithm.
    #[must_use]
    pub(crate) fn algorithm(&self) -> ChecksumAlgorithmKind {
        match self {
            Self::Crc32(_) => ChecksumAlgorithmKind::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithmKind::Crc32c,
            Self::Sha1(_) => ChecksumAlgorithmKind::Sha1,
            Self::Sha256(_) => ChecksumAlgorithmKind::Sha256,
        }
    }

    /// Returns the base64 checksum text.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Crc32(value) | Self::Crc32c(value) | Self::Sha1(value) | Self::Sha256(value) => {
                value
            }
        }
    }
}

/// Completed object checksum value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StoredChecksum {
    /// Checksum calculated over the full object body.
    Full(FullObjectChecksum),
    /// Multipart checksum composed from per-part checksum bytes.
    Composite(CompositeChecksum),
}

impl StoredChecksum {
    /// Builds a completed-object checksum for a supported algorithm/type pair.
    #[must_use]
    pub(crate) fn new(
        algorithm: ChecksumAlgorithmKind,
        checksum_type: ChecksumTypeKind,
        value: String,
    ) -> Option<Self> {
        let checksum = match checksum_type {
            ChecksumTypeKind::FullObject => Self::Full(FullObjectChecksum::new(algorithm, value)),
            ChecksumTypeKind::Composite => {
                Self::Composite(CompositeChecksum::new(algorithm, value)?)
            }
        };
        Some(checksum)
    }

    /// Returns the checksum algorithm.
    #[must_use]
    pub(crate) fn algorithm(&self) -> ChecksumAlgorithmKind {
        match self {
            Self::Full(checksum) => checksum.algorithm(),
            Self::Composite(checksum) => checksum.algorithm(),
        }
    }

    /// Returns the checksum aggregation type.
    #[must_use]
    pub(crate) fn checksum_type(&self) -> ChecksumTypeKind {
        match self {
            Self::Full(_) => ChecksumTypeKind::FullObject,
            Self::Composite(_) => ChecksumTypeKind::Composite,
        }
    }

    /// Returns the base64 checksum text.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Full(checksum) => checksum.as_str(),
            Self::Composite(checksum) => checksum.as_str(),
        }
    }

    /// Returns the checksum value when this object checksum uses the algorithm.
    #[must_use]
    pub(crate) fn value_for(&self, algorithm: ChecksumAlgorithmKind) -> Option<&str> {
        (self.algorithm() == algorithm).then(|| self.as_str())
    }
}

/// Encodes bytes as lowercase hexadecimal text.
#[must_use]
pub(crate) fn encode_hex(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let input = input.as_ref();
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Decodes exactly sixteen bytes from lowercase or uppercase MD5 hex text.
#[must_use]
pub(crate) fn decode_md5_hex(value: &str) -> Option<[u8; 16]> {
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
        return None;
    }

    let mut decoded = [0; 16];
    for (idx, pair) in bytes.chunks_exact(2).enumerate() {
        let upper = nibble(pair[0])?;
        let lower = nibble(pair[1])?;
        decoded[idx] = (upper << 4) | lower;
    }
    Some(decoded)
}

/// Encodes bytes as standard padded base64 text.
#[must_use]
pub(crate) fn encode_base64(input: &[u8]) -> String {
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

/// Decodes standard padded base64 text.
///
/// Empty input, invalid padding, non-canonical padding bits, and characters
/// outside the standard alphabet return `None`.
#[must_use]
pub(crate) fn decode_base64(value: &str) -> Option<Vec<u8>> {
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

/// Computes a checksum and returns its standard padded base64 representation.
#[must_use]
pub(crate) fn checksum_value(kind: ChecksumAlgorithmKind, value: &[u8]) -> String {
    match kind {
        ChecksumAlgorithmKind::Crc32 => encode_base64(Crc32::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Crc32c => encode_base64(Crc32c::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Crc64Nvme => encode_base64(Crc64Nvme::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Sha1 => encode_base64(Sha1::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Sha256 => encode_base64(Sha256::checksum(value).as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKSUM_CRC32, CHECKSUM_CRC32C, CHECKSUM_CRC64NVME, CHECKSUM_SHA1, CHECKSUM_SHA256,
        CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT, ChecksumAlgorithmKind,
        ChecksumTypeKind, FullObjectChecksum, PartChecksum, StoredChecksum, checksum_value,
        decode_base64, decode_md5_hex, encode_base64, encode_hex,
    };

    #[test]
    fn maps_checksum_algorithm_names() {
        for (name, kind) in [
            (CHECKSUM_CRC32, ChecksumAlgorithmKind::Crc32),
            (CHECKSUM_CRC32C, ChecksumAlgorithmKind::Crc32c),
            (CHECKSUM_CRC64NVME, ChecksumAlgorithmKind::Crc64Nvme),
            (CHECKSUM_SHA1, ChecksumAlgorithmKind::Sha1),
            (CHECKSUM_SHA256, ChecksumAlgorithmKind::Sha256),
        ] {
            assert_eq!(ChecksumAlgorithmKind::from_name(name), Some(kind));
            assert_eq!(kind.as_str(), name);
        }
        assert_eq!(ChecksumAlgorithmKind::from_name("MD5"), None);
    }

    #[test]
    fn maps_checksum_type_names() {
        for (name, kind) in [
            (CHECKSUM_TYPE_COMPOSITE, ChecksumTypeKind::Composite),
            (CHECKSUM_TYPE_FULL_OBJECT, ChecksumTypeKind::FullObject),
        ] {
            assert_eq!(ChecksumTypeKind::from_name(name), Some(kind));
            assert_eq!(kind.as_str(), name);
        }
        assert_eq!(ChecksumTypeKind::from_name("LINEAR"), None);
    }

    #[test]
    fn part_checksum_variant_carries_algorithm() {
        let value = PartChecksum::new(ChecksumAlgorithmKind::Sha256, "sha".to_string());
        assert_eq!(value.algorithm(), ChecksumAlgorithmKind::Sha256);
        assert_eq!(value.as_str(), "sha");
        assert_eq!(value.value_for(ChecksumAlgorithmKind::Sha256), Some("sha"));
        assert_eq!(value.value_for(ChecksumAlgorithmKind::Sha1), None);
    }

    #[test]
    fn stored_checksum_variant_carries_algorithm_and_type() {
        let checksum = StoredChecksum::new(
            ChecksumAlgorithmKind::Crc64Nvme,
            ChecksumTypeKind::FullObject,
            "crc".to_string(),
        );
        assert_eq!(
            checksum.as_ref().map(StoredChecksum::algorithm),
            Some(ChecksumAlgorithmKind::Crc64Nvme)
        );
        assert_eq!(
            checksum.as_ref().map(StoredChecksum::checksum_type),
            Some(ChecksumTypeKind::FullObject)
        );
        assert_eq!(checksum.as_ref().map(StoredChecksum::as_str), Some("crc"));
        assert_eq!(
            checksum,
            Some(StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                "crc".to_string()
            )))
        );
        assert_eq!(
            StoredChecksum::new(
                ChecksumAlgorithmKind::Crc64Nvme,
                ChecksumTypeKind::Composite,
                "crc".to_string(),
            ),
            None
        );
    }

    #[test]
    fn encodes_lowercase_hex() {
        assert_eq!(encode_hex([0x00, 0x0f, 0x10, 0xab, 0xff]), "000f10abff");
    }

    #[test]
    fn decodes_md5_hex() {
        assert_eq!(
            decode_md5_hex("EB733A00C0C9D336E65691A37AB54293"),
            Some([
                0xeb, 0x73, 0x3a, 0x00, 0xc0, 0xc9, 0xd3, 0x36, 0xe6, 0x56, 0x91, 0xa3, 0x7a, 0xb5,
                0x42, 0x93
            ])
        );
        assert_eq!(decode_md5_hex("too-short"), None);
        assert_eq!(decode_md5_hex("gg733a00c0c9d336e65691a37ab54293"), None);
    }

    #[test]
    fn encodes_standard_base64() {
        assert_eq!(encode_base64(b""), "");
        assert_eq!(encode_base64(b"f"), "Zg==");
        assert_eq!(encode_base64(b"fo"), "Zm8=");
        assert_eq!(encode_base64(b"foo"), "Zm9v");
        assert_eq!(encode_base64(b"foob"), "Zm9vYg==");
    }

    #[test]
    fn decodes_standard_base64() {
        assert_eq!(
            decode_base64("SpRKmvVRaPLiBjkHxCGwYQ=="),
            Some(
                [
                    0x4a, 0x94, 0x4a, 0x9a, 0xf5, 0x51, 0x68, 0xf2, 0xe2, 0x06, 0x39, 0x07, 0xc4,
                    0x21, 0xb0, 0x61
                ]
                .to_vec()
            )
        );
    }

    #[test]
    fn rejects_invalid_base64() {
        for value in ["", "abc", "not base64", "YW=J", "YR==", "YWJ="] {
            assert_eq!(decode_base64(value), None, "{value}");
        }
    }

    #[test]
    fn computes_base64_checksum_values() {
        let value = b"test data";
        assert_eq!(
            checksum_value(ChecksumAlgorithmKind::Crc32, value),
            "0wiusg=="
        );
        assert_eq!(
            checksum_value(ChecksumAlgorithmKind::Crc32c, value),
            "M3m0yg=="
        );
        assert_eq!(
            checksum_value(ChecksumAlgorithmKind::Crc64Nvme, value),
            "rsrzr5yYqFU="
        );
        assert_eq!(
            checksum_value(ChecksumAlgorithmKind::Sha1, value),
            "9I3YU4IIYIFsddVND1hNyGMyenw="
        );
        assert_eq!(
            checksum_value(ChecksumAlgorithmKind::Sha256, value),
            "kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk="
        );
    }
}
