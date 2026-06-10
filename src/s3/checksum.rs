//! S3 checksum model, storage metadata, request validation, and response mapping.
//!
//! Checksum headers and multipart checksum semantics are S3 behavior. `SQLite`
//! persists the typed metadata defined here, but checksum parsing, calculation,
//! and response shaping stay owned by the S3 layer.

use std::ops::Not;

use s3s::{
    S3ErrorCode, S3Result,
    crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256},
    dto::{Checksum as S3Checksum, ChecksumAlgorithm, ChecksumMode, ChecksumType},
};

use crate::sqlite::{CompletedObjectPart, KeyValue, SqliteError, SqliteResult};

use super::{s3_error, s3_error_from_sqlite, s3_error_with_message};

/// Re-exported lowercase hexadecimal encoder used for S3 `ETag` formatting.
pub(super) use self::encode_hex as hex;

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

/// Checksum aggregation modes supported by S3 object checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChecksumTypeKind {
    /// Multipart checksum built from per-part checksum bytes.
    Composite,
    /// Checksum calculated over the full object body.
    FullObject,
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

/// Incremental checksum state for one S3 checksum algorithm.
pub(crate) enum ChecksumAccumulator {
    /// Incremental `CRC32` checksum state.
    Crc32(Crc32),
    /// Incremental `CRC32C` checksum state.
    Crc32c(Crc32c),
    /// Incremental `CRC64NVME` checksum state.
    Crc64Nvme(Crc64Nvme),
    /// Incremental `SHA1` checksum state.
    Sha1(Sha1),
    /// Incremental `SHA256` checksum state.
    Sha256(Sha256),
}

impl ChecksumAccumulator {
    /// Creates an empty accumulator for the selected checksum algorithm.
    #[must_use]
    pub(crate) fn new(algorithm: ChecksumAlgorithmKind) -> Self {
        match algorithm {
            ChecksumAlgorithmKind::Crc32 => Self::Crc32(Crc32::new()),
            ChecksumAlgorithmKind::Crc32c => Self::Crc32c(Crc32c::new()),
            ChecksumAlgorithmKind::Crc64Nvme => Self::Crc64Nvme(Crc64Nvme::new()),
            ChecksumAlgorithmKind::Sha1 => Self::Sha1(Sha1::new()),
            ChecksumAlgorithmKind::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    /// Returns the algorithm this accumulator computes.
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

    /// Adds bytes to the checksum state.
    pub(crate) fn update(&mut self, value: &[u8]) {
        match self {
            Self::Crc32(checksum) => checksum.update(value),
            Self::Crc32c(checksum) => checksum.update(value),
            Self::Crc64Nvme(checksum) => checksum.update(value),
            Self::Sha1(checksum) => checksum.update(value),
            Self::Sha256(checksum) => checksum.update(value),
        }
    }

    /// Finalizes the accumulator as a base64 S3 checksum value.
    #[must_use]
    pub(crate) fn finish_value(self) -> String {
        match self {
            Self::Crc32(checksum) => encode_base64(checksum.finalize().as_ref()),
            Self::Crc32c(checksum) => encode_base64(checksum.finalize().as_ref()),
            Self::Crc64Nvme(checksum) => encode_base64(checksum.finalize().as_ref()),
            Self::Sha1(checksum) => encode_base64(checksum.finalize().as_ref()),
            Self::Sha256(checksum) => encode_base64(checksum.finalize().as_ref()),
        }
    }

    /// Finalizes the accumulator as a part checksum.
    #[must_use]
    pub(crate) fn finish_part(self) -> PartChecksum {
        let algorithm = self.algorithm();
        PartChecksum::new(algorithm, self.finish_value())
    }

    /// Finalizes the accumulator as completed-object checksum metadata.
    #[must_use]
    pub(crate) fn finish_stored(self, checksum_type: ChecksumTypeKind) -> Option<StoredChecksum> {
        let algorithm = self.algorithm();
        StoredChecksum::new(algorithm, checksum_type, self.finish_value())
    }

    /// Finalizes the accumulator as a multipart composite checksum.
    #[must_use]
    pub(crate) fn finish_composite(self, part_count: usize) -> Option<StoredChecksum> {
        let algorithm = self.algorithm();
        let value = format!("{}-{part_count}", self.finish_value());
        StoredChecksum::new(algorithm, ChecksumTypeKind::Composite, value)
    }
}

/// Checksum algorithm and aggregation mode requested for an active multipart upload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MultipartChecksumConfig {
    /// Algorithm used for uploaded parts and final multipart checksum metadata.
    pub(crate) algorithm: ChecksumAlgorithmKind,
    /// Aggregation mode used for the completed object checksum.
    pub(crate) checksum_type: ChecksumTypeKind,
}

impl MultipartChecksumConfig {
    /// Builds a checksum configuration for an active multipart upload.
    #[must_use]
    pub(crate) fn new(algorithm: ChecksumAlgorithmKind, checksum_type: ChecksumTypeKind) -> Self {
        Self {
            algorithm,
            checksum_type,
        }
    }
}

/// Checksum fields supplied with a multipart completion request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MultipartCompletionChecksum {
    /// Checksum algorithm named by the completion request, if any.
    pub(crate) algorithm: Option<ChecksumAlgorithmKind>,
    /// Requested checksum type, even when no final checksum value was supplied.
    pub(crate) checksum_type: Option<ChecksumTypeKind>,
    /// Client-supplied final checksum value, if any.
    pub(crate) value: Option<String>,
}

impl MultipartCompletionChecksum {
    /// Returns the supplied checksum algorithm.
    #[must_use]
    pub(crate) fn algorithm(&self) -> Option<ChecksumAlgorithmKind> {
        self.algorithm
    }

    /// Returns the requested checksum type.
    #[must_use]
    pub(crate) fn checksum_type(&self) -> Option<ChecksumTypeKind> {
        self.checksum_type
    }

    /// Returns the checksum value for an algorithm when supplied.
    #[must_use]
    pub(crate) fn value_for(&self, algorithm: ChecksumAlgorithmKind) -> Option<&str> {
        (self.algorithm == Some(algorithm))
            .then_some(self.value.as_deref())
            .flatten()
    }

    /// Returns whether the request supplied a checksum value.
    #[must_use]
    pub(crate) fn has_value(&self) -> bool {
        self.value.is_some()
    }
}

/// Borrowed checksum metadata in the shapes needed by S3 response headers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ChecksumView<'a> {
    /// No checksum metadata is available.
    None,
    /// Completed-object checksum metadata with algorithm, type, and value.
    Object(&'a StoredChecksum),
    /// Part-level checksum metadata with algorithm and value.
    Part(&'a PartChecksum),
    /// Active multipart checksum configuration with algorithm and type only.
    MultipartConfig(MultipartChecksumConfig),
}

impl<'a> ChecksumView<'a> {
    /// Returns the checksum algorithm when metadata is present.
    #[must_use]
    pub(crate) fn algorithm(self) -> Option<ChecksumAlgorithmKind> {
        match self {
            Self::None => None,
            Self::Object(checksum) => Some(checksum.algorithm()),
            Self::Part(checksum) => Some(checksum.algorithm()),
            Self::MultipartConfig(checksum) => Some(checksum.algorithm),
        }
    }

    /// Returns the object checksum aggregation type when metadata carries one.
    #[must_use]
    pub(crate) fn checksum_type(self) -> Option<ChecksumTypeKind> {
        match self {
            Self::None | Self::Part(_) => None,
            Self::Object(checksum) => Some(checksum.checksum_type()),
            Self::MultipartConfig(checksum) => Some(checksum.checksum_type),
        }
    }

    /// Returns the checksum value for a specific algorithm.
    #[must_use]
    pub(crate) fn value_for(self, algorithm: ChecksumAlgorithmKind) -> Option<&'a str> {
        match self {
            Self::None | Self::MultipartConfig(_) => None,
            Self::Object(checksum) => checksum.value_for(algorithm),
            Self::Part(checksum) => checksum.value_for(algorithm),
        }
    }

    /// Returns whether this view carries an actual checksum value.
    #[must_use]
    pub(crate) fn has_value(self) -> bool {
        match self {
            Self::None | Self::MultipartConfig(_) => false,
            Self::Object(_) | Self::Part(_) => true,
        }
    }
}

impl<'a> From<&'a StoredChecksum> for ChecksumView<'a> {
    fn from(checksum: &'a StoredChecksum) -> Self {
        Self::Object(checksum)
    }
}

impl<'a> From<Option<&'a StoredChecksum>> for ChecksumView<'a> {
    fn from(checksum: Option<&'a StoredChecksum>) -> Self {
        checksum.map_or(Self::None, Self::Object)
    }
}

impl<'a> From<&'a Option<StoredChecksum>> for ChecksumView<'a> {
    fn from(checksum: &'a Option<StoredChecksum>) -> Self {
        checksum.as_ref().into()
    }
}

impl<'a> From<&'a PartChecksum> for ChecksumView<'a> {
    fn from(checksum: &'a PartChecksum) -> Self {
        Self::Part(checksum)
    }
}

impl<'a> From<Option<&'a PartChecksum>> for ChecksumView<'a> {
    fn from(checksum: Option<&'a PartChecksum>) -> Self {
        checksum.map_or(Self::None, Self::Part)
    }
}

impl<'a> From<&'a Option<PartChecksum>> for ChecksumView<'a> {
    fn from(checksum: &'a Option<PartChecksum>) -> Self {
        checksum.as_ref().into()
    }
}

impl From<MultipartChecksumConfig> for ChecksumView<'_> {
    fn from(checksum: MultipartChecksumConfig) -> Self {
        Self::MultipartConfig(checksum)
    }
}

impl From<Option<MultipartChecksumConfig>> for ChecksumView<'_> {
    fn from(checksum: Option<MultipartChecksumConfig>) -> Self {
        checksum.map_or(Self::None, Self::MultipartConfig)
    }
}

impl From<&Option<MultipartChecksumConfig>> for ChecksumView<'_> {
    fn from(checksum: &Option<MultipartChecksumConfig>) -> Self {
        (*checksum).into()
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

/// Policy for implicit checksum calculation when a request omits checksum input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ChecksumPolicy {
    /// Attach S3's default full-object `CRC64NVME` checksum when omitted.
    DefaultCrc64NvmeFullObject,
}

impl ChecksumPolicy {
    /// Returns the implicit algorithm for this policy.
    fn default_kind(self) -> ChecksumAlgorithmKind {
        match self {
            Self::DefaultCrc64NvmeFullObject => ChecksumAlgorithmKind::Crc64Nvme,
        }
    }
}

/// Raw S3 checksum fields extracted from object or multipart requests.
#[derive(Clone, Debug, Default)]
pub(super) struct ChecksumInput {
    /// Requested checksum algorithm, if the request supplied one.
    pub(super) algorithm: Option<ChecksumAlgorithm>,
    /// Requested checksum type, usually `COMPOSITE` or `FULL_OBJECT`.
    pub(super) checksum_type: Option<ChecksumType>,
    /// Request-provided `CRC32` checksum.
    pub(super) crc32: Option<String>,
    /// Request-provided `CRC32C` checksum.
    pub(super) crc32c: Option<String>,
    /// Request-provided `CRC64NVME` checksum.
    pub(super) crc64nvme: Option<String>,
    /// Request-provided `SHA1` checksum.
    pub(super) sha1: Option<String>,
    /// Request-provided `SHA256` checksum.
    pub(super) sha256: Option<String>,
}

impl ChecksumInput {
    /// Builds checksum input from object or upload-part request fields.
    pub(super) fn write_request(
        algorithm: Option<ChecksumAlgorithm>,
        crc32: Option<String>,
        crc32c: Option<String>,
        crc64nvme: Option<String>,
        sha1: Option<String>,
        sha256: Option<String>,
    ) -> Self {
        Self {
            algorithm,
            crc32,
            crc32c,
            crc64nvme,
            sha1,
            sha256,
            ..Self::default()
        }
    }

    /// Builds checksum input from completion request checksum fields.
    pub(super) fn completion_request(
        checksum_type: Option<ChecksumType>,
        crc32: Option<String>,
        crc32c: Option<String>,
        crc64nvme: Option<String>,
        sha1: Option<String>,
        sha256: Option<String>,
    ) -> Self {
        Self {
            checksum_type,
            crc32,
            crc32c,
            crc64nvme,
            sha1,
            sha256,
            ..Self::default()
        }
    }

    /// Builds checksum input from checksum value fields without an algorithm hint.
    pub(super) fn value_fields(
        crc32: Option<String>,
        crc32c: Option<String>,
        crc64nvme: Option<String>,
        sha1: Option<String>,
        sha256: Option<String>,
    ) -> Self {
        Self {
            crc32,
            crc32c,
            crc64nvme,
            sha1,
            sha256,
            ..Self::default()
        }
    }

    /// Returns the explicitly requested algorithm or infers it from one value field.
    fn algorithm_kind(&self) -> S3Result<Option<ChecksumAlgorithmKind>> {
        if self.algorithm.is_some() {
            return checksum_algorithm_from_request(self.algorithm.clone());
        }

        let mut inferred = [
            self.crc32.as_ref().map(|_| ChecksumAlgorithmKind::Crc32),
            self.crc32c.as_ref().map(|_| ChecksumAlgorithmKind::Crc32c),
            self.crc64nvme
                .as_ref()
                .map(|_| ChecksumAlgorithmKind::Crc64Nvme),
            self.sha1.as_ref().map(|_| ChecksumAlgorithmKind::Sha1),
            self.sha256.as_ref().map(|_| ChecksumAlgorithmKind::Sha256),
        ]
        .into_iter()
        .flatten();

        let first = inferred.next();
        if inferred.next().is_some() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "multiple checksum values were supplied",
            ));
        }
        Ok(first)
    }

    /// Returns the request-provided checksum value for the selected algorithm.
    fn requested_for(&self, kind: ChecksumAlgorithmKind) -> Option<&str> {
        match kind {
            ChecksumAlgorithmKind::Crc32 => self.crc32.as_deref(),
            ChecksumAlgorithmKind::Crc32c => self.crc32c.as_deref(),
            ChecksumAlgorithmKind::Crc64Nvme => self.crc64nvme.as_deref(),
            ChecksumAlgorithmKind::Sha1 => self.sha1.as_deref(),
            ChecksumAlgorithmKind::Sha256 => self.sha256.as_deref(),
        }
    }

    /// Converts completion checksum fields into a typed request checksum.
    pub(super) fn into_completion_checksum(self) -> S3Result<MultipartCompletionChecksum> {
        let algorithm = self.algorithm_kind()?;
        let checksum_type = checksum_type_name(self.checksum_type.clone())?;
        let Some(algorithm) = algorithm else {
            return Ok(MultipartCompletionChecksum {
                algorithm: None,
                checksum_type,
                value: None,
            });
        };

        Ok(MultipartCompletionChecksum {
            algorithm: Some(algorithm),
            checksum_type,
            value: self.requested_for(algorithm).map(str::to_string),
        })
    }

    /// Converts checksum value fields into a part-level checksum.
    pub(super) fn into_part_checksum(self) -> S3Result<Option<PartChecksum>> {
        let Some(algorithm) = self.algorithm_kind()? else {
            return Ok(None);
        };
        let Some(value) = self.requested_for(algorithm) else {
            return Ok(None);
        };
        Ok(Some(PartChecksum::new(algorithm, value.to_string())))
    }
}

/// Converts an S3 checksum algorithm DTO into the internal algorithm kind.
pub(super) fn checksum_algorithm_from_request(
    algorithm: Option<ChecksumAlgorithm>,
) -> S3Result<Option<ChecksumAlgorithmKind>> {
    let Some(algorithm) = algorithm else {
        return Ok(None);
    };
    let kind = checksum_algorithm_kind(&algorithm).ok_or_else(|| {
        s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "unsupported checksum algorithm",
        )
    })?;
    Ok(Some(kind))
}

/// Converts an S3 checksum algorithm name into its storage kind.
fn checksum_algorithm_kind(algorithm: &ChecksumAlgorithm) -> Option<ChecksumAlgorithmKind> {
    let kind = match algorithm.as_str() {
        ChecksumAlgorithm::CRC32 => ChecksumAlgorithmKind::Crc32,
        ChecksumAlgorithm::CRC32C => ChecksumAlgorithmKind::Crc32c,
        ChecksumAlgorithm::CRC64NVME => ChecksumAlgorithmKind::Crc64Nvme,
        ChecksumAlgorithm::SHA1 => ChecksumAlgorithmKind::Sha1,
        ChecksumAlgorithm::SHA256 => ChecksumAlgorithmKind::Sha256,
        _ => return None,
    };
    Some(kind)
}

/// Converts a storage checksum kind into an S3 checksum algorithm DTO.
fn checksum_algorithm_dto(kind: ChecksumAlgorithmKind) -> ChecksumAlgorithm {
    let value = match kind {
        ChecksumAlgorithmKind::Crc32 => ChecksumAlgorithm::CRC32,
        ChecksumAlgorithmKind::Crc32c => ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithmKind::Crc64Nvme => ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithmKind::Sha1 => ChecksumAlgorithm::SHA1,
        ChecksumAlgorithmKind::Sha256 => ChecksumAlgorithm::SHA256,
    };
    ChecksumAlgorithm::from_static(value)
}

/// Converts an optional S3 checksum type DTO into its internal kind.
pub(super) fn checksum_type_name(
    checksum_type: Option<ChecksumType>,
) -> S3Result<Option<ChecksumTypeKind>> {
    let Some(checksum_type) = checksum_type else {
        return Ok(None);
    };
    checksum_type_kind(&checksum_type).map(Some).ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidArgument, "unsupported checksum type")
    })
}

/// Converts an S3 checksum type name into its storage kind.
fn checksum_type_kind(checksum_type: &ChecksumType) -> Option<ChecksumTypeKind> {
    let kind = match checksum_type.as_str() {
        ChecksumType::COMPOSITE => ChecksumTypeKind::Composite,
        ChecksumType::FULL_OBJECT => ChecksumTypeKind::FullObject,
        _ => return None,
    };
    Some(kind)
}

/// Converts a storage checksum type into an S3 checksum type DTO.
fn checksum_type_dto(kind: ChecksumTypeKind) -> ChecksumType {
    let value = match kind {
        ChecksumTypeKind::Composite => ChecksumType::COMPOSITE,
        ChecksumTypeKind::FullObject => ChecksumType::FULL_OBJECT,
    };
    ChecksumType::from_static(value)
}

/// Returns S3's default multipart checksum type for an initiated algorithm.
pub(super) fn multipart_checksum_type(kind: ChecksumAlgorithmKind) -> ChecksumTypeKind {
    match kind {
        ChecksumAlgorithmKind::Crc64Nvme => ChecksumTypeKind::FullObject,
        ChecksumAlgorithmKind::Crc32
        | ChecksumAlgorithmKind::Crc32c
        | ChecksumAlgorithmKind::Sha1
        | ChecksumAlgorithmKind::Sha256 => ChecksumTypeKind::Composite,
    }
}

/// Builds checksum configuration for an initiated multipart upload.
pub(super) fn multipart_checksum_config(
    algorithm: Option<ChecksumAlgorithm>,
    checksum_type: Option<ChecksumType>,
) -> S3Result<Option<MultipartChecksumConfig>> {
    let checksum_kind = checksum_algorithm_from_request(algorithm)?;
    let checksum_type = checksum_type_name(checksum_type)?;
    let Some(algorithm) = checksum_kind else {
        if checksum_type.is_some() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "checksum type requires checksum algorithm",
            ));
        }
        return Ok(None);
    };
    Ok(Some(MultipartChecksumConfig::new(
        algorithm,
        checksum_type.unwrap_or_else(|| multipart_checksum_type(algorithm)),
    )))
}

/// Returns whether a request asked checksum fields to be included in read responses.
pub(super) fn checksum_mode_enabled(checksum_mode: Option<&ChecksumMode>) -> bool {
    checksum_mode.is_some_and(|mode| mode.as_str() == ChecksumMode::ENABLED)
}

/// Verifies the optional `Content-MD5` header against the computed body digest.
pub(super) fn validate_content_md5(
    header_present: bool,
    content_md5: Option<&str>,
    computed_md5: &[u8],
) -> S3Result<()> {
    let Some(content_md5) = content_md5 else {
        if header_present {
            return Err(s3_error(S3ErrorCode::InvalidDigest));
        }
        return Ok(());
    };

    let decoded = decode_base64(content_md5).ok_or_else(|| s3_error(S3ErrorCode::InvalidDigest))?;
    if decoded.len() != 16 {
        return Err(s3_error(S3ErrorCode::InvalidDigest));
    }
    if decoded != computed_md5 {
        return Err(s3_error(S3ErrorCode::BadDigest));
    }

    Ok(())
}

/// Creates an accumulator for an object payload checksum.
pub(super) fn payload_checksum_accumulator(
    input: &ChecksumInput,
    policy: ChecksumPolicy,
) -> S3Result<ChecksumAccumulator> {
    let explicit_kind = input.algorithm_kind()?;
    Ok(ChecksumAccumulator::new(
        explicit_kind.unwrap_or_else(|| policy.default_kind()),
    ))
}

/// Finalizes and validates an object payload checksum accumulator.
pub(super) fn finish_payload_checksum(
    input: &ChecksumInput,
    accumulator: ChecksumAccumulator,
    checksum_type: ChecksumTypeKind,
) -> S3Result<StoredChecksum> {
    let checksum = accumulator
        .finish_stored(checksum_type)
        .ok_or(SqliteError::ChecksumMismatch)
        .map_err(s3_error_from_sqlite)?;
    validate_stored_checksum(input, checksum)
}

/// Creates an accumulator for an explicitly requested part checksum.
pub(super) fn part_checksum_accumulator(
    input: &ChecksumInput,
) -> S3Result<Option<ChecksumAccumulator>> {
    let Some(kind) = input.algorithm_kind()? else {
        return Ok(None);
    };
    Ok(Some(ChecksumAccumulator::new(kind)))
}

/// Finalizes and validates an explicitly requested part checksum accumulator.
pub(super) fn finish_part_checksum(
    input: &ChecksumInput,
    accumulator: Option<ChecksumAccumulator>,
) -> S3Result<Option<PartChecksum>> {
    let Some(accumulator) = accumulator else {
        return Ok(None);
    };
    let kind = accumulator.algorithm();
    let checksum = accumulator.finish_part();
    if let Some(requested) = input.requested_for(kind)
        && checksum.value_for(kind) != Some(requested)
    {
        return Err(s3_error(S3ErrorCode::BadDigest));
    }
    Ok(Some(checksum))
}

/// Validates a computed object checksum against request-provided checksum text.
fn validate_stored_checksum(
    input: &ChecksumInput,
    checksum: StoredChecksum,
) -> S3Result<StoredChecksum> {
    let kind = checksum.algorithm();
    if let Some(requested) = input.requested_for(kind)
        && checksum.value_for(kind) != Some(requested)
    {
        return Err(s3_error(S3ErrorCode::BadDigest));
    }

    Ok(checksum)
}

/// Creates the default completed-object checksum configuration.
pub(super) fn default_crc64nvme_full_object_checksum() -> MultipartChecksumConfig {
    MultipartChecksumConfig::new(
        ChecksumAlgorithmKind::Crc64Nvme,
        ChecksumTypeKind::FullObject,
    )
}

/// Computes full-object checksum metadata for an object write.
pub(super) fn object_write_checksum_from_value(
    kind: ChecksumAlgorithmKind,
    object: &KeyValue,
) -> SqliteResult<StoredChecksum> {
    let value = object.value.as_deref().ok_or_else(|| {
        SqliteError::Configuration("object metadata has no stored value".to_string())
    })?;
    let mut accumulator = ChecksumAccumulator::new(kind);
    accumulator.update(value);
    accumulator
        .finish_stored(ChecksumTypeKind::FullObject)
        .ok_or(SqliteError::ChecksumMismatch)
}

/// Computes per-part checksum metadata for `UploadPartCopy`.
pub(super) fn configured_part_checksum(
    upload_checksum: Option<MultipartChecksumConfig>,
    value: &[u8],
) -> Option<PartChecksum> {
    upload_checksum.map(|checksum| {
        let mut accumulator = ChecksumAccumulator::new(checksum.algorithm);
        accumulator.update(value);
        accumulator.finish_part()
    })
}

/// Converts stored checksum algorithm metadata into an S3 response DTO.
pub(super) fn checksum_algorithm_output<'a>(
    checksum: impl Into<ChecksumView<'a>>,
) -> Option<ChecksumAlgorithm> {
    checksum.into().algorithm().map(checksum_algorithm_dto)
}

/// Converts stored checksum type metadata into an S3 response DTO.
pub(super) fn checksum_type_output<'a>(
    checksum: impl Into<ChecksumView<'a>>,
) -> Option<ChecksumType> {
    checksum.into().checksum_type().map(checksum_type_dto)
}

/// Returns a checksum response field for an algorithm.
fn checksum_field(checksum: ChecksumView<'_>, kind: ChecksumAlgorithmKind) -> Option<String> {
    checksum.value_for(kind).map(str::to_string)
}

/// Returns the `CRC32` checksum response field.
pub(super) fn checksum_crc32<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Crc32)
}

/// Returns the `CRC32C` checksum response field.
pub(super) fn checksum_crc32c<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Crc32c)
}

/// Returns the `CRC64NVME` checksum response field.
pub(super) fn checksum_crc64nvme<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Crc64Nvme)
}

/// Returns the `SHA1` checksum response field.
pub(super) fn checksum_sha1<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Sha1)
}

/// Returns the `SHA256` checksum response field.
pub(super) fn checksum_sha256<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Sha256)
}

/// Converts stored checksum metadata into the nested `GetObjectAttributes` checksum DTO.
pub(super) fn checksum_struct<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<S3Checksum> {
    let checksum = checksum.into();
    checksum.has_value().then(|| S3Checksum {
        checksum_crc32: checksum_crc32(checksum),
        checksum_crc32c: checksum_crc32c(checksum),
        checksum_crc64nvme: checksum_crc64nvme(checksum),
        checksum_sha1: checksum_sha1(checksum),
        checksum_sha256: checksum_sha256(checksum),
        checksum_type: checksum_type_output(checksum),
    })
}

/// Selects the checksum metadata that should be returned for a whole object or part read.
pub(super) fn object_checksum_for_part(
    object_checksum: Option<&StoredChecksum>,
    part: Option<&CompletedObjectPart>,
) -> Option<StoredChecksum> {
    let Some(part) = part else {
        return object_checksum.cloned();
    };
    let part_checksum = part.checksum.as_ref()?;
    let object_checksum = object_checksum?;
    StoredChecksum::new(
        part_checksum.algorithm(),
        object_checksum.checksum_type(),
        part_checksum.as_str().to_string(),
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};

    use super::{ChecksumAlgorithmKind, encode_base64};

    /// Computes this checksum over a byte slice and base64-encodes the digest.
    #[must_use]
    pub(crate) fn checksum_value(algorithm: ChecksumAlgorithmKind, value: &[u8]) -> String {
        match algorithm {
            ChecksumAlgorithmKind::Crc32 => encode_base64(Crc32::checksum(value).as_ref()),
            ChecksumAlgorithmKind::Crc32c => encode_base64(Crc32c::checksum(value).as_ref()),
            ChecksumAlgorithmKind::Crc64Nvme => encode_base64(Crc64Nvme::checksum(value).as_ref()),
            ChecksumAlgorithmKind::Sha1 => encode_base64(Sha1::checksum(value).as_ref()),
            ChecksumAlgorithmKind::Sha256 => encode_base64(Sha256::checksum(value).as_ref()),
        }
    }
}

#[cfg(test)]
mod model_tests {
    use super::{
        ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, ChecksumView,
        CompositeChecksum, FullObjectChecksum, MultipartChecksumConfig,
        MultipartCompletionChecksum, PartChecksum, StoredChecksum, decode_base64, decode_md5_hex,
        encode_base64, encode_hex, test_support,
    };

    #[test]
    fn part_checksum_variant_carries_algorithm() {
        let value = PartChecksum::new(ChecksumAlgorithmKind::Sha256, "sha".to_string());
        assert_eq!(value.algorithm(), ChecksumAlgorithmKind::Sha256);
        assert_eq!(value.as_str(), "sha");
        assert_eq!(value.value_for(ChecksumAlgorithmKind::Sha256), Some("sha"));
        assert_eq!(value.value_for(ChecksumAlgorithmKind::Sha1), None);
    }

    #[test]
    fn part_checksums_cover_each_algorithm_variant() {
        for algorithm in [
            ChecksumAlgorithmKind::Crc32,
            ChecksumAlgorithmKind::Crc32c,
            ChecksumAlgorithmKind::Crc64Nvme,
            ChecksumAlgorithmKind::Sha1,
            ChecksumAlgorithmKind::Sha256,
        ] {
            let checksum = PartChecksum::new(algorithm, "value".to_string());
            assert_eq!(checksum.algorithm(), algorithm);
            assert_eq!(checksum.as_str(), "value");
            assert_eq!(checksum.value_for(algorithm), Some("value"));
        }
    }

    #[test]
    fn full_object_checksums_cover_each_algorithm_variant() {
        for algorithm in [
            ChecksumAlgorithmKind::Crc32,
            ChecksumAlgorithmKind::Crc32c,
            ChecksumAlgorithmKind::Crc64Nvme,
            ChecksumAlgorithmKind::Sha1,
            ChecksumAlgorithmKind::Sha256,
        ] {
            let checksum = FullObjectChecksum::new(algorithm, "value".to_string());
            assert_eq!(checksum.algorithm(), algorithm);
            assert_eq!(checksum.as_str(), "value");
        }
    }

    #[test]
    fn composite_checksums_cover_supported_algorithm_variants() {
        for algorithm in [
            ChecksumAlgorithmKind::Crc32,
            ChecksumAlgorithmKind::Crc32c,
            ChecksumAlgorithmKind::Sha1,
            ChecksumAlgorithmKind::Sha256,
        ] {
            let checksum = CompositeChecksum::new(algorithm, "value".to_string());
            let Some(checksum) = checksum else {
                unreachable!("algorithm is supported by composite checksums");
            };
            assert_eq!(checksum.algorithm(), algorithm);
            assert_eq!(checksum.as_str(), "value");
        }
        assert_eq!(
            CompositeChecksum::new(ChecksumAlgorithmKind::Crc64Nvme, "crc".to_string()),
            None
        );
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
    fn multipart_completion_checksum_keeps_type_and_value_separate() {
        let checksum = MultipartCompletionChecksum {
            algorithm: Some(ChecksumAlgorithmKind::Crc32),
            checksum_type: None,
            value: Some("WgDhBQ==".to_string()),
        };

        assert_eq!(checksum.algorithm(), Some(ChecksumAlgorithmKind::Crc32));
        assert_eq!(checksum.checksum_type(), None);
        assert_eq!(
            checksum.value_for(ChecksumAlgorithmKind::Crc32),
            Some("WgDhBQ==")
        );
        assert_eq!(checksum.value_for(ChecksumAlgorithmKind::Crc32c), None);
        assert!(checksum.has_value());
    }

    #[test]
    fn checksum_views_expose_only_available_metadata() {
        let object = StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("object".to_string()));
        let part = PartChecksum::Sha1("part".to_string());
        let config = MultipartChecksumConfig::new(
            ChecksumAlgorithmKind::Crc32c,
            ChecksumTypeKind::Composite,
        );

        let object_view = ChecksumView::from(&object);
        assert_eq!(
            object_view.algorithm(),
            Some(ChecksumAlgorithmKind::Crc64Nvme)
        );
        assert_eq!(
            object_view.checksum_type(),
            Some(ChecksumTypeKind::FullObject)
        );
        assert_eq!(
            object_view.value_for(ChecksumAlgorithmKind::Crc64Nvme),
            Some("object")
        );
        assert!(object_view.has_value());

        let part_view = ChecksumView::from(&part);
        assert_eq!(part_view.algorithm(), Some(ChecksumAlgorithmKind::Sha1));
        assert_eq!(part_view.checksum_type(), None);
        assert_eq!(
            part_view.value_for(ChecksumAlgorithmKind::Sha1),
            Some("part")
        );
        assert!(part_view.has_value());

        let config_view = ChecksumView::from(config);
        assert_eq!(config_view.algorithm(), Some(ChecksumAlgorithmKind::Crc32c));
        assert_eq!(
            config_view.checksum_type(),
            Some(ChecksumTypeKind::Composite)
        );
        assert_eq!(config_view.value_for(ChecksumAlgorithmKind::Crc32c), None);
        assert!(!config_view.has_value());

        let no_object: Option<StoredChecksum> = None;
        let no_part: Option<PartChecksum> = None;
        let no_config: Option<MultipartChecksumConfig> = None;
        assert_eq!(ChecksumView::from(&no_object).algorithm(), None);
        assert_eq!(ChecksumView::from(&no_part).algorithm(), None);
        assert_eq!(ChecksumView::from(&no_config).algorithm(), None);
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
        for value in ["", "abc", "!!!!", "YW=J", "YR==", "YWJ=", "YQ==YQ=="] {
            assert_eq!(decode_base64(value), None, "{value}");
        }
    }

    #[test]
    fn computes_base64_checksum_values() {
        let value = b"test data";
        assert_eq!(
            test_support::checksum_value(ChecksumAlgorithmKind::Crc32, value),
            "0wiusg=="
        );
        assert_eq!(
            test_support::checksum_value(ChecksumAlgorithmKind::Crc32c, value),
            "M3m0yg=="
        );
        assert_eq!(
            test_support::checksum_value(ChecksumAlgorithmKind::Crc64Nvme, value),
            "rsrzr5yYqFU="
        );
        assert_eq!(
            test_support::checksum_value(ChecksumAlgorithmKind::Sha1, value),
            "9I3YU4IIYIFsddVND1hNyGMyenw="
        );
        assert_eq!(
            test_support::checksum_value(ChecksumAlgorithmKind::Sha256, value),
            "kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk="
        );
    }

    #[test]
    fn accumulator_matches_one_shot_checksum_values() {
        for algorithm in [
            ChecksumAlgorithmKind::Crc32,
            ChecksumAlgorithmKind::Crc32c,
            ChecksumAlgorithmKind::Crc64Nvme,
            ChecksumAlgorithmKind::Sha1,
            ChecksumAlgorithmKind::Sha256,
        ] {
            let mut accumulator = ChecksumAccumulator::new(algorithm);
            accumulator.update(b"test ");
            accumulator.update(b"data");
            assert_eq!(
                accumulator.finish_value(),
                test_support::checksum_value(algorithm, b"test data")
            );
        }
    }

    #[test]
    fn accumulator_finishes_part_checksum() {
        let mut accumulator = ChecksumAccumulator::new(ChecksumAlgorithmKind::Sha256);
        accumulator.update(b"test ");
        accumulator.update(b"data");
        assert_eq!(
            accumulator.finish_part(),
            PartChecksum::Sha256("kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk=".to_string())
        );
    }

    #[test]
    fn accumulator_finishes_stored_full_object_checksum() {
        let mut accumulator = ChecksumAccumulator::new(ChecksumAlgorithmKind::Crc64Nvme);
        accumulator.update(b"test ");
        accumulator.update(b"data");
        assert_eq!(
            accumulator.finish_stored(ChecksumTypeKind::FullObject),
            Some(StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                "rsrzr5yYqFU=".to_string()
            )))
        );
    }

    #[test]
    fn accumulator_finishes_composite_checksum() {
        let mut accumulator = ChecksumAccumulator::new(ChecksumAlgorithmKind::Sha256);
        accumulator.update(b"part");
        accumulator.update(b"digest");
        assert_eq!(
            accumulator.finish_composite(2),
            Some(StoredChecksum::Composite(CompositeChecksum::Sha256(
                format!(
                    "{}-2",
                    test_support::checksum_value(ChecksumAlgorithmKind::Sha256, b"partdigest")
                )
            )))
        );

        let mut crc64 = ChecksumAccumulator::new(ChecksumAlgorithmKind::Crc64Nvme);
        crc64.update(b"partdigest");
        assert_eq!(crc64.finish_composite(1), None);
    }
}

#[cfg(test)]
mod tests {
    use s3s::{
        S3ErrorCode,
        dto::{ChecksumAlgorithm, ChecksumType},
    };

    use crate::sqlite::{CompletedObjectPart, KeyMetadata, KeyValue};

    use super::{
        ChecksumAlgorithmKind, ChecksumInput, ChecksumPolicy, ChecksumTypeKind, FullObjectChecksum,
        MultipartChecksumConfig, MultipartCompletionChecksum, PartChecksum, StoredChecksum,
        checksum_algorithm_dto, checksum_algorithm_from_request, checksum_algorithm_kind,
        checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_mode_enabled,
        checksum_struct, checksum_type_dto, checksum_type_kind, checksum_type_name,
        configured_part_checksum, default_crc64nvme_full_object_checksum, finish_part_checksum,
        finish_payload_checksum, multipart_checksum_config, multipart_checksum_type,
        object_checksum_for_part, object_write_checksum_from_value, part_checksum_accumulator,
        payload_checksum_accumulator, validate_content_md5,
    };

    fn assert_error_code<T>(result: s3s::S3Result<T>, code: &S3ErrorCode) {
        let err = result.err();
        assert!(
            err.as_ref().is_some_and(|err| err.code() == code),
            "expected {code:?}"
        );
    }

    #[test]
    fn parses_request_checksum_algorithm() {
        assert_eq!(
            checksum_algorithm_from_request(Some(ChecksumAlgorithm::from_static(
                ChecksumAlgorithm::CRC64NVME
            )))
            .ok()
            .flatten(),
            Some(ChecksumAlgorithmKind::Crc64Nvme)
        );
        assert_error_code(
            checksum_algorithm_from_request(Some(ChecksumAlgorithm::from("UNKNOWN".to_string()))),
            &S3ErrorCode::InvalidArgument,
        );
    }

    #[test]
    fn maps_checksum_algorithms_to_s3_names() {
        for (name, kind) in [
            (ChecksumAlgorithm::CRC32, ChecksumAlgorithmKind::Crc32),
            (ChecksumAlgorithm::CRC32C, ChecksumAlgorithmKind::Crc32c),
            (
                ChecksumAlgorithm::CRC64NVME,
                ChecksumAlgorithmKind::Crc64Nvme,
            ),
            (ChecksumAlgorithm::SHA1, ChecksumAlgorithmKind::Sha1),
            (ChecksumAlgorithm::SHA256, ChecksumAlgorithmKind::Sha256),
        ] {
            let algorithm = ChecksumAlgorithm::from_static(name);
            assert_eq!(checksum_algorithm_kind(&algorithm), Some(kind));
            assert_eq!(checksum_algorithm_dto(kind).as_str(), name);
        }
        assert_eq!(
            checksum_algorithm_kind(&ChecksumAlgorithm::from("MD5".to_string())),
            None
        );
    }

    #[test]
    fn validates_checksum_type_names() {
        assert_eq!(
            checksum_type_name(Some(s3s::dto::ChecksumType::from_static(
                s3s::dto::ChecksumType::FULL_OBJECT
            )))
            .ok()
            .flatten(),
            Some(ChecksumTypeKind::FullObject)
        );
        assert_error_code(
            checksum_type_name(Some(s3s::dto::ChecksumType::from("BOGUS".to_string()))),
            &S3ErrorCode::InvalidArgument,
        );
    }

    #[test]
    fn maps_checksum_types_to_s3_names() {
        for (name, kind) in [
            (ChecksumType::COMPOSITE, ChecksumTypeKind::Composite),
            (ChecksumType::FULL_OBJECT, ChecksumTypeKind::FullObject),
        ] {
            let checksum_type = ChecksumType::from_static(name);
            assert_eq!(checksum_type_kind(&checksum_type), Some(kind));
            assert_eq!(checksum_type_dto(kind).as_str(), name);
        }
        assert_eq!(
            checksum_type_kind(&ChecksumType::from("LINEAR".to_string())),
            None
        );
    }

    #[test]
    fn maps_multipart_default_checksum_type() {
        assert_eq!(
            multipart_checksum_type(ChecksumAlgorithmKind::Crc64Nvme),
            ChecksumTypeKind::FullObject
        );
        assert_eq!(
            multipart_checksum_type(ChecksumAlgorithmKind::Sha256),
            ChecksumTypeKind::Composite
        );
    }

    #[test]
    fn builds_multipart_checksum_config() {
        let config = multipart_checksum_config(
            Some(ChecksumAlgorithm::from_static(ChecksumAlgorithm::SHA256)),
            None,
        )
        .ok()
        .flatten();
        let Some(config) = config else {
            unreachable!("SHA256 multipart checksum config should be valid");
        };
        assert_eq!(config.algorithm, ChecksumAlgorithmKind::Sha256);
        assert_eq!(config.checksum_type, ChecksumTypeKind::Composite);

        let default = default_crc64nvme_full_object_checksum();
        assert_eq!(default.algorithm, ChecksumAlgorithmKind::Crc64Nvme);
        assert_eq!(default.checksum_type, ChecksumTypeKind::FullObject);
    }

    #[test]
    fn rejects_checksum_type_without_algorithm() {
        assert_error_code(
            multipart_checksum_config(
                None,
                Some(ChecksumType::from_static(ChecksumType::FULL_OBJECT)),
            ),
            &S3ErrorCode::InvalidArgument,
        );
    }

    #[test]
    fn rejects_ambiguous_checksum_values() {
        let input = ChecksumInput {
            crc32: Some("crc32".to_string()),
            sha1: Some("sha1".to_string()),
            ..ChecksumInput::default()
        };
        assert_error_code(input.into_part_checksum(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn completion_checksum_value_does_not_imply_checksum_type() {
        let checksum = (ChecksumInput {
            crc32: Some("WgDhBQ==".to_string()),
            ..ChecksumInput::default()
        })
        .into_completion_checksum()
        .ok();

        assert_eq!(
            checksum
                .as_ref()
                .and_then(MultipartCompletionChecksum::algorithm),
            Some(ChecksumAlgorithmKind::Crc32)
        );
        assert_eq!(
            checksum
                .as_ref()
                .and_then(MultipartCompletionChecksum::checksum_type),
            None
        );
        assert_eq!(
            checksum
                .as_ref()
                .and_then(|checksum| checksum.value_for(ChecksumAlgorithmKind::Crc32)),
            Some("WgDhBQ==")
        );
    }

    #[test]
    fn completion_checksum_preserves_explicit_checksum_type() {
        let checksum = (ChecksumInput {
            checksum_type: Some(ChecksumType::from_static(ChecksumType::COMPOSITE)),
            crc32c: Some("xU+Krw==".to_string()),
            ..ChecksumInput::default()
        })
        .into_completion_checksum()
        .ok();

        assert_eq!(
            checksum
                .as_ref()
                .and_then(MultipartCompletionChecksum::algorithm),
            Some(ChecksumAlgorithmKind::Crc32c)
        );
        assert_eq!(
            checksum
                .as_ref()
                .and_then(MultipartCompletionChecksum::checksum_type),
            Some(ChecksumTypeKind::Composite)
        );
        assert_eq!(
            checksum
                .as_ref()
                .and_then(|checksum| checksum.value_for(ChecksumAlgorithmKind::Crc32c)),
            Some("xU+Krw==")
        );
    }

    #[test]
    fn empty_checksum_input_has_no_part_or_completion_value() {
        assert_eq!(
            ChecksumInput::default().into_part_checksum().ok().flatten(),
            None
        );
        let completion = ChecksumInput::default().into_completion_checksum().ok();
        assert_eq!(
            completion
                .as_ref()
                .and_then(MultipartCompletionChecksum::algorithm),
            None
        );
        assert_eq!(
            completion
                .as_ref()
                .and_then(MultipartCompletionChecksum::checksum_type),
            None
        );
    }

    #[test]
    fn detects_enabled_checksum_mode() {
        assert!(checksum_mode_enabled(Some(
            &s3s::dto::ChecksumMode::from_static(s3s::dto::ChecksumMode::ENABLED)
        )));
        assert!(!checksum_mode_enabled(None));
    }

    #[test]
    fn rejects_malformed_content_md5() {
        assert_error_code(
            validate_content_md5(true, None, &[0; 16]),
            &S3ErrorCode::InvalidDigest,
        );
        assert_error_code(
            validate_content_md5(true, Some("YWJyYWNhZGFicmE="), &[0; 16]),
            &S3ErrorCode::InvalidDigest,
        );
        assert_error_code(
            validate_content_md5(true, Some("not base64"), &[0; 16]),
            &S3ErrorCode::InvalidDigest,
        );
    }

    #[test]
    fn distinguishes_wrong_content_md5() {
        assert_error_code(
            validate_content_md5(true, Some("rL0Y20xC+Fzt72VPzMSk2A=="), &[0; 16]),
            &S3ErrorCode::BadDigest,
        );
    }

    #[test]
    fn computes_default_payload_checksum() {
        let input = ChecksumInput::default();
        let accumulator =
            payload_checksum_accumulator(&input, ChecksumPolicy::DefaultCrc64NvmeFullObject);
        assert!(accumulator.is_ok());
        let mut accumulator = accumulator.ok();
        assert!(accumulator.is_some());
        if let Some(accumulator) = accumulator.as_mut() {
            accumulator.update(b"test data");
        }
        let checksum = accumulator.and_then(|accumulator| {
            finish_payload_checksum(&input, accumulator, ChecksumTypeKind::FullObject).ok()
        });
        assert_eq!(
            checksum,
            Some(StoredChecksum::Full(FullObjectChecksum::Crc64Nvme(
                "rsrzr5yYqFU=".to_string()
            )))
        );
    }

    #[test]
    fn rejects_mismatched_payload_checksum() {
        let input = ChecksumInput {
            algorithm: Some(ChecksumAlgorithm::from_static(ChecksumAlgorithm::SHA256)),
            sha256: Some("wrong".to_string()),
            ..ChecksumInput::default()
        };
        let mut accumulator = match payload_checksum_accumulator(
            &input,
            ChecksumPolicy::DefaultCrc64NvmeFullObject,
        ) {
            Ok(accumulator) => accumulator,
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        accumulator.update(b"test data");
        assert_error_code(
            finish_payload_checksum(&input, accumulator, ChecksumTypeKind::FullObject),
            &S3ErrorCode::BadDigest,
        );
    }

    #[test]
    fn computes_part_checksum_from_explicit_input() {
        let input = ChecksumInput {
            algorithm: Some(ChecksumAlgorithm::from_static(ChecksumAlgorithm::SHA256)),
            ..ChecksumInput::default()
        };
        let mut accumulator = part_checksum_accumulator(&input).ok().flatten();
        assert!(accumulator.is_some());
        if let Some(accumulator) = accumulator.as_mut() {
            accumulator.update(b"test data");
        }
        let checksum = finish_part_checksum(&input, accumulator).ok().flatten();
        assert_eq!(
            checksum,
            Some(PartChecksum::Sha256(
                "kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk=".to_string()
            ))
        );
    }

    #[test]
    fn computes_sha1_part_checksum_from_value_field() {
        let input = ChecksumInput {
            sha1: Some("9I3YU4IIYIFsddVND1hNyGMyenw=".to_string()),
            ..ChecksumInput::default()
        };
        let mut accumulator = part_checksum_accumulator(&input).ok().flatten();
        assert!(accumulator.is_some());
        if let Some(accumulator) = accumulator.as_mut() {
            accumulator.update(b"test data");
        }
        let checksum = finish_part_checksum(&input, accumulator).ok().flatten();
        assert_eq!(
            checksum,
            Some(PartChecksum::Sha1(
                "9I3YU4IIYIFsddVND1hNyGMyenw=".to_string()
            ))
        );
    }

    #[test]
    fn computes_configured_part_checksum_when_upload_requested_one() {
        let config = multipart_checksum_config(
            Some(ChecksumAlgorithm::from_static(ChecksumAlgorithm::SHA256)),
            None,
        )
        .ok()
        .flatten();
        let checksum = configured_part_checksum(config, b"test data");
        assert_eq!(
            checksum,
            Some(PartChecksum::Sha256(
                "kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk=".to_string()
            ))
        );
    }

    #[test]
    fn object_write_checksum_requires_loaded_value() {
        let object = KeyValue {
            key: "key".to_string(),
            value: None,
            key_metadata: KeyMetadata {
                version_id: "version".to_string(),
                size: 4,
                metadata: None,
                content_type: None,
                content_encoding: None,
                cache_control: None,
                expires: None,
                last_modified: time::OffsetDateTime::UNIX_EPOCH,
                md5: Some("md5".to_string()),
                checksum: None,
                stored_sse: None,
                object_lock: crate::s3::StoredObjectLock::default(),
                is_latest: true,
                is_delete_marker: false,
            },
        };

        assert!(matches!(
            object_write_checksum_from_value(ChecksumAlgorithmKind::Crc64Nvme, &object),
            Err(crate::sqlite::SqliteError::Configuration(_))
        ));
    }

    #[test]
    fn response_checksum_fields_reflect_view_algorithm() {
        let checksum = StoredChecksum::Full(FullObjectChecksum::Crc32("crc32".to_string()));
        assert_eq!(checksum_crc32(&checksum), Some("crc32".to_string()));
        assert_eq!(checksum_crc32c(&checksum), None);
        assert_eq!(checksum_crc64nvme(&checksum), None);

        let config = MultipartChecksumConfig::new(
            ChecksumAlgorithmKind::Sha256,
            ChecksumTypeKind::Composite,
        );
        assert_eq!(checksum_struct(config), None);
    }

    #[test]
    fn suppresses_part_checksum_when_part_has_none() {
        let object_checksum =
            StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("rsrzr5yYqFU=".to_string()));
        let part = CompletedObjectPart {
            part_number: 1,
            start: 0,
            size: 9,
            checksum: None,
        };
        assert_eq!(
            object_checksum_for_part(Some(&object_checksum), Some(&part)),
            None
        );
    }
}
