//! Shared checksum and wire-encoding primitives.
//!
//! This module intentionally contains only storage/protocol-neutral helpers.
//! S3 request validation and response shaping live in `s3::checksum`; `SQLite` row
//! conversion and metadata storage live in `sqlite::checksum`.

use std::ops::Not;

use s3s::{
    crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256},
    dto::{ChecksumAlgorithm, ChecksumType},
};

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
            ChecksumAlgorithm::CRC32 => Self::Crc32,
            ChecksumAlgorithm::CRC32C => Self::Crc32c,
            ChecksumAlgorithm::CRC64NVME => Self::Crc64Nvme,
            ChecksumAlgorithm::SHA1 => Self::Sha1,
            ChecksumAlgorithm::SHA256 => Self::Sha256,
            _ => return None,
        };
        Some(kind)
    }

    /// Returns the S3 checksum algorithm name.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => ChecksumAlgorithm::CRC32,
            Self::Crc32c => ChecksumAlgorithm::CRC32C,
            Self::Crc64Nvme => ChecksumAlgorithm::CRC64NVME,
            Self::Sha1 => ChecksumAlgorithm::SHA1,
            Self::Sha256 => ChecksumAlgorithm::SHA256,
        }
    }

    /// Converts this checksum algorithm to the generated S3 DTO newtype.
    #[must_use]
    pub(crate) fn to_s3(self) -> ChecksumAlgorithm {
        ChecksumAlgorithm::from_static(self.as_str())
    }

    /// Computes this checksum over a byte slice and base64-encodes the digest.
    #[must_use]
    pub(crate) fn checksum_value(self, value: &[u8]) -> String {
        match self {
            Self::Crc32 => encode_base64(Crc32::checksum(value).as_ref()),
            Self::Crc32c => encode_base64(Crc32c::checksum(value).as_ref()),
            Self::Crc64Nvme => encode_base64(Crc64Nvme::checksum(value).as_ref()),
            Self::Sha1 => encode_base64(Sha1::checksum(value).as_ref()),
            Self::Sha256 => encode_base64(Sha256::checksum(value).as_ref()),
        }
    }
}

impl From<ChecksumAlgorithmKind> for ChecksumAlgorithm {
    fn from(kind: ChecksumAlgorithmKind) -> Self {
        kind.to_s3()
    }
}

impl TryFrom<&ChecksumAlgorithm> for ChecksumAlgorithmKind {
    type Error = ();

    fn try_from(algorithm: &ChecksumAlgorithm) -> Result<Self, Self::Error> {
        Self::from_name(algorithm.as_str()).ok_or(())
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
            ChecksumType::COMPOSITE => Self::Composite,
            ChecksumType::FULL_OBJECT => Self::FullObject,
            _ => return None,
        };
        Some(kind)
    }

    /// Returns the S3 checksum type name.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Composite => ChecksumType::COMPOSITE,
            Self::FullObject => ChecksumType::FULL_OBJECT,
        }
    }

    /// Converts this checksum type to the generated S3 DTO newtype.
    #[must_use]
    pub(crate) fn to_s3(self) -> ChecksumType {
        ChecksumType::from_static(self.as_str())
    }
}

impl From<ChecksumTypeKind> for ChecksumType {
    fn from(kind: ChecksumTypeKind) -> Self {
        kind.to_s3()
    }
}

impl TryFrom<&ChecksumType> for ChecksumTypeKind {
    type Error = ();

    fn try_from(checksum_type: &ChecksumType) -> Result<Self, Self::Error> {
        Self::from_name(checksum_type.as_str()).ok_or(())
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
    /// Requested checksum type, even when no final checksum value was supplied.
    pub(crate) checksum_type: Option<ChecksumTypeKind>,
    /// Client-supplied final checksum value.
    pub(crate) checksum: Option<StoredChecksum>,
}

impl MultipartCompletionChecksum {
    /// Returns the supplied checksum algorithm.
    #[must_use]
    pub(crate) fn algorithm(&self) -> Option<ChecksumAlgorithmKind> {
        self.checksum.as_ref().map(StoredChecksum::algorithm)
    }

    /// Returns the requested checksum type.
    #[must_use]
    pub(crate) fn checksum_type(&self) -> Option<ChecksumTypeKind> {
        self.checksum
            .as_ref()
            .map(StoredChecksum::checksum_type)
            .or(self.checksum_type)
    }

    /// Returns the checksum value for an algorithm when supplied.
    #[must_use]
    pub(crate) fn value_for(&self, algorithm: ChecksumAlgorithmKind) -> Option<&str> {
        self.checksum
            .as_ref()
            .and_then(|checksum| checksum.value_for(algorithm))
    }

    /// Returns whether the request supplied a checksum value.
    #[must_use]
    pub(crate) fn has_value(&self) -> bool {
        self.checksum.is_some()
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

#[cfg(test)]
mod tests {
    use s3s::dto::{ChecksumAlgorithm, ChecksumType};

    use super::{
        ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, CompositeChecksum,
        FullObjectChecksum, PartChecksum, StoredChecksum, decode_base64, decode_md5_hex,
        encode_base64, encode_hex,
    };

    #[test]
    fn maps_checksum_algorithm_names() {
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
            assert_eq!(ChecksumAlgorithmKind::from_name(name), Some(kind));
            assert_eq!(kind.as_str(), name);
            assert_eq!(
                ChecksumAlgorithmKind::try_from(&ChecksumAlgorithm::from_static(name)),
                Ok(kind)
            );
            assert_eq!(kind.to_s3().as_str(), name);
        }
        assert_eq!(ChecksumAlgorithmKind::from_name("MD5"), None);
    }

    #[test]
    fn maps_checksum_type_names() {
        for (name, kind) in [
            (ChecksumType::COMPOSITE, ChecksumTypeKind::Composite),
            (ChecksumType::FULL_OBJECT, ChecksumTypeKind::FullObject),
        ] {
            assert_eq!(ChecksumTypeKind::from_name(name), Some(kind));
            assert_eq!(kind.as_str(), name);
            assert_eq!(
                ChecksumTypeKind::try_from(&ChecksumType::from_static(name)),
                Ok(kind)
            );
            assert_eq!(kind.to_s3().as_str(), name);
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
            ChecksumAlgorithmKind::Crc32.checksum_value(value),
            "0wiusg=="
        );
        assert_eq!(
            ChecksumAlgorithmKind::Crc32c.checksum_value(value),
            "M3m0yg=="
        );
        assert_eq!(
            ChecksumAlgorithmKind::Crc64Nvme.checksum_value(value),
            "rsrzr5yYqFU="
        );
        assert_eq!(
            ChecksumAlgorithmKind::Sha1.checksum_value(value),
            "9I3YU4IIYIFsddVND1hNyGMyenw="
        );
        assert_eq!(
            ChecksumAlgorithmKind::Sha256.checksum_value(value),
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
                algorithm.checksum_value(b"test data")
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
                    ChecksumAlgorithmKind::Sha256.checksum_value(b"partdigest")
                )
            )))
        );

        let mut crc64 = ChecksumAccumulator::new(ChecksumAlgorithmKind::Crc64Nvme);
        crc64.update(b"partdigest");
        assert_eq!(crc64.finish_composite(1), None);
    }
}
