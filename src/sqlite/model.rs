//! Storage-owned entities shared between `SQLite` operations and S3 mapping.

use std::ops::Not;

use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};
use time::OffsetDateTime;

/// Compact database code for bucket default SSE-S3.
pub(crate) const SSE_S3_ALGORITHM_CODE: i64 = 1;

/// Normalized S3 bucket CORS rule persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredCorsRule {
    pub(crate) id: Option<String>,
    pub(crate) allowed_origins: Vec<String>,
    pub(crate) allowed_methods: Vec<String>,
    pub(crate) allowed_headers: Vec<String>,
    pub(crate) expose_headers: Vec<String>,
    pub(crate) max_age_seconds: Option<i32>,
}

/// Normalized S3 tag persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredTag {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Checksum algorithms supported by stored checksum metadata.
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

/// Checksum aggregation modes supported by stored object checksums.
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
    pub(crate) fn algorithm(&self) -> ChecksumAlgorithmKind {
        match self {
            Self::Crc32(_) => ChecksumAlgorithmKind::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithmKind::Crc32c,
            Self::Sha1(_) => ChecksumAlgorithmKind::Sha1,
            Self::Sha256(_) => ChecksumAlgorithmKind::Sha256,
        }
    }

    /// Returns the base64 checksum text.
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
    pub(crate) fn algorithm(&self) -> ChecksumAlgorithmKind {
        match self {
            Self::Full(checksum) => checksum.algorithm(),
            Self::Composite(checksum) => checksum.algorithm(),
        }
    }

    /// Returns the checksum aggregation type.
    pub(crate) fn checksum_type(&self) -> ChecksumTypeKind {
        match self {
            Self::Full(_) => ChecksumTypeKind::FullObject,
            Self::Composite(_) => ChecksumTypeKind::Composite,
        }
    }

    /// Returns the base64 checksum text.
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Full(checksum) => checksum.as_str(),
            Self::Composite(checksum) => checksum.as_str(),
        }
    }

    /// Returns the checksum value when this object checksum uses the algorithm.
    pub(crate) fn value_for(&self, algorithm: ChecksumAlgorithmKind) -> Option<&str> {
        (self.algorithm() == algorithm).then(|| self.as_str())
    }
}

/// Incremental checksum state for one stored checksum algorithm.
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

    /// Finalizes the accumulator as a base64 checksum value.
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
    pub(crate) fn algorithm(&self) -> Option<ChecksumAlgorithmKind> {
        self.algorithm
    }

    /// Returns the requested checksum type.
    pub(crate) fn checksum_type(&self) -> Option<ChecksumTypeKind> {
        self.checksum_type
    }

    /// Returns the checksum value for an algorithm when supplied.
    pub(crate) fn value_for(&self, algorithm: ChecksumAlgorithmKind) -> Option<&str> {
        (self.algorithm == Some(algorithm))
            .then_some(self.value.as_deref())
            .flatten()
    }

    /// Returns whether the request supplied a checksum value.
    pub(crate) fn has_value(&self) -> bool {
        self.value.is_some()
    }
}

/// Encodes bytes as lowercase hexadecimal text.
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

/// Stored object/upload envelope encryption metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredSse {
    pub(crate) nonce: Vec<u8>,
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) tag: Vec<u8>,
}

/// Normalized S3 bucket lifecycle rule persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLifecycleRule {
    pub(crate) id: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) filter: StoredLifecycleFilter,
    pub(crate) expiration: Option<StoredLifecycleExpiration>,
    pub(crate) noncurrent_expiration: Option<StoredNoncurrentVersionExpiration>,
    pub(crate) abort_incomplete_multipart_days: Option<i32>,
}

/// Normalized lifecycle filter persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StoredLifecycleFilter {
    Empty,
    LegacyPrefix(String),
    Prefix(String),
    Tag(StoredTag),
    SizeGreaterThan(i64),
    SizeLessThan(i64),
    And(StoredLifecycleAndFilter),
}

/// Normalized lifecycle `And` filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLifecycleAndFilter {
    pub(crate) prefix: Option<String>,
    pub(crate) tags: Vec<StoredTag>,
    pub(crate) object_size_greater_than: Option<i64>,
    pub(crate) object_size_less_than: Option<i64>,
}

/// Normalized current-version expiration action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLifecycleExpiration {
    pub(crate) days: Option<i32>,
    pub(crate) date: Option<OffsetDateTime>,
    pub(crate) expired_object_delete_marker: bool,
}

/// Normalized noncurrent-version expiration action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredNoncurrentVersionExpiration {
    pub(crate) noncurrent_days: i32,
    pub(crate) newer_noncurrent_versions: Option<i32>,
}

/// Lifecycle expiration selected for an object by storage rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObjectLifecycleExpiration {
    pub(crate) expires_at: OffsetDateTime,
    pub(crate) rule_id: String,
}

/// Returns whether a lifecycle rule matches the supplied object attributes.
#[must_use]
pub(crate) fn lifecycle_rule_matches_object(
    rule: &StoredLifecycleRule,
    key: &str,
    size: i64,
    tags: &[StoredTag],
) -> bool {
    lifecycle_filter_matches_object(&rule.filter, key, size, tags)
}

/// Returns whether a lifecycle filter matches the supplied object attributes.
fn lifecycle_filter_matches_object(
    filter: &StoredLifecycleFilter,
    key: &str,
    size: i64,
    tags: &[StoredTag],
) -> bool {
    match filter {
        StoredLifecycleFilter::Empty => true,
        StoredLifecycleFilter::LegacyPrefix(prefix) | StoredLifecycleFilter::Prefix(prefix) => {
            key.starts_with(prefix)
        }
        StoredLifecycleFilter::Tag(tag) => tags.iter().any(|stored| stored == tag),
        StoredLifecycleFilter::SizeGreaterThan(min) => size > *min,
        StoredLifecycleFilter::SizeLessThan(max) => size < *max,
        StoredLifecycleFilter::And(and) => {
            and.prefix
                .as_ref()
                .is_none_or(|prefix| key.starts_with(prefix))
                && and.object_size_greater_than.is_none_or(|min| size > min)
                && and.object_size_less_than.is_none_or(|max| size < max)
                && and
                    .tags
                    .iter()
                    .all(|tag| tags.iter().any(|stored| stored == tag))
        }
    }
}

/// Returns the eligibility time for a current-version expiration action.
#[must_use]
pub(crate) fn current_expiration_time(
    expiration: &StoredLifecycleExpiration,
    last_modified: OffsetDateTime,
) -> Option<OffsetDateTime> {
    if expiration.expired_object_delete_marker {
        return None;
    }
    expiration.date.or_else(|| {
        expiration
            .days
            .map(|days| last_modified + time::Duration::days(i64::from(days)))
    })
}

/// Returns the eligibility time for a noncurrent-version expiration action.
#[must_use]
pub(crate) fn noncurrent_expiration_time(
    expiration: &StoredNoncurrentVersionExpiration,
    last_modified: OffsetDateTime,
) -> OffsetDateTime {
    last_modified + time::Duration::days(i64::from(expiration.noncurrent_days))
}

/// Selects an object's storage-level lifecycle expiration when one applies.
#[must_use]
pub(crate) fn object_lifecycle_expiration(
    rules: &[StoredLifecycleRule],
    key: &str,
    size: i64,
    last_modified: OffsetDateTime,
    tags: &[StoredTag],
) -> Option<ObjectLifecycleExpiration> {
    rules
        .iter()
        .filter(|rule| rule.enabled && lifecycle_rule_matches_object(rule, key, size, tags))
        .filter_map(|rule| {
            let expiration = rule.expiration.as_ref()?;
            let expires_at = current_expiration_time(expiration, last_modified)?;
            Some(ObjectLifecycleExpiration {
                expires_at,
                rule_id: rule.id.clone().unwrap_or_default(),
            })
        })
        .min_by_key(|expiration| expiration.expires_at)
}

/// Stored Object Lock retention mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoredObjectLockMode {
    Governance,
    Compliance,
}

impl StoredObjectLockMode {
    /// Returns the stable database representation.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Governance => "GOVERNANCE",
            Self::Compliance => "COMPLIANCE",
        }
    }

    /// Parses a stable database representation.
    pub(crate) fn from_sql(value: &str) -> Option<Self> {
        match value {
            "GOVERNANCE" => Some(Self::Governance),
            "COMPLIANCE" => Some(Self::Compliance),
            _ => None,
        }
    }
}

/// Stored per-object-version retention metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredObjectRetention {
    pub(crate) mode: StoredObjectLockMode,
    pub(crate) retain_until: OffsetDateTime,
}

/// Stored per-object-version Object Lock metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StoredObjectLock {
    pub(crate) legal_hold: Option<bool>,
    pub(crate) retention: Option<StoredObjectRetention>,
}

/// Stored bucket default retention rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredObjectLockDefaultRetention {
    pub(crate) mode: StoredObjectLockMode,
    pub(crate) days: Option<i32>,
    pub(crate) years: Option<i32>,
}

/// Stored bucket Object Lock configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StoredObjectLockConfig {
    pub(crate) enabled: bool,
    pub(crate) default_retention: Option<StoredObjectLockDefaultRetention>,
}

/// Returns whether background lifecycle cleanup may permanently delete stored object data.
#[must_use]
pub(crate) fn object_lock_allows_lifecycle_delete(
    lock: &StoredObjectLock,
    now: OffsetDateTime,
) -> bool {
    if lock.legal_hold.is_some_and(|legal_hold| legal_hold) {
        return false;
    }

    let Some(retention) = lock.retention.as_ref() else {
        return true;
    };
    retention.retain_until <= now
}

#[cfg(test)]
pub(crate) mod test_support {
    use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};

    use super::{ChecksumAlgorithmKind, encode_base64};

    /// Computes a base64 checksum value for tests.
    pub(crate) fn checksum_value(algorithm: ChecksumAlgorithmKind, value: &[u8]) -> String {
        match algorithm {
            ChecksumAlgorithmKind::Crc32 => {
                let mut checksum = Crc32::new();
                checksum.update(value);
                encode_base64(checksum.finalize().as_ref())
            }
            ChecksumAlgorithmKind::Crc32c => {
                let mut checksum = Crc32c::new();
                checksum.update(value);
                encode_base64(checksum.finalize().as_ref())
            }
            ChecksumAlgorithmKind::Crc64Nvme => {
                let mut checksum = Crc64Nvme::new();
                checksum.update(value);
                encode_base64(checksum.finalize().as_ref())
            }
            ChecksumAlgorithmKind::Sha1 => {
                let mut checksum = Sha1::new();
                checksum.update(value);
                encode_base64(checksum.finalize().as_ref())
            }
            ChecksumAlgorithmKind::Sha256 => {
                let mut checksum = Sha256::new();
                checksum.update(value);
                encode_base64(checksum.finalize().as_ref())
            }
        }
    }
}
