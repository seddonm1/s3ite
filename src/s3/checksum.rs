use std::ops::Not;

use s3s::{
    S3ErrorCode, S3Result,
    crypto::{Checksum as CryptoChecksum, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256},
    dto::{Checksum as S3Checksum, ChecksumAlgorithm, ChecksumMode, ChecksumType},
};

use crate::sqlite::{ChecksumMetadata, CompletedObjectPart, KeyValue, SqliteError, SqliteResult};

use super::{s3_error, s3_error_from_sqlite, s3_error_with_message};

/// Encodes bytes as lowercase hexadecimal text.
pub(super) fn hex(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let input = input.as_ref();
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Decodes standard base64 text.
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

/// Encodes bytes as standard padded base64 text.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ChecksumAlgorithmKind {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithmKind {
    pub(super) fn from_algorithm(algorithm: Option<ChecksumAlgorithm>) -> S3Result<Option<Self>> {
        let Some(algorithm) = algorithm else {
            return Ok(None);
        };
        let kind = Self::from_name(algorithm.as_str()).map_err(|_| {
            s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "unsupported checksum algorithm",
            )
        })?;
        Ok(Some(kind))
    }

    pub(super) fn from_name(algorithm: &str) -> SqliteResult<Self> {
        let kind = match algorithm {
            ChecksumAlgorithm::CRC32 => Self::Crc32,
            ChecksumAlgorithm::CRC32C => Self::Crc32c,
            ChecksumAlgorithm::CRC64NVME => Self::Crc64Nvme,
            ChecksumAlgorithm::SHA1 => Self::Sha1,
            ChecksumAlgorithm::SHA256 => Self::Sha256,
            _ => return Err(SqliteError::ChecksumMismatch),
        };
        Ok(kind)
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => ChecksumAlgorithm::CRC32,
            Self::Crc32c => ChecksumAlgorithm::CRC32C,
            Self::Crc64Nvme => ChecksumAlgorithm::CRC64NVME,
            Self::Sha1 => ChecksumAlgorithm::SHA1,
            Self::Sha256 => ChecksumAlgorithm::SHA256,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ChecksumPolicy {
    ExplicitOnly,
    DefaultCrc64NvmeFullObject,
}

impl ChecksumPolicy {
    fn default_kind(self) -> Option<ChecksumAlgorithmKind> {
        match self {
            Self::ExplicitOnly => None,
            Self::DefaultCrc64NvmeFullObject => Some(ChecksumAlgorithmKind::Crc64Nvme),
        }
    }

    fn default_type(self) -> Option<&'static str> {
        match self {
            Self::ExplicitOnly => None,
            Self::DefaultCrc64NvmeFullObject => Some(ChecksumType::FULL_OBJECT),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ChecksumInput {
    pub(super) algorithm: Option<ChecksumAlgorithm>,
    pub(super) checksum_type: Option<ChecksumType>,
    pub(super) crc32: Option<String>,
    pub(super) crc32c: Option<String>,
    pub(super) crc64nvme: Option<String>,
    pub(super) sha1: Option<String>,
    pub(super) sha256: Option<String>,
}

impl ChecksumInput {
    fn algorithm_kind(&self) -> S3Result<Option<ChecksumAlgorithmKind>> {
        if self.algorithm.is_some() {
            return ChecksumAlgorithmKind::from_algorithm(self.algorithm.clone());
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

    fn requested_for(&self, kind: ChecksumAlgorithmKind) -> Option<&str> {
        match kind {
            ChecksumAlgorithmKind::Crc32 => self.crc32.as_deref(),
            ChecksumAlgorithmKind::Crc32c => self.crc32c.as_deref(),
            ChecksumAlgorithmKind::Crc64Nvme => self.crc64nvme.as_deref(),
            ChecksumAlgorithmKind::Sha1 => self.sha1.as_deref(),
            ChecksumAlgorithmKind::Sha256 => self.sha256.as_deref(),
        }
    }

    pub(super) fn into_requested_metadata(self) -> S3Result<ChecksumMetadata> {
        let algorithm = self.algorithm_kind()?.map(|kind| kind.as_str().to_string());
        let checksum_type = checksum_type_name(self.checksum_type)?;
        Ok(ChecksumMetadata {
            algorithm,
            checksum_type,
            crc32: self.crc32,
            crc32c: self.crc32c,
            crc64nvme: self.crc64nvme,
            sha1: self.sha1,
            sha256: self.sha256,
        })
    }
}

pub(super) fn multipart_checksum_type(kind: ChecksumAlgorithmKind) -> &'static str {
    match kind {
        ChecksumAlgorithmKind::Crc64Nvme => ChecksumType::FULL_OBJECT,
        ChecksumAlgorithmKind::Crc32
        | ChecksumAlgorithmKind::Crc32c
        | ChecksumAlgorithmKind::Sha1
        | ChecksumAlgorithmKind::Sha256 => ChecksumType::COMPOSITE,
    }
}

pub(super) fn checksum_type_name(checksum_type: Option<ChecksumType>) -> S3Result<Option<String>> {
    let Some(checksum_type) = checksum_type else {
        return Ok(None);
    };
    match checksum_type.as_str() {
        ChecksumType::COMPOSITE | ChecksumType::FULL_OBJECT => {
            Ok(Some(checksum_type.as_str().to_string()))
        }
        _ => Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "unsupported checksum type",
        )),
    }
}

pub(super) fn compute_checksum(kind: ChecksumAlgorithmKind, value: &[u8]) -> String {
    match kind {
        ChecksumAlgorithmKind::Crc32 => {
            base64_encode(<Crc32 as CryptoChecksum>::checksum(value).as_ref())
        }
        ChecksumAlgorithmKind::Crc32c => {
            base64_encode(<Crc32c as CryptoChecksum>::checksum(value).as_ref())
        }
        ChecksumAlgorithmKind::Crc64Nvme => {
            base64_encode(<Crc64Nvme as CryptoChecksum>::checksum(value).as_ref())
        }
        ChecksumAlgorithmKind::Sha1 => {
            base64_encode(<Sha1 as CryptoChecksum>::checksum(value).as_ref())
        }
        ChecksumAlgorithmKind::Sha256 => {
            base64_encode(<Sha256 as CryptoChecksum>::checksum(value).as_ref())
        }
    }
}

pub(super) fn checksum_metadata(
    kind: ChecksumAlgorithmKind,
    value: &[u8],
    checksum_type: Option<&str>,
) -> SqliteResult<ChecksumMetadata> {
    let computed = compute_checksum(kind, value);
    let mut checksum = ChecksumMetadata {
        algorithm: Some(kind.as_str().to_string()),
        checksum_type: checksum_type.map(str::to_string),
        ..ChecksumMetadata::default()
    };
    checksum.set_value(kind.as_str(), computed)?;
    Ok(checksum)
}

pub(super) fn validate_payload_checksum(
    input: &ChecksumInput,
    value: &[u8],
    policy: ChecksumPolicy,
    checksum_type: Option<&str>,
) -> S3Result<ChecksumMetadata> {
    let explicit_kind = input.algorithm_kind()?;
    let Some(kind) = explicit_kind.or_else(|| policy.default_kind()) else {
        return Ok(ChecksumMetadata::default());
    };
    let checksum_type = checksum_type.or_else(|| {
        explicit_kind
            .is_none()
            .then(|| policy.default_type())
            .flatten()
    });

    let checksum = checksum_metadata(kind, value, checksum_type).map_err(s3_error_from_sqlite)?;
    if let Some(requested) = input.requested_for(kind)
        && checksum.value_for(kind.as_str()) != Some(requested)
    {
        return Err(s3_error(S3ErrorCode::BadDigest));
    }

    Ok(checksum)
}

pub(super) fn default_crc64nvme_full_object_checksum() -> ChecksumMetadata {
    ChecksumMetadata {
        algorithm: Some(ChecksumAlgorithm::CRC64NVME.to_string()),
        checksum_type: Some(ChecksumType::FULL_OBJECT.to_string()),
        ..ChecksumMetadata::default()
    }
}

pub(super) fn object_write_checksum_from_value(
    kind: ChecksumAlgorithmKind,
    object: &KeyValue,
) -> SqliteResult<ChecksumMetadata> {
    let value = object.value.as_deref().ok_or_else(|| {
        SqliteError::Configuration("object metadata has no stored value".to_string())
    })?;
    checksum_metadata(kind, value, Some(ChecksumType::FULL_OBJECT))
}

pub(super) fn copied_part_checksum(
    upload_checksum: &ChecksumMetadata,
    value: &[u8],
) -> SqliteResult<ChecksumMetadata> {
    let Some(algorithm) = upload_checksum.algorithm.as_deref() else {
        return Ok(ChecksumMetadata::default());
    };
    let kind = ChecksumAlgorithmKind::from_name(algorithm)?;
    checksum_metadata(kind, value, None)
}

pub(super) fn checksum_algorithm_output(checksum: &ChecksumMetadata) -> Option<ChecksumAlgorithm> {
    checksum
        .algorithm
        .as_deref()
        .map(|algorithm| ChecksumAlgorithm::from(algorithm.to_string()))
}

pub(super) fn checksum_type_output(checksum: &ChecksumMetadata) -> Option<ChecksumType> {
    checksum
        .checksum_type
        .as_deref()
        .map(|checksum_type| ChecksumType::from(checksum_type.to_string()))
}

pub(super) fn checksum_struct(checksum: &ChecksumMetadata) -> Option<S3Checksum> {
    checksum.has_value().then(|| S3Checksum {
        checksum_crc32: checksum.crc32.clone(),
        checksum_crc32c: checksum.crc32c.clone(),
        checksum_crc64nvme: checksum.crc64nvme.clone(),
        checksum_sha1: checksum.sha1.clone(),
        checksum_sha256: checksum.sha256.clone(),
        checksum_type: checksum_type_output(checksum),
    })
}

pub(super) fn object_checksum_for_part(
    object_checksum: &ChecksumMetadata,
    part: Option<&CompletedObjectPart>,
) -> ChecksumMetadata {
    let Some(part) = part else {
        return object_checksum.clone();
    };
    if part.checksum.has_value().not() {
        return ChecksumMetadata::default();
    }
    ChecksumMetadata {
        algorithm: object_checksum.algorithm.clone(),
        checksum_type: object_checksum.checksum_type.clone(),
        crc32: part.checksum.crc32.clone(),
        crc32c: part.checksum.crc32c.clone(),
        crc64nvme: part.checksum.crc64nvme.clone(),
        sha1: part.checksum.sha1.clone(),
        sha256: part.checksum.sha256.clone(),
    }
}

pub(super) fn checksum_mode_enabled(checksum_mode: Option<&ChecksumMode>) -> bool {
    checksum_mode.is_some_and(|mode| mode.as_str() == ChecksumMode::ENABLED)
}

#[cfg(test)]
mod tests {
    use s3s::S3ErrorCode;

    use super::{decode_base64, validate_content_md5};

    fn assert_error_code(result: s3s::S3Result<()>, code: &S3ErrorCode) {
        match result {
            Ok(()) => panic!("expected {code:?}"),
            Err(err) => assert_eq!(err.code(), code),
        }
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
}
