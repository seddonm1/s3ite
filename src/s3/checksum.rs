use std::ops::Not;

use s3s::{
    S3ErrorCode, S3Result,
    dto::{Checksum as S3Checksum, ChecksumAlgorithm, ChecksumMode, ChecksumType},
};

use crate::{
    checksum::{
        CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT,
        ChecksumAlgorithmKind as StorageChecksumAlgorithmKind, checksum_value, decode_base64,
    },
    sqlite::{ChecksumMetadata, CompletedObjectPart, KeyValue, SqliteError, SqliteResult},
};

use super::{s3_error, s3_error_from_sqlite, s3_error_with_message};

pub(super) use crate::checksum::encode_hex as hex;

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

pub(super) type ChecksumAlgorithmKind = StorageChecksumAlgorithmKind;

pub(super) fn checksum_algorithm_from_request(
    algorithm: Option<ChecksumAlgorithm>,
) -> S3Result<Option<ChecksumAlgorithmKind>> {
    let Some(algorithm) = algorithm else {
        return Ok(None);
    };
    let kind = ChecksumAlgorithmKind::from_name(algorithm.as_str()).ok_or_else(|| {
        s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "unsupported checksum algorithm",
        )
    })?;
    Ok(Some(kind))
}

pub(super) fn checksum_algorithm_from_name(algorithm: &str) -> SqliteResult<ChecksumAlgorithmKind> {
    ChecksumAlgorithmKind::from_name(algorithm).ok_or(SqliteError::ChecksumMismatch)
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
        ChecksumAlgorithmKind::Crc64Nvme => CHECKSUM_TYPE_FULL_OBJECT,
        ChecksumAlgorithmKind::Crc32
        | ChecksumAlgorithmKind::Crc32c
        | ChecksumAlgorithmKind::Sha1
        | ChecksumAlgorithmKind::Sha256 => CHECKSUM_TYPE_COMPOSITE,
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

pub(super) fn checksum_metadata(
    kind: ChecksumAlgorithmKind,
    value: &[u8],
    checksum_type: Option<&str>,
) -> SqliteResult<ChecksumMetadata> {
    let computed = checksum_value(kind, value);
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
    let kind = checksum_algorithm_from_name(algorithm)?;
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

    use super::validate_content_md5;

    fn assert_error_code(result: s3s::S3Result<()>, code: &S3ErrorCode) {
        match result {
            Ok(()) => panic!("expected {code:?}"),
            Err(err) => assert_eq!(err.code(), code),
        }
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
