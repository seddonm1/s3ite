//! S3 protocol checksum validation and response mapping.
//!
//! Request DTOs expose checksum values as optional S3 header fields. Storage
//! uses closed Rust types, so this module is the boundary that validates wire
//! input, calculates missing defaults, and shapes typed checksums back into S3
//! responses.

use s3s::{
    S3ErrorCode, S3Result,
    dto::{Checksum as S3Checksum, ChecksumAlgorithm, ChecksumMode, ChecksumType},
};

use crate::{
    checksum::{
        ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, ChecksumView,
        MultipartChecksumConfig, MultipartCompletionChecksum, PartChecksum, StoredChecksum,
        decode_base64,
    },
    sqlite::{CompletedObjectPart, KeyValue, SqliteError, SqliteResult},
};

use super::{s3_error, s3_error_from_sqlite, s3_error_with_message};

/// Re-exported lowercase hexadecimal encoder used for S3 `ETag` formatting.
pub(super) use crate::checksum::encode_hex as hex;

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
    let kind = ChecksumAlgorithmKind::try_from(&algorithm).map_err(|()| {
        s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "unsupported checksum algorithm",
        )
    })?;
    Ok(Some(kind))
}

/// Converts an optional S3 checksum type DTO into its internal kind.
pub(super) fn checksum_type_name(
    checksum_type: Option<ChecksumType>,
) -> S3Result<Option<ChecksumTypeKind>> {
    let Some(checksum_type) = checksum_type else {
        return Ok(None);
    };
    ChecksumTypeKind::try_from(&checksum_type)
        .map(Some)
        .map_err(|()| {
            s3_error_with_message(S3ErrorCode::InvalidArgument, "unsupported checksum type")
        })
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
    checksum.into().algorithm().map(Into::into)
}

/// Converts stored checksum type metadata into an S3 response DTO.
pub(super) fn checksum_type_output<'a>(
    checksum: impl Into<ChecksumView<'a>>,
) -> Option<ChecksumType> {
    checksum.into().checksum_type().map(Into::into)
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
mod tests {
    use s3s::{
        S3ErrorCode,
        dto::{ChecksumAlgorithm, ChecksumType},
    };

    use crate::{
        checksum::{ChecksumTypeKind, FullObjectChecksum, PartChecksum, StoredChecksum},
        sqlite::CompletedObjectPart,
    };

    use super::{
        ChecksumAlgorithmKind, ChecksumInput, ChecksumPolicy, checksum_algorithm_from_request,
        checksum_mode_enabled, checksum_type_name, configured_part_checksum,
        default_crc64nvme_full_object_checksum, finish_part_checksum, finish_payload_checksum,
        multipart_checksum_config, multipart_checksum_type, object_checksum_for_part,
        part_checksum_accumulator, payload_checksum_accumulator, validate_content_md5,
    };

    fn assert_error_code<T>(result: s3s::S3Result<T>, code: &S3ErrorCode) {
        match result {
            Ok(_) => panic!("expected {code:?}"),
            Err(err) => assert_eq!(err.code(), code),
        }
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
        let config = match multipart_checksum_config(
            Some(ChecksumAlgorithm::from_static(ChecksumAlgorithm::SHA256)),
            None,
        ) {
            Ok(Some(config)) => config,
            Ok(None) => panic!("expected checksum config"),
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        assert_eq!(config.algorithm, ChecksumAlgorithmKind::Sha256);
        assert_eq!(config.checksum_type, ChecksumTypeKind::Composite);

        let default = default_crc64nvme_full_object_checksum();
        assert_eq!(default.algorithm, ChecksumAlgorithmKind::Crc64Nvme);
        assert_eq!(default.checksum_type, ChecksumTypeKind::FullObject);
    }

    #[test]
    fn completion_checksum_value_does_not_imply_checksum_type() {
        let checksum = match (ChecksumInput {
            crc32: Some("WgDhBQ==".to_string()),
            ..ChecksumInput::default()
        })
        .into_completion_checksum()
        {
            Ok(checksum) => checksum,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert_eq!(checksum.algorithm(), Some(ChecksumAlgorithmKind::Crc32));
        assert_eq!(checksum.checksum_type(), None);
        assert_eq!(
            checksum.value_for(ChecksumAlgorithmKind::Crc32),
            Some("WgDhBQ==")
        );
    }

    #[test]
    fn completion_checksum_preserves_explicit_checksum_type() {
        let checksum = match (ChecksumInput {
            checksum_type: Some(ChecksumType::from_static(ChecksumType::COMPOSITE)),
            crc32c: Some("xU+Krw==".to_string()),
            ..ChecksumInput::default()
        })
        .into_completion_checksum()
        {
            Ok(checksum) => checksum,
            Err(err) => panic!("unexpected error: {err:?}"),
        };

        assert_eq!(checksum.algorithm(), Some(ChecksumAlgorithmKind::Crc32c));
        assert_eq!(checksum.checksum_type(), Some(ChecksumTypeKind::Composite));
        assert_eq!(
            checksum.value_for(ChecksumAlgorithmKind::Crc32c),
            Some("xU+Krw==")
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
        let mut accumulator = match payload_checksum_accumulator(
            &input,
            ChecksumPolicy::DefaultCrc64NvmeFullObject,
        ) {
            Ok(accumulator) => accumulator,
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        accumulator.update(b"test data");
        let checksum =
            match finish_payload_checksum(&input, accumulator, ChecksumTypeKind::FullObject) {
                Ok(checksum) => checksum,
                Err(err) => panic!("unexpected error: {err:?}"),
            };
        assert_eq!(
            checksum,
            StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("rsrzr5yYqFU=".to_string()))
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
        let mut accumulator = match part_checksum_accumulator(&input) {
            Ok(Some(accumulator)) => accumulator,
            Ok(None) => panic!("expected checksum accumulator"),
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        accumulator.update(b"test data");
        let checksum = match finish_part_checksum(&input, Some(accumulator)) {
            Ok(Some(checksum)) => checksum,
            Ok(None) => panic!("expected part checksum"),
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        assert_eq!(
            checksum,
            PartChecksum::Sha256("kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk=".to_string())
        );
    }

    #[test]
    fn computes_configured_part_checksum_when_upload_requested_one() {
        let config = match multipart_checksum_config(
            Some(ChecksumAlgorithm::from_static(ChecksumAlgorithm::SHA256)),
            None,
        ) {
            Ok(config) => config,
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        let Some(checksum) = configured_part_checksum(config, b"test data") else {
            panic!("expected copied part checksum");
        };
        assert_eq!(
            checksum,
            PartChecksum::Sha256("kW8AJ6V1B0znKjMXd8NHjWUT94alkb2JLaGld78jNfk=".to_string())
        );
    }

    #[test]
    fn suppresses_part_checksum_when_part_has_none() {
        let object_checksum =
            StoredChecksum::Full(FullObjectChecksum::Crc64Nvme("rsrzr5yYqFU=".to_string()));
        let part = CompletedObjectPart {
            part_number: 1,
            start: 0,
            size: 9,
            md5: None,
            checksum: None,
        };
        assert_eq!(
            object_checksum_for_part(Some(&object_checksum), Some(&part)),
            None
        );
    }
}
