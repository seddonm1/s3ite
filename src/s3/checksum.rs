//! S3 checksum request validation and response mapping.
//!
//! `SQLite` owns the persisted checksum metadata. This module keeps the S3 wire
//! parsing, checksum policy, and response header mapping.

use std::borrow::Cow;

use hyper::HeaderMap;
use s3s::{
    S3ErrorCode, S3Result, TrailingHeaders,
    dto::{Checksum as S3Checksum, ChecksumAlgorithm, ChecksumMode, ChecksumType},
    header::X_AMZ_SDK_CHECKSUM_ALGORITHM,
};

use crate::sqlite::{
    ChecksumAccumulator, ChecksumAlgorithmKind, ChecksumTypeKind, CompletedObjectPart,
    MultipartChecksumConfig, MultipartCompletionChecksum, PartChecksum, SqliteError,
    StoredChecksum, decode_base64,
};

use super::{s3_error, s3_error_from_sqlite, s3_error_with_message};

/// Re-exported lowercase hexadecimal encoder used for S3 `ETag` formatting.
pub(super) use crate::sqlite::encode_hex as hex;

#[cfg(test)]
use crate::sqlite::{
    CompositeChecksum, FullObjectChecksum, decode_md5_hex, encode_base64, encode_hex,
};

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
    pub(crate) fn algorithm(self) -> Option<ChecksumAlgorithmKind> {
        match self {
            Self::None => None,
            Self::Object(checksum) => Some(checksum.algorithm()),
            Self::Part(checksum) => Some(checksum.algorithm()),
            Self::MultipartConfig(checksum) => Some(checksum.algorithm),
        }
    }

    /// Returns the object checksum aggregation type when metadata carries one.
    pub(crate) fn checksum_type(self) -> Option<ChecksumTypeKind> {
        match self {
            Self::None | Self::Part(_) => None,
            Self::Object(checksum) => Some(checksum.checksum_type()),
            Self::MultipartConfig(checksum) => Some(checksum.checksum_type),
        }
    }

    /// Returns the checksum value for a specific algorithm.
    pub(crate) fn value_for(self, algorithm: ChecksumAlgorithmKind) -> Option<&'a str> {
        match self {
            Self::None | Self::MultipartConfig(_) => None,
            Self::Object(checksum) => checksum.value_for(algorithm),
            Self::Part(checksum) => checksum.value_for(algorithm),
        }
    }

    /// Returns whether this view carries an actual checksum value.
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
    /// Request-provided `SHA512` checksum.
    pub(super) sha512: Option<String>,
    /// Request-provided `MD5` checksum.
    pub(super) md5: Option<String>,
    /// Request-provided `XXHASH3` checksum.
    pub(super) xxhash3: Option<String>,
    /// Request-provided `XXHASH64` checksum.
    pub(super) xxhash64: Option<String>,
    /// Request-provided `XXHASH128` checksum.
    pub(super) xxhash128: Option<String>,
    /// Trailer handle, populated by the decoder after the body.
    pub(super) trailer: Option<ChecksumTrailer>,
}

/// A checksum value that arrives in the `aws-chunked` trailer instead of a header.
#[derive(Clone, Debug)]
pub(super) struct ChecksumTrailer {
    /// The declared checksum field and algorithm, when a checksum was declared.
    declared: Option<(String, ChecksumAlgorithmKind)>,
    /// s3s handle that is filled once the body stream has been fully consumed.
    headers: TrailingHeaders,
}

/// Extracts the common digest fields from the generated S3 request DTOs.
macro_rules! checksum_input {
    ($source:ident, $algorithm:expr, $checksum_type:expr) => {
        ChecksumInput {
            algorithm: $algorithm,
            checksum_type: $checksum_type,
            crc32: $source.checksum_crc32,
            crc32c: $source.checksum_crc32c,
            crc64nvme: $source.checksum_crc64nvme,
            sha1: $source.checksum_sha1,
            sha256: $source.checksum_sha256,
            sha512: $source.checksum_sha512,
            md5: $source.checksum_md5,
            xxhash3: $source.checksum_xxhash3,
            xxhash64: $source.checksum_xxhash64,
            xxhash128: $source.checksum_xxhash128,
            trailer: None,
        }
    };
}
pub(super) use checksum_input;

impl ChecksumInput {
    /// Reconciles the SDK header with the algorithm parsed from the S3 header or checksum trailer.
    pub(super) fn with_sdk_algorithm(mut self, headers: &HeaderMap) -> S3Result<Self> {
        if let Some(header) = headers.get(X_AMZ_SDK_CHECKSUM_ALGORITHM) {
            let name = header
                .to_str()
                .map_err(|_| s3_error(S3ErrorCode::InvalidArgument))?;
            if self
                .algorithm
                .as_ref()
                .is_some_and(|value| value.as_str() != name)
            {
                return Err(s3_error(S3ErrorCode::BadDigest));
            }
            self.algorithm = Some(ChecksumAlgorithm::from(name.to_owned()));
        }
        Ok(self)
    }

    /// Keeps the decoder's trailer handle even when no checksum was declared.
    pub(super) fn with_trailer(
        mut self,
        headers: &HeaderMap,
        trailing_headers: Option<TrailingHeaders>,
    ) -> S3Result<Self> {
        let name = declared_checksum_trailer(headers)?;
        let declared = name
            .map(|name| self.bind_trailer_algorithm(name))
            .transpose()?;
        if declared.is_some() && trailing_headers.is_none() {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidRequest,
                "x-amz-trailer requires an aws-chunked request body",
            ));
        }
        self.trailer = trailing_headers.map(|headers| ChecksumTrailer { declared, headers });
        Ok(self)
    }

    /// Selects the checksum that must be computed while reading the body.
    fn bind_trailer_algorithm(
        &mut self,
        name: String,
    ) -> S3Result<(String, ChecksumAlgorithmKind)> {
        let kind = checksum_algorithm_kind(&ChecksumAlgorithm::from(
            name.trim_start_matches(CHECKSUM_HEADER_PREFIX)
                .to_ascii_uppercase(),
        ))
        .ok_or_else(|| {
            s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "unsupported checksum trailer algorithm",
            )
        })?;
        match &self.algorithm {
            Some(algorithm) if checksum_algorithm_kind(algorithm) != Some(kind) => {
                return Err(s3_error_with_message(
                    S3ErrorCode::InvalidRequest,
                    "x-amz-checksum-algorithm does not match the declared checksum trailer",
                ));
            }
            Some(_) => {}
            None => self.algorithm = Some(checksum_algorithm_dto(kind)),
        }
        Ok((name, kind))
    }

    /// Validates and merges the checksum trailer after the body is consumed.
    fn resolve_trailer(&self) -> S3Result<Cow<'_, Self>> {
        let Some(trailer) = &self.trailer else {
            return Ok(Cow::Borrowed(self));
        };
        let value = trailer
            .headers
            .read(|headers| trailer.checksum_value(headers))
            .transpose()?
            .flatten();
        let Some((_, kind)) = trailer.declared else {
            return Ok(Cow::Borrowed(self));
        };
        let value = value.ok_or_else(|| {
            s3_error_with_message(
                S3ErrorCode::InvalidRequest,
                "declared checksum trailer must be sent exactly once",
            )
        })?;

        let mut resolved = self.clone();
        let slot = resolved.requested_slot(kind);
        if slot.as_ref().is_some_and(|existing| *existing != value) {
            return Err(s3_error(S3ErrorCode::BadDigest));
        }
        *slot = Some(value);
        Ok(Cow::Owned(resolved))
    }

    /// Returns the explicitly requested algorithm or infers it from one value field.
    fn algorithm_kind(&self) -> S3Result<Option<ChecksumAlgorithmKind>> {
        let explicit = checksum_algorithm_from_request(self.algorithm.clone())?;

        let mut inferred = [
            self.crc32.as_ref().map(|_| ChecksumAlgorithmKind::Crc32),
            self.crc32c.as_ref().map(|_| ChecksumAlgorithmKind::Crc32c),
            self.crc64nvme
                .as_ref()
                .map(|_| ChecksumAlgorithmKind::Crc64Nvme),
            self.sha1.as_ref().map(|_| ChecksumAlgorithmKind::Sha1),
            self.sha256.as_ref().map(|_| ChecksumAlgorithmKind::Sha256),
            self.sha512.as_ref().map(|_| ChecksumAlgorithmKind::Sha512),
            self.md5.as_ref().map(|_| ChecksumAlgorithmKind::Md5),
            self.xxhash3
                .as_ref()
                .map(|_| ChecksumAlgorithmKind::XxHash3),
            self.xxhash64
                .as_ref()
                .map(|_| ChecksumAlgorithmKind::XxHash64),
            self.xxhash128
                .as_ref()
                .map(|_| ChecksumAlgorithmKind::XxHash128),
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
        if explicit.is_some() && first.is_some() && explicit != first {
            return Err(s3_error(S3ErrorCode::BadDigest));
        }
        Ok(explicit.or(first))
    }

    /// Returns the mutable request field that carries the checksum for `kind`.
    fn requested_slot(&mut self, kind: ChecksumAlgorithmKind) -> &mut Option<String> {
        match kind {
            ChecksumAlgorithmKind::Crc32 => &mut self.crc32,
            ChecksumAlgorithmKind::Crc32c => &mut self.crc32c,
            ChecksumAlgorithmKind::Crc64Nvme => &mut self.crc64nvme,
            ChecksumAlgorithmKind::Sha1 => &mut self.sha1,
            ChecksumAlgorithmKind::Sha256 => &mut self.sha256,
            ChecksumAlgorithmKind::Sha512 => &mut self.sha512,
            ChecksumAlgorithmKind::Md5 => &mut self.md5,
            ChecksumAlgorithmKind::XxHash3 => &mut self.xxhash3,
            ChecksumAlgorithmKind::XxHash64 => &mut self.xxhash64,
            ChecksumAlgorithmKind::XxHash128 => &mut self.xxhash128,
        }
    }

    /// Returns the request-provided checksum value for the selected algorithm.
    fn requested_for(&self, kind: ChecksumAlgorithmKind) -> Option<&str> {
        match kind {
            ChecksumAlgorithmKind::Crc32 => self.crc32.as_deref(),
            ChecksumAlgorithmKind::Crc32c => self.crc32c.as_deref(),
            ChecksumAlgorithmKind::Crc64Nvme => self.crc64nvme.as_deref(),
            ChecksumAlgorithmKind::Sha1 => self.sha1.as_deref(),
            ChecksumAlgorithmKind::Sha256 => self.sha256.as_deref(),
            ChecksumAlgorithmKind::Sha512 => self.sha512.as_deref(),
            ChecksumAlgorithmKind::Md5 => self.md5.as_deref(),
            ChecksumAlgorithmKind::XxHash3 => self.xxhash3.as_deref(),
            ChecksumAlgorithmKind::XxHash64 => self.xxhash64.as_deref(),
            ChecksumAlgorithmKind::XxHash128 => self.xxhash128.as_deref(),
        }
    }

    /// Validates algorithms that must be declared when the multipart upload is initiated.
    pub(super) fn validate_upload_part(
        &self,
        config: Option<MultipartChecksumConfig>,
    ) -> S3Result<()> {
        let explicit = self.algorithm_kind()?;
        let configured = config.map(|c| c.algorithm);
        if let Some(kind) = explicit
            && Some(kind) != configured
            && (kind.requires_explicit_multipart_config()
                || configured
                    .is_some_and(ChecksumAlgorithmKind::requires_explicit_multipart_config))
        {
            return Err(s3_error(S3ErrorCode::InvalidRequest));
        }
        Ok(())
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

/// Prefix shared by every `x-amz-checksum-*` header and trailer field.
const CHECKSUM_HEADER_PREFIX: &str = "x-amz-checksum-";

/// Returns the single lowercase `x-amz-checksum-*` field named by `x-amz-trailer`.
///
/// Non-checksum trailer fields are ignored. Declaring more than one checksum
/// trailer is `InvalidArgument`, matching the s3s algorithm parser. Every
/// `x-amz-trailer` field line counts, so a second checksum cannot hide in a
/// repeated header.
fn declared_checksum_trailer(headers: &HeaderMap) -> S3Result<Option<String>> {
    let mut lines = Vec::new();
    for value in headers.get_all("x-amz-trailer") {
        lines.push(
            value
                .to_str()
                .map_err(|_| s3_error(S3ErrorCode::InvalidArgument))?,
        );
    }
    let mut names = lines
        .into_iter()
        .flat_map(|line| line.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| name.starts_with(CHECKSUM_HEADER_PREFIX));
    let name = names.next();
    if names.next().is_some() {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "x-amz-trailer declares more than one checksum",
        ));
    }
    Ok(name)
}

impl ChecksumInput {
    /// Returns whether the request carries a checksum s3ite will verify.
    ///
    /// That is a recognized `x-amz-checksum-*` value, a declared checksum
    /// trailer, or an SDK checksum algorithm whose value the body computes.
    pub(super) fn supplies_checksum(&self) -> bool {
        self.algorithm.is_some()
            || self
                .trailer
                .as_ref()
                .is_some_and(|trailer| trailer.declared.is_some())
            || [
                &self.crc32,
                &self.crc32c,
                &self.crc64nvme,
                &self.sha1,
                &self.sha256,
                &self.sha512,
                &self.md5,
                &self.xxhash3,
                &self.xxhash64,
                &self.xxhash128,
            ]
            .iter()
            .any(|value| value.is_some())
    }
}

impl ChecksumTrailer {
    /// Rejects malformed trailer declarations before returning the checksum value.
    fn checksum_value(&self, headers: &HeaderMap) -> S3Result<Option<String>> {
        let declared = self.declared.as_ref().map(|(name, _)| name.as_str());
        // AWS requires the checksum trailer name to match x-amz-trailer
        // (Checking object integrity for data uploads). InvalidRequest describes
        // a malformed trailer declaration; BadDigest describes a digest mismatch.
        if headers.keys().any(|name| {
            name.as_str().starts_with(CHECKSUM_HEADER_PREFIX) && Some(name.as_str()) != declared
        }) {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidRequest,
                "checksum trailer was not declared in x-amz-trailer",
            ));
        }
        let Some(name) = declared else {
            return Ok(None);
        };
        let mut values = headers.get_all(name).iter();
        Ok(match (values.next(), values.next()) {
            (Some(value), None) => value.to_str().ok().map(str::to_owned),
            _ => None,
        })
    }
}

/// Converts an S3 checksum algorithm name into its storage kind.
fn checksum_algorithm_kind(algorithm: &ChecksumAlgorithm) -> Option<ChecksumAlgorithmKind> {
    let kind = match algorithm.as_str() {
        ChecksumAlgorithm::CRC32 => ChecksumAlgorithmKind::Crc32,
        ChecksumAlgorithm::CRC32C => ChecksumAlgorithmKind::Crc32c,
        ChecksumAlgorithm::CRC64NVME => ChecksumAlgorithmKind::Crc64Nvme,
        ChecksumAlgorithm::SHA1 => ChecksumAlgorithmKind::Sha1,
        ChecksumAlgorithm::SHA256 => ChecksumAlgorithmKind::Sha256,
        ChecksumAlgorithm::SHA512 => ChecksumAlgorithmKind::Sha512,
        ChecksumAlgorithm::MD5 => ChecksumAlgorithmKind::Md5,
        ChecksumAlgorithm::XXHASH3 => ChecksumAlgorithmKind::XxHash3,
        ChecksumAlgorithm::XXHASH64 => ChecksumAlgorithmKind::XxHash64,
        ChecksumAlgorithm::XXHASH128 => ChecksumAlgorithmKind::XxHash128,
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
        ChecksumAlgorithmKind::Sha512 => ChecksumAlgorithm::SHA512,
        ChecksumAlgorithmKind::Md5 => ChecksumAlgorithm::MD5,
        ChecksumAlgorithmKind::XxHash3 => ChecksumAlgorithm::XXHASH3,
        ChecksumAlgorithmKind::XxHash64 => ChecksumAlgorithm::XXHASH64,
        ChecksumAlgorithmKind::XxHash128 => ChecksumAlgorithm::XXHASH128,
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
        | ChecksumAlgorithmKind::Sha256
        | ChecksumAlgorithmKind::Sha512
        | ChecksumAlgorithmKind::Md5
        | ChecksumAlgorithmKind::XxHash3
        | ChecksumAlgorithmKind::XxHash64
        | ChecksumAlgorithmKind::XxHash128 => ChecksumTypeKind::Composite,
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
    if algorithm.requires_explicit_multipart_config()
        && checksum_type == Some(ChecksumTypeKind::FullObject)
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "this multipart checksum requires COMPOSITE",
        ));
    }
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
    validate_stored_checksum(&*input.resolve_trailer()?, checksum)
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

/// Verifies trailer declarations and finalizes any explicitly requested part checksum.
pub(super) fn finish_part_checksum(
    input: &ChecksumInput,
    accumulator: Option<ChecksumAccumulator>,
) -> S3Result<Option<PartChecksum>> {
    let input = input.resolve_trailer()?;
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

/// Returns the `SHA512` checksum response field.
pub(super) fn checksum_sha512<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Sha512)
}

/// Returns the `MD5` checksum response field.
pub(super) fn checksum_md5<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::Md5)
}

/// Returns the `XXHASH3` checksum response field.
pub(super) fn checksum_xxhash3<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::XxHash3)
}

/// Returns the `XXHASH64` checksum response field.
pub(super) fn checksum_xxhash64<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::XxHash64)
}

/// Returns the `XXHASH128` checksum response field.
pub(super) fn checksum_xxhash128<'a>(checksum: impl Into<ChecksumView<'a>>) -> Option<String> {
    checksum_field(checksum.into(), ChecksumAlgorithmKind::XxHash128)
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
        checksum_sha512: checksum_sha512(checksum),
        checksum_md5: checksum_md5(checksum),
        checksum_xxhash3: checksum_xxhash3(checksum),
        checksum_xxhash64: checksum_xxhash64(checksum),
        checksum_xxhash128: checksum_xxhash128(checksum),
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
    use s3s::crypto::{
        Checksum as _, Crc32, Crc32c, Crc64Nvme, Md5, Sha1, Sha256, Sha512, XxHash3, XxHash64,
        XxHash128,
    };

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
            ChecksumAlgorithmKind::Sha512 => encode_base64(Sha512::checksum(value).as_ref()),
            ChecksumAlgorithmKind::Md5 => encode_base64(Md5::checksum(value).as_ref()),
            ChecksumAlgorithmKind::XxHash3 => encode_base64(XxHash3::checksum(value).as_ref()),
            ChecksumAlgorithmKind::XxHash64 => encode_base64(XxHash64::checksum(value).as_ref()),
            ChecksumAlgorithmKind::XxHash128 => encode_base64(XxHash128::checksum(value).as_ref()),
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
            ChecksumAlgorithmKind::Sha512,
            ChecksumAlgorithmKind::Md5,
            ChecksumAlgorithmKind::XxHash3,
            ChecksumAlgorithmKind::XxHash64,
            ChecksumAlgorithmKind::XxHash128,
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
            ChecksumAlgorithmKind::Sha512,
            ChecksumAlgorithmKind::Md5,
            ChecksumAlgorithmKind::XxHash3,
            ChecksumAlgorithmKind::XxHash64,
            ChecksumAlgorithmKind::XxHash128,
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
            ChecksumAlgorithmKind::Sha512,
            ChecksumAlgorithmKind::Md5,
            ChecksumAlgorithmKind::XxHash3,
            ChecksumAlgorithmKind::XxHash64,
            ChecksumAlgorithmKind::XxHash128,
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
            ChecksumAlgorithmKind::Sha512,
            ChecksumAlgorithmKind::Md5,
            ChecksumAlgorithmKind::XxHash3,
            ChecksumAlgorithmKind::XxHash64,
            ChecksumAlgorithmKind::XxHash128,
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
            StoredChecksum::new(
                ChecksumAlgorithmKind::Sha256,
                ChecksumTypeKind::Composite,
                format!(
                    "{}-2",
                    test_support::checksum_value(ChecksumAlgorithmKind::Sha256, b"partdigest")
                )
            )
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

    use crate::sqlite::CompletedObjectPart;

    use super::{
        ChecksumAlgorithmKind, ChecksumInput, ChecksumPolicy, ChecksumTypeKind, FullObjectChecksum,
        MultipartChecksumConfig, MultipartCompletionChecksum, PartChecksum, StoredChecksum,
        checksum_algorithm_dto, checksum_algorithm_from_request, checksum_algorithm_kind,
        checksum_crc32, checksum_crc32c, checksum_crc64nvme, checksum_mode_enabled,
        checksum_struct, checksum_type_dto, checksum_type_kind, checksum_type_name,
        declared_checksum_trailer, default_crc64nvme_full_object_checksum, finish_part_checksum,
        finish_payload_checksum, multipart_checksum_config, multipart_checksum_type,
        object_checksum_for_part, part_checksum_accumulator, payload_checksum_accumulator,
        validate_content_md5,
    };
    use hyper::HeaderMap;

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
            (ChecksumAlgorithm::SHA512, ChecksumAlgorithmKind::Sha512),
            (ChecksumAlgorithm::MD5, ChecksumAlgorithmKind::Md5),
            (ChecksumAlgorithm::XXHASH3, ChecksumAlgorithmKind::XxHash3),
            (ChecksumAlgorithm::XXHASH64, ChecksumAlgorithmKind::XxHash64),
            (
                ChecksumAlgorithm::XXHASH128,
                ChecksumAlgorithmKind::XxHash128,
            ),
        ] {
            let algorithm = ChecksumAlgorithm::from_static(name);
            assert_eq!(checksum_algorithm_kind(&algorithm), Some(kind));
            assert_eq!(checksum_algorithm_dto(kind).as_str(), name);
        }
        assert_eq!(
            checksum_algorithm_kind(&ChecksumAlgorithm::from("UNKNOWN".to_string())),
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

    /// Builds request headers containing a single `x-amz-trailer` value.
    fn trailer_headers(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-trailer",
            hyper::header::HeaderValue::from_static(value),
        );
        headers
    }

    /// Checks that only `x-amz-checksum-*` names in `x-amz-trailer` declare a checksum trailer.
    #[test]
    fn parses_declared_checksum_trailer() {
        assert_eq!(
            declared_checksum_trailer(&HeaderMap::new()).ok(),
            Some(None)
        );
        assert_eq!(
            declared_checksum_trailer(&trailer_headers("x-amz-meta-foo")).ok(),
            Some(None)
        );
        assert_eq!(
            declared_checksum_trailer(&trailer_headers(" X-Amz-Checksum-CRC32 , x-amz-meta-a"))
                .ok(),
            Some(Some("x-amz-checksum-crc32".to_string()))
        );
        assert_error_code(
            declared_checksum_trailer(&trailer_headers(
                "x-amz-checksum-crc32,x-amz-checksum-sha256",
            )),
            &S3ErrorCode::InvalidArgument,
        );

        // A second checksum on a repeated header line is still a second checksum.
        let mut repeated = trailer_headers("x-amz-checksum-crc32");
        repeated.append(
            "x-amz-trailer",
            hyper::header::HeaderValue::from_static("x-amz-meta-a, x-amz-checksum-sha256"),
        );
        assert_error_code(
            declared_checksum_trailer(&repeated),
            &S3ErrorCode::InvalidArgument,
        );
        // Repeated lines without a second checksum still find the declared one.
        let mut split = trailer_headers("x-amz-meta-a");
        split.append(
            "x-amz-trailer",
            hyper::header::HeaderValue::from_static("x-amz-checksum-sha256"),
        );
        assert_eq!(
            declared_checksum_trailer(&split).ok(),
            Some(Some("x-amz-checksum-sha256".to_string()))
        );
    }

    /// Checks that trailers which could never be verified are rejected before the body is read.
    #[test]
    fn rejects_unverifiable_checksum_trailers() {
        let input = ChecksumInput::default();
        assert!(
            input
                .clone()
                .with_trailer(&HeaderMap::new(), None)
                .is_ok_and(|input| input.trailer.is_none())
        );
        assert_error_code(
            input
                .clone()
                .with_trailer(&trailer_headers("x-amz-checksum-crc32"), None),
            &S3ErrorCode::InvalidRequest,
        );
        assert_error_code(
            input.with_trailer(&trailer_headers("x-amz-checksum-bogus"), None),
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
