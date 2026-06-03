//! `SQLite` checksum column conversion.
//!
//! Completed objects store exactly one checksum as compact
//! `(algorithm, type, value)` columns. Active multipart uploads store only a
//! checksum configuration, while multipart part rows store only part-level
//! values.

use rusqlite::{
    ToSql,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};

use crate::checksum::{
    ChecksumAlgorithmKind, ChecksumTypeKind, MultipartChecksumConfig,
    decode_md5_hex as decode_storage_md5_hex,
};
pub(crate) use crate::checksum::{PartChecksum, StoredChecksum};

use super::{SqliteError, SqliteResult};

pub(super) use crate::checksum::{decode_base64, encode_hex};

const CHECKSUM_ALGORITHM_CRC32: i64 = 1;
const CHECKSUM_ALGORITHM_CRC32C: i64 = 2;
const CHECKSUM_ALGORITHM_CRC64NVME: i64 = 3;
const CHECKSUM_ALGORITHM_SHA1: i64 = 4;
const CHECKSUM_ALGORITHM_SHA256: i64 = 5;
const CHECKSUM_TYPE_COMPOSITE: i64 = 1;
const CHECKSUM_TYPE_FULL_OBJECT: i64 = 2;

impl ToSql for ChecksumAlgorithmKind {
    /// Serializes a checksum algorithm as its compact database code.
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let code = match self {
            Self::Crc32 => CHECKSUM_ALGORITHM_CRC32,
            Self::Crc32c => CHECKSUM_ALGORITHM_CRC32C,
            Self::Crc64Nvme => CHECKSUM_ALGORITHM_CRC64NVME,
            Self::Sha1 => CHECKSUM_ALGORITHM_SHA1,
            Self::Sha256 => CHECKSUM_ALGORITHM_SHA256,
        };
        Ok(ToSqlOutput::from(code))
    }
}

impl FromSql for ChecksumAlgorithmKind {
    /// Deserializes a checksum algorithm from its compact database code.
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let ValueRef::Integer(code) = value else {
            return Err(FromSqlError::InvalidType);
        };
        match code {
            CHECKSUM_ALGORITHM_CRC32 => Ok(Self::Crc32),
            CHECKSUM_ALGORITHM_CRC32C => Ok(Self::Crc32c),
            CHECKSUM_ALGORITHM_CRC64NVME => Ok(Self::Crc64Nvme),
            CHECKSUM_ALGORITHM_SHA1 => Ok(Self::Sha1),
            CHECKSUM_ALGORITHM_SHA256 => Ok(Self::Sha256),
            _ => Err(FromSqlError::Other(Box::new(SqliteError::ChecksumMismatch))),
        }
    }
}

impl ToSql for ChecksumTypeKind {
    /// Serializes a checksum type as its compact database code.
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let code = match self {
            Self::Composite => CHECKSUM_TYPE_COMPOSITE,
            Self::FullObject => CHECKSUM_TYPE_FULL_OBJECT,
        };
        Ok(ToSqlOutput::from(code))
    }
}

impl FromSql for ChecksumTypeKind {
    /// Deserializes a checksum type from its compact database code.
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let ValueRef::Integer(code) = value else {
            return Err(FromSqlError::InvalidType);
        };
        match code {
            CHECKSUM_TYPE_COMPOSITE => Ok(Self::Composite),
            CHECKSUM_TYPE_FULL_OBJECT => Ok(Self::FullObject),
            _ => Err(FromSqlError::Other(Box::new(SqliteError::ChecksumMismatch))),
        }
    }
}

/// Decodes a multipart MD5 `ETag` fragment from hexadecimal storage text.
pub(super) fn decode_md5_hex(value: &str) -> SqliteResult<[u8; 16]> {
    decode_storage_md5_hex(value).ok_or(SqliteError::InvalidMultipartPart)
}

/// Builds a stored object checksum from non-null `SQLite` columns.
pub(super) fn stored_checksum(
    algorithm: ChecksumAlgorithmKind,
    checksum_type: ChecksumTypeKind,
    value: String,
) -> SqliteResult<StoredChecksum> {
    StoredChecksum::new(algorithm, checksum_type, value).ok_or(SqliteError::ChecksumMismatch)
}

/// Builds an optional stored object checksum from nullable `SQLite` columns.
pub(super) fn optional_stored_checksum(
    algorithm: Option<ChecksumAlgorithmKind>,
    checksum_type: Option<ChecksumTypeKind>,
    value: Option<String>,
) -> rusqlite::Result<Option<StoredChecksum>> {
    match (algorithm, checksum_type, value) {
        (None, None, None) => Ok(None),
        (Some(algorithm), Some(checksum_type), Some(value)) => {
            stored_checksum(algorithm, checksum_type, value)
                .map(Some)
                .map_err(checksum_conversion_error)
        }
        _ => Err(checksum_conversion_error(SqliteError::ChecksumMismatch)),
    }
}

/// Builds a required stored object checksum from `SQLite` columns.
pub(super) fn required_stored_checksum(
    algorithm: ChecksumAlgorithmKind,
    checksum_type: ChecksumTypeKind,
    value: String,
) -> rusqlite::Result<StoredChecksum> {
    stored_checksum(algorithm, checksum_type, value).map_err(checksum_conversion_error)
}

/// Builds an optional multipart checksum configuration from nullable columns.
pub(super) fn optional_multipart_checksum_config(
    algorithm: Option<ChecksumAlgorithmKind>,
    checksum_type: Option<ChecksumTypeKind>,
    value: Option<String>,
) -> rusqlite::Result<Option<MultipartChecksumConfig>> {
    match (algorithm, checksum_type, value) {
        (None, None, None) => Ok(None),
        (Some(algorithm), Some(checksum_type), None) => {
            Ok(Some(MultipartChecksumConfig::new(algorithm, checksum_type)))
        }
        _ => Err(checksum_conversion_error(SqliteError::ChecksumMismatch)),
    }
}

/// Builds an optional part checksum from nullable columns.
pub(super) fn optional_part_checksum(
    algorithm: Option<ChecksumAlgorithmKind>,
    checksum_type: Option<ChecksumTypeKind>,
    value: Option<String>,
) -> rusqlite::Result<Option<PartChecksum>> {
    match (algorithm, checksum_type, value) {
        (None, None, None) => Ok(None),
        (Some(algorithm), None, Some(value)) => Ok(Some(PartChecksum::new(algorithm, value))),
        _ => Err(checksum_conversion_error(SqliteError::ChecksumMismatch)),
    }
}

/// Converts a storage checksum error into a row-conversion failure.
pub(super) fn checksum_conversion_error(error: SqliteError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, Box::new(error))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{
        checksum::{
            ChecksumAlgorithmKind, ChecksumTypeKind, FullObjectChecksum, PartChecksum,
            StoredChecksum,
        },
        sqlite::{
            MultipartChecksumConfig, SqliteError, optional_multipart_checksum_config,
            optional_part_checksum, optional_stored_checksum, required_stored_checksum,
        },
    };

    use super::{decode_base64, decode_md5_hex, encode_hex, stored_checksum};

    fn assert_sqlite_error<T>(result: crate::sqlite::SqliteResult<T>, expected: &SqliteError) {
        let err = result.err();
        assert!(
            err.as_ref()
                .is_some_and(|err| std::mem::discriminant(err) == std::mem::discriminant(expected)),
            "expected {expected:?}"
        );
        if let Some(err) = err {
            assert_eq!(
                std::mem::discriminant(&err),
                std::mem::discriminant(expected),
                "{err:?}"
            );
        }
    }

    #[test]
    fn algorithm_and_type_roundtrip_through_sqlite_columns() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        for algorithm in [
            ChecksumAlgorithmKind::Crc32,
            ChecksumAlgorithmKind::Crc32c,
            ChecksumAlgorithmKind::Crc64Nvme,
            ChecksumAlgorithmKind::Sha1,
            ChecksumAlgorithmKind::Sha256,
        ] {
            let decoded = connection.query_row("SELECT ?1;", [algorithm], |row| {
                row.get::<_, ChecksumAlgorithmKind>(0)
            })?;
            assert_eq!(decoded, algorithm);
        }
        for checksum_type in [ChecksumTypeKind::Composite, ChecksumTypeKind::FullObject] {
            let decoded = connection.query_row("SELECT ?1;", [checksum_type], |row| {
                row.get::<_, ChecksumTypeKind>(0)
            })?;
            assert_eq!(decoded, checksum_type);
        }
        Ok(())
    }

    #[test]
    fn invalid_sql_codes_fail_conversion() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        assert!(
            connection
                .query_row("SELECT 99;", (), |row| row
                    .get::<_, ChecksumAlgorithmKind>(0))
                .is_err()
        );
        assert!(
            connection
                .query_row("SELECT 99;", (), |row| row.get::<_, ChecksumTypeKind>(0))
                .is_err()
        );
        assert!(
            connection
                .query_row("SELECT 'crc32';", (), |row| row
                    .get::<_, ChecksumAlgorithmKind>(0))
                .is_err()
        );
        assert!(
            connection
                .query_row("SELECT 'full-object';", (), |row| row
                    .get::<_, ChecksumTypeKind>(0))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn stored_checksum_rejects_crc64nvme_composite() {
        assert_sqlite_error(
            stored_checksum(
                ChecksumAlgorithmKind::Crc64Nvme,
                ChecksumTypeKind::Composite,
                "value".to_string(),
            ),
            &SqliteError::ChecksumMismatch,
        );
        assert_eq!(
            stored_checksum(
                ChecksumAlgorithmKind::Crc64Nvme,
                ChecksumTypeKind::FullObject,
                "value".to_string(),
            )
            .ok(),
            Some(StoredChecksum::Full(
                crate::checksum::FullObjectChecksum::Crc64Nvme("value".to_string())
            ))
        );
    }

    #[test]
    fn part_checksum_never_carries_type() {
        assert_eq!(
            optional_part_checksum(
                Some(ChecksumAlgorithmKind::Sha256),
                None,
                Some("sha".to_string())
            )
            .ok()
            .flatten(),
            Some(PartChecksum::Sha256("sha".to_string()))
        );
        assert!(
            optional_part_checksum(
                Some(ChecksumAlgorithmKind::Sha256),
                Some(ChecksumTypeKind::FullObject),
                Some("sha".to_string()),
            )
            .is_err()
        );
    }

    #[test]
    fn multipart_config_never_carries_value() {
        assert_eq!(
            optional_multipart_checksum_config(
                Some(ChecksumAlgorithmKind::Sha256),
                Some(ChecksumTypeKind::Composite),
                None,
            )
            .ok()
            .flatten(),
            Some(MultipartChecksumConfig::new(
                ChecksumAlgorithmKind::Sha256,
                ChecksumTypeKind::Composite,
            ))
        );
        assert!(
            optional_multipart_checksum_config(
                Some(ChecksumAlgorithmKind::Sha256),
                Some(ChecksumTypeKind::Composite),
                Some("sha".to_string()),
            )
            .is_err()
        );
    }

    #[test]
    fn optional_stored_checksum_requires_all_object_columns() {
        assert!(
            optional_stored_checksum(
                Some(ChecksumAlgorithmKind::Sha256),
                None,
                Some("sha".to_string())
            )
            .is_err()
        );
        assert_eq!(
            optional_stored_checksum(
                Some(ChecksumAlgorithmKind::Sha256),
                Some(ChecksumTypeKind::FullObject),
                Some("sha".to_string()),
            )
            .ok()
            .flatten()
            .map(|checksum| (checksum.algorithm(), checksum.checksum_type())),
            Some((ChecksumAlgorithmKind::Sha256, ChecksumTypeKind::FullObject))
        );
    }

    #[test]
    fn required_stored_checksum_maps_to_row_conversion_errors() {
        assert!(matches!(
            required_stored_checksum(
                ChecksumAlgorithmKind::Crc64Nvme,
                ChecksumTypeKind::Composite,
                "crc".to_string(),
            ),
            Err(rusqlite::Error::FromSqlConversionFailure(..))
        ));
        assert_eq!(
            required_stored_checksum(
                ChecksumAlgorithmKind::Crc32,
                ChecksumTypeKind::FullObject,
                "crc32".to_string(),
            )
            .ok(),
            Some(StoredChecksum::Full(FullObjectChecksum::Crc32(
                "crc32".to_string()
            )))
        );
    }

    #[test]
    fn maps_invalid_md5_hex_to_invalid_multipart_part() {
        assert_eq!(
            decode_md5_hex("EB733A00C0C9D336E65691A37AB54293").ok(),
            Some([
                0xeb, 0x73, 0x3a, 0x00, 0xc0, 0xc9, 0xd3, 0x36, 0xe6, 0x56, 0x91, 0xa3, 0x7a, 0xb5,
                0x42, 0x93
            ])
        );
        assert_sqlite_error(decode_md5_hex("bad"), &SqliteError::InvalidMultipartPart);
    }

    #[test]
    fn computes_checksum_by_storage_algorithm() {
        assert_eq!(
            ChecksumAlgorithmKind::Crc64Nvme.checksum_value(b"test data"),
            "rsrzr5yYqFU="
        );
    }

    #[test]
    fn exposes_shared_codecs_to_sqlite_modules() {
        assert_eq!(encode_hex([0xab, 0xcd]), "abcd");
        assert_eq!(decode_base64("Zm8="), Some(b"fo".to_vec()));
    }
}
