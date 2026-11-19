//! S3 envelope crypto, request handling, response mapping, and encrypted body encoding.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload, consts::U12},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hyper::http::{HeaderMap, HeaderValue};
use s3s::{S3Error, S3ErrorCode, S3Result, dto::ServerSideEncryption};

use crate::sqlite::{ChunkPosition, EncodedBlobChunk, EncryptionMode, Sqlite, StoredSse};
#[cfg(test)]
use crate::sqlite::{SqliteError, SqliteResult, StoredBlobChunk};

use super::kms;
use super::{InternalError, s3_error_from_sqlite, s3_error_with_message};

const MASTER_KEY_LEN: usize = 32;
const DATA_KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// Encryption state for a new write or existing multipart upload.
pub(crate) struct ActiveSse {
    pub(crate) stored: StoredSse,
    pub(super) data_key: [u8; DATA_KEY_LEN],
}

/// Encrypted chunk data plus the nonce/tag needed to decrypt it.
#[derive(Debug)]
pub(crate) struct EncryptedChunk {
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
    pub(crate) tag: Vec<u8>,
}

/// Parses the configured base64-encoded SSE-S3 master key.
pub(crate) fn parse_master_key(value: &str) -> Result<[u8; MASTER_KEY_LEN], String> {
    let bytes = STANDARD
        .decode(value)
        .map_err(|err| format!("sse_s3_key must be base64: {err}"))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!(
            "sse_s3_key must decode to {MASTER_KEY_LEN} bytes, got {}",
            bytes.len()
        )
    })
}

/// Authenticated data that binds a wrapped data key to its object.
///
/// It is the object ARN, the same value SSE-KMS keys carry as their
/// `aws:s3:arn` encryption context. Multipart uploads complete under their own
/// key and copies wrap a fresh data key, so the ARN never changes for a key.
fn object_aad(bucket: &str, key: &str) -> Vec<u8> {
    format!("arn:aws:s3:::{bucket}/{key}").into_bytes()
}

/// Authenticated data that binds an encrypted chunk to its place in its part.
///
/// A chunk then fails to decrypt if it is reordered, moved to another part, or
/// left behind when the part is truncated, because the part size is bound too.
fn chunk_aad(position: ChunkPosition) -> [u8; 16] {
    let mut aad = [0; 16];
    aad[..4].copy_from_slice(&position.part_number.to_be_bytes());
    aad[4..8].copy_from_slice(&position.chunk_number.to_be_bytes());
    aad[8..].copy_from_slice(&position.part_size.to_be_bytes());
    aad
}

/// Creates a new data key for `bucket`/`key` and wraps it with the master key.
pub(crate) fn new_active_sse(
    master_key: &[u8; MASTER_KEY_LEN],
    bucket: &str,
    key: &str,
) -> Result<ActiveSse, String> {
    let mut data_key = [0; DATA_KEY_LEN];
    getrandom::fill(&mut data_key).map_err(|err| err.to_string())?;

    let mut nonce = [0; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|err| err.to_string())?;

    let encrypted = encrypt_bytes(master_key, &nonce, &data_key, &object_aad(bucket, key))?;
    let (ciphertext, tag) = split_ciphertext_tag(encrypted)?;
    Ok(ActiveSse {
        stored: StoredSse::Local {
            nonce: nonce.to_vec(),
            ciphertext,
            tag,
        },
        data_key,
    })
}

/// Unwraps the stored data key of the object or upload at `bucket`/`key`.
pub(crate) fn active_sse_from_stored(
    master_key: &[u8; MASTER_KEY_LEN],
    stored: StoredSse,
    bucket: &str,
    key: &str,
) -> Result<ActiveSse, String> {
    let StoredSse::Local {
        nonce,
        ciphertext,
        tag,
    } = &stored
    else {
        return Err("expected a locally wrapped data key".into());
    };
    let nonce = nonce_array(nonce)?;
    let encrypted = join_ciphertext_tag(ciphertext, tag);
    let data_key = decrypt_bytes(master_key, &nonce, &encrypted, &object_aad(bucket, key))?;
    let data_key = data_key
        .try_into()
        .map_err(|bytes: Vec<u8>| format!("invalid SSE data key length: {}", bytes.len()))?;
    Ok(ActiveSse { stored, data_key })
}

/// Encrypts one object storage chunk, bound to its `position` in its part.
pub(crate) fn encrypt_chunk(
    active: &ActiveSse,
    position: ChunkPosition,
    plaintext: &[u8],
) -> Result<EncryptedChunk, String> {
    let mut nonce = [0; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|err| err.to_string())?;
    let encrypted = encrypt_bytes(&active.data_key, &nonce, plaintext, &chunk_aad(position))?;
    let (ciphertext, tag) = split_ciphertext_tag(encrypted)?;
    Ok(EncryptedChunk {
        ciphertext,
        nonce: nonce.to_vec(),
        tag,
    })
}

/// Decrypts one object storage chunk, which must be at the `position` it was encrypted for.
pub(crate) fn decrypt_chunk(
    active: &ActiveSse,
    position: ChunkPosition,
    nonce: &[u8],
    tag: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    let nonce = nonce_array(nonce)?;
    let encrypted = join_ciphertext_tag(ciphertext, tag);
    decrypt_bytes(&active.data_key, &nonce, &encrypted, &chunk_aad(position))
}

/// Encrypts bytes with AES-256-GCM, authenticating `aad`, and returns ciphertext plus tag.
fn encrypt_bytes(
    key: &[u8; MASTER_KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|err| err.to_string())?;
    let nonce = <&Nonce<U12>>::try_from(nonce.as_slice()).map_err(|err| err.to_string())?;
    cipher
        .encrypt(
            nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|err| err.to_string())
}

/// Decrypts bytes produced by `encrypt_bytes`; fails unless `aad` matches.
fn decrypt_bytes(
    key: &[u8; MASTER_KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|err| err.to_string())?;
    let nonce = <&Nonce<U12>>::try_from(nonce.as_slice()).map_err(|err| err.to_string())?;
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|err| err.to_string())
}

/// Splits AES-GCM output into storage ciphertext and authentication tag fields.
fn split_ciphertext_tag(mut encrypted: Vec<u8>) -> Result<(Vec<u8>, Vec<u8>), String> {
    if encrypted.len() < TAG_LEN {
        return Err("SSE ciphertext is shorter than the authentication tag".to_string());
    }
    let tag = encrypted.split_off(encrypted.len() - TAG_LEN);
    Ok((encrypted, tag))
}

/// Joins storage ciphertext and authentication tag fields for AES-GCM decryption.
fn join_ciphertext_tag(ciphertext: &[u8], tag: &[u8]) -> Vec<u8> {
    let mut encrypted = Vec::with_capacity(ciphertext.len() + tag.len());
    encrypted.extend_from_slice(ciphertext);
    encrypted.extend_from_slice(tag);
    encrypted
}

/// Validates and converts stored nonce bytes into AES-GCM nonce length.
fn nonce_array(nonce: &[u8]) -> Result<[u8; NONCE_LEN], String> {
    nonce
        .try_into()
        .map_err(|_| format!("SSE nonce must be {NONCE_LEN} bytes"))
}

const SSE_HEADER: &str = "x-amz-server-side-encryption";
const KMS_KEY_HEADER: &str = "x-amz-server-side-encryption-aws-kms-key-id";
const KMS_CONTEXT_HEADER: &str = "x-amz-server-side-encryption-context";
const SSE_S3_ALGORITHM: &str = "AES256";
const UNSUPPORTED_SSE_HEADERS: &[&str] = &[
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-copy-source-server-side-encryption-customer-algorithm",
    "x-amz-copy-source-server-side-encryption-customer-key",
    "x-amz-copy-source-server-side-encryption-customer-key-md5",
];

/// Rejects request-level SSE headers for operations where they do not apply.
pub(super) fn validate_no_sse_headers(headers: &HeaderMap) -> S3Result<()> {
    if [SSE_HEADER, KMS_KEY_HEADER, KMS_CONTEXT_HEADER]
        .iter()
        .any(|name| headers.contains_key(*name))
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "server-side encryption headers are not valid for this operation",
        ));
    }
    validate_no_unsupported_sse_headers(headers)
}

/// Recognizes repeated KMS declarations on individual parts and completion.
pub(super) fn multipart_kms_headers(headers: &HeaderMap) -> S3Result<bool> {
    if headers
        .get(SSE_HEADER)
        .is_some_and(|algorithm| algorithm == "aws:kms")
        && !headers.contains_key(KMS_CONTEXT_HEADER)
    {
        validate_no_unsupported_sse_headers(headers)?;
        return Ok(true);
    }
    validate_no_sse_headers(headers)?;
    Ok(false)
}

/// Keeps the key chosen at initiation authoritative throughout a multipart upload.
pub(super) fn validate_multipart_mode(declares_kms: bool, upload_uses_kms: bool) -> S3Result<()> {
    if declares_kms && !upload_uses_kms {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "KMS headers require an SSE-KMS upload",
        ));
    }
    Ok(())
}

/// Checks the raw headers because the S3 parser treats an empty algorithm as absent.
pub(super) fn validate_no_unsupported_sse_headers(headers: &HeaderMap) -> S3Result<()> {
    if [SSE_HEADER, KMS_KEY_HEADER, KMS_CONTEXT_HEADER]
        .iter()
        .any(|name| headers.get(*name).is_some_and(HeaderValue::is_empty))
        || UNSUPPORTED_SSE_HEADERS
            .iter()
            .any(|header| headers.contains_key(*header))
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "unsupported encryption headers",
        ));
    }
    Ok(())
}

/// Explicit encryption settings supplied with an object write.
#[derive(Clone, Debug)]
pub(super) struct WriteEncryption {
    mode: EncryptionMode,
    context: Option<String>,
}

/// Validates the encryption algorithm and its companion headers before writing any data.
pub(super) fn requested_encryption(
    algorithm: Option<&ServerSideEncryption>,
    key_id: Option<&str>,
    context: Option<&str>,
    customer_settings: bool,
    bucket_key: bool,
) -> S3Result<Option<WriteEncryption>> {
    if !customer_settings && !bucket_key {
        match (algorithm.map(ServerSideEncryption::as_str), key_id, context) {
            (None, None, None) => return Ok(None),
            (Some("AES256"), None, None) => {
                return Ok(Some(WriteEncryption {
                    mode: EncryptionMode::Aes256,
                    context: None,
                }));
            }
            (Some("aws:kms"), Some(id), _) if !id.is_empty() => {
                return Ok(Some(WriteEncryption {
                    mode: EncryptionMode::Kms(id.to_owned()),
                    context: context.map(str::to_owned),
                }));
            }
            _ => {}
        }
    }
    Err(s3_error_with_message(
        S3ErrorCode::InvalidArgument,
        "invalid encryption settings or missing KMS key ID",
    ))
}

/// Resolves the bucket default or explicit encryption and creates a fresh data key.
pub(super) async fn resolve_write_encryption(
    sqlite: &Sqlite,
    bucket: &str,
    key: &str,
    explicit: Option<&WriteEncryption>,
) -> S3Result<Option<ActiveSse>> {
    let mode = if let Some(settings) = explicit {
        Some(settings.mode.clone())
    } else {
        sqlite
            .get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?
            .read(Sqlite::get_bucket_encryption)
            .await
            .map_err(s3_error_from_sqlite)?
    };
    match mode {
        Some(EncryptionMode::Aes256) => {
            new_active_sse(&require_sse_s3_master_key(sqlite)?, bucket, key)
                .map(Some)
                .map_err(|_| s3_error_with_message(InternalError, "SSE-S3 encryption failed"))
        }
        Some(EncryptionMode::Kms(id)) => kms::generate(
            &sqlite.config,
            bucket,
            key,
            &id,
            explicit.and_then(|s| s.context.as_deref()),
        )
        .await
        .map(Some),
        None => Ok(None),
    }
}

/// Reopens the persisted encryption envelope of `bucket`/`key` outside the storage transaction.
pub(super) async fn open_encryption(
    sqlite: &Sqlite,
    bucket: &str,
    key: &str,
    stored: StoredSse,
) -> S3Result<ActiveSse> {
    match &stored {
        StoredSse::Local { .. } => {
            active_sse_from_stored(&require_sse_s3_master_key(sqlite)?, stored, bucket, key)
                .map_err(|_| s3_error_with_message(InternalError, "SSE-S3 decryption failed"))
        }
        StoredSse::Kms {
            key_id,
            ciphertext,
            context,
        } => {
            let data_key = kms::decrypt(&sqlite.config, key_id, ciphertext, context).await?;
            Ok(ActiveSse { stored, data_key })
        }
    }
}

/// Maps the stored encryption mode to response headers.
pub(super) fn encryption_headers(
    mode: Option<EncryptionMode>,
) -> (Option<ServerSideEncryption>, Option<String>) {
    match mode {
        Some(EncryptionMode::Aes256) => (
            Some(ServerSideEncryption::from_static(SSE_S3_ALGORITHM)),
            None,
        ),
        Some(EncryptionMode::Kms(id)) => {
            (Some(ServerSideEncryption::from_static("aws:kms")), Some(id))
        }
        None => (None, None),
    }
}

/// Returns the configured SSE-S3 master key or a protocol error.
pub(super) fn require_sse_s3_master_key(sqlite: &Sqlite) -> S3Result<[u8; 32]> {
    sqlite.config.sse_s3_master_key().ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidRequest, "SSE-S3 is not configured")
    })
}

#[cfg(test)]
/// Encrypts a plaintext value into storage chunks.
pub(super) fn encrypt_value_chunks(
    value: &[u8],
    chunk_size: usize,
    encryption: &ActiveSse,
) -> S3Result<Vec<EncodedBlobChunk>> {
    if chunk_size == 0 {
        return Err(S3Error::with_message(
            InternalError,
            "object blob chunk size must be greater than zero",
        ));
    }
    let mut chunks = Vec::new();
    let mut next_offset = 0_i64;
    let part_size = i64::try_from(value.len()).map_err(S3Error::internal_error)?;
    for plaintext in value.chunks(chunk_size) {
        push_encrypted_chunk(
            &mut chunks,
            &mut next_offset,
            plaintext,
            (encryption, part_size),
        )?;
    }
    Ok(chunks)
}

#[cfg(test)]
/// Decrypts authenticated storage chunks into plaintext bytes.
pub(super) fn decrypt_stored_chunks(
    encryption: &ActiveSse,
    chunks: Vec<StoredBlobChunk>,
) -> SqliteResult<Vec<u8>> {
    // Test values are stored as a single part covering every chunk.
    let part_size = chunks.iter().map(|chunk| chunk.size).sum();
    let mut value = Vec::new();
    let mut expected_offset = 0_i64;
    for (index, chunk) in chunks.into_iter().enumerate() {
        let position = ChunkPosition {
            part_number: 1,
            chunk_number: i32::try_from(index + 1)
                .map_err(|_| SqliteError::ObjectBodyInconsistent)?,
            part_size,
        };
        if chunk.offset != expected_offset {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        let (Some(nonce), Some(tag)) = (chunk.sse_nonce.as_deref(), chunk.sse_tag.as_deref())
        else {
            return Err(SqliteError::ObjectBodyInconsistent);
        };
        let plaintext = decrypt_chunk(encryption, position, nonce, tag, &chunk.body)
            .map_err(SqliteError::Configuration)?;
        if i64::try_from(plaintext.len()).map_err(|_| SqliteError::ObjectBodyInconsistent)?
            != chunk.size
        {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        value.extend_from_slice(&plaintext);
        expected_offset = expected_offset
            .checked_add(chunk.size)
            .ok_or(SqliteError::ObjectBodyInconsistent)?;
    }
    Ok(value)
}

#[cfg(test)]
/// Encrypts one plaintext chunk of a single-part value of `part_size` bytes and appends it.
fn push_encrypted_chunk(
    chunks: &mut Vec<EncodedBlobChunk>,
    next_offset: &mut i64,
    plaintext: &[u8],
    (encryption, part_size): (&ActiveSse, i64),
) -> S3Result<()> {
    let position = ChunkPosition {
        part_number: 1,
        chunk_number: i32::try_from(chunks.len() + 1).map_err(S3Error::internal_error)?,
        part_size,
    };
    let chunk = encode_chunk(Some(encryption), position, *next_offset, plaintext.to_vec())?;
    *next_offset = next_offset
        .checked_add(chunk.size)
        .ok_or_else(|| S3Error::with_message(InternalError, "upload chunk offset overflow"))?;
    chunks.push(chunk);

    Ok(())
}

/// Encodes one plaintext storage chunk, encrypting it when `encryption` is set.
///
/// The stored `size` is always the plaintext length; AES-GCM ciphertext has the
/// same length, with the nonce and tag kept in their own columns.
pub(super) fn encode_chunk(
    encryption: Option<&ActiveSse>,
    position: ChunkPosition,
    offset: i64,
    plaintext: Vec<u8>,
) -> S3Result<EncodedBlobChunk> {
    let chunk_number = position.chunk_number;
    let size = i64::try_from(plaintext.len()).map_err(S3Error::internal_error)?;
    let Some(encryption) = encryption else {
        return Ok(EncodedBlobChunk {
            chunk_number,
            offset,
            size,
            body: plaintext,
            sse_nonce: None,
            sse_tag: None,
        });
    };
    let encrypted = encrypt_chunk(encryption, position, &plaintext)
        .map_err(|_| S3Error::with_message(InternalError, "object chunk encryption failed"))?;
    Ok(EncodedBlobChunk {
        chunk_number,
        offset,
        size,
        body: encrypted.ciphertext,
        sse_nonce: Some(encrypted.nonce),
        sse_tag: Some(encrypted.tag),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use s3s::S3Error;
    use tokio::sync::RwLock;

    use super::*;
    use crate::{Config, sqlite::DEFAULT_OBJECT_BLOB_CHUNK_SIZE};

    /// Builds storage with custom configuration for encryption tests.
    fn sqlite_with_config(config: Config) -> Sqlite {
        Sqlite {
            root: PathBuf::new(),
            config,
            buckets: Arc::new(RwLock::new(HashMap::new())),
            bucket_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            blob_chunk_size: DEFAULT_OBJECT_BLOB_CHUNK_SIZE,
            requests: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    /// Checks the operations that reject write-time encryption headers.
    #[test]
    fn rejects_server_side_encryption_headers() {
        let mut headers = HeaderMap::new();
        assert!(validate_no_sse_headers(&headers).is_ok());

        headers.insert(SSE_HEADER, HeaderValue::from_static("AES256"));
        let error = validate_no_sse_headers(&headers).err();
        assert!(matches!(
            error.as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
        assert!(
            format!("{error:?}").contains("not valid for this operation"),
            "error should explain operation-level SSE rejection"
        );

        for header in UNSUPPORTED_SSE_HEADERS {
            headers.clear();
            headers.insert(*header, HeaderValue::from_static("AES256"));
            let error = validate_no_sse_headers(&headers).err();
            assert!(matches!(
                error.as_ref().map(S3Error::code),
                Some(&S3ErrorCode::InvalidArgument)
            ));
            assert!(
                format!("{error:?}").contains("unsupported encryption headers"),
                "error should explain unsupported SSE header {header}"
            );
        }
    }

    /// Rejects a wrapped key with an invalid plaintext length or a damaged authentication tag.
    #[test]
    fn rejects_malformed_wrapped_data_key() -> Result<(), String> {
        let master_key = [0; MASTER_KEY_LEN];
        let nonce = [0; NONCE_LEN];
        let encrypted = encrypt_bytes(&master_key, &nonce, b"short", &object_aad("bucket", "key"))?;
        let (ciphertext, tag) = split_ciphertext_tag(encrypted)?;
        let mut stored = StoredSse::Local {
            nonce: nonce.to_vec(),
            ciphertext,
            tag,
        };
        assert_eq!(
            active_sse_from_stored(&master_key, stored.clone(), "bucket", "key")
                .err()
                .as_deref(),
            Some("invalid SSE data key length: 5")
        );
        if let StoredSse::Local { tag, .. } = &mut stored {
            tag[0] ^= 1;
        }
        assert!(active_sse_from_stored(&master_key, stored, "bucket", "key").is_err());
        Ok(())
    }

    #[test]
    fn parses_master_key_and_rejects_bad_lengths() {
        assert!(parse_master_key("not base64").is_err());
        assert!(parse_master_key("AQID").is_err());
    }

    /// Checks missing local key configuration and SSE-S3 response headers.
    #[test]
    fn sse_master_key_helpers_reject_missing_config_and_map_output_header() {
        let sqlite = sqlite_with_config(Config::default());
        assert!(matches!(
            require_sse_s3_master_key(&sqlite)
                .err()
                .as_ref()
                .map(S3Error::code),
            Some(&S3ErrorCode::InvalidRequest)
        ));
        assert_eq!(
            encryption_headers(Some(EncryptionMode::Aes256))
                .0
                .as_ref()
                .map(ServerSideEncryption::as_str),
            Some(ServerSideEncryption::AES256)
        );
        assert!(encryption_headers(None).0.is_none());
    }

    /// Rejects explicit local encryption when no master key is configured.
    #[tokio::test]
    async fn resolve_write_encryption_rejects_sse_without_master_key() {
        let sqlite = sqlite_with_config(Config::default());
        assert!(matches!(
            resolve_write_encryption(
                &sqlite,
                "bucket",
                "key",
                Some(&WriteEncryption {
                    mode: EncryptionMode::Aes256,
                    context: None
                })
            )
            .await
            .err()
            .as_ref()
            .map(S3Error::code),
            Some(&S3ErrorCode::InvalidRequest)
        ));
    }

    /// Checks encryption algorithms and their required companion settings.
    #[test]
    fn rejects_unsupported_write_encryption_inputs() -> S3Result<()> {
        assert!(matches!(
            requested_encryption(None, Some("key"), None, false, false)
                .err()
                .as_ref()
                .map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
        assert!(matches!(
            requested_encryption(None, None, None, true, false)
                .err()
                .as_ref()
                .map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
        assert!(matches!(
            requested_encryption(None, None, None, false, true)
                .err()
                .as_ref()
                .map(S3Error::code),
            Some(&S3ErrorCode::InvalidArgument)
        ));
        assert!(
            requested_encryption(
                Some(&ServerSideEncryption::from_static(
                    ServerSideEncryption::AES256
                )),
                None,
                None,
                false,
                false,
            )?
            .is_some()
        );
        assert!(requested_encryption(None, None, None, false, false)?.is_none());

        Ok(())
    }

    /// Rejects malformed local envelopes and incomplete encrypted chunks.
    #[test]
    fn encrypt_and_decrypt_chunk_helpers_reject_inconsistent_input() -> S3Result<()> {
        let encryption = new_active_sse(&[6; 32], "bucket", "key")
            .map_err(|err| S3Error::with_message(InternalError, err))?;

        assert!(split_ciphertext_tag(vec![1, 2, 3]).is_err());
        assert!(encrypt_value_chunks(b"value", 0, &encryption).is_err());
        assert!(
            active_sse_from_stored(
                &[0; 32],
                StoredSse::Local {
                    nonce: vec![0],
                    ciphertext: vec![0; 32],
                    tag: vec![0; 16],
                },
                "bucket",
                "key",
            )
            .is_err()
        );
        let mut chunks = Vec::new();
        let mut next_offset = i64::MAX;
        assert!(
            push_encrypted_chunk(&mut chunks, &mut next_offset, b"x", (&encryption, 1)).is_err()
        );

        let chunks = encrypt_value_chunks(b"abcdef", 3, &encryption)?;
        let mut stored = chunks
            .into_iter()
            .map(StoredBlobChunk::from)
            .collect::<Vec<_>>();
        stored[0].offset = 1;
        assert!(matches!(
            decrypt_stored_chunks(&encryption, stored),
            Err(SqliteError::ObjectBodyInconsistent)
        ));

        let mut stored = encrypt_value_chunks(b"abcdef", 3, &encryption)?
            .into_iter()
            .map(StoredBlobChunk::from)
            .collect::<Vec<_>>();
        stored[0].sse_nonce = None;
        assert!(matches!(
            decrypt_stored_chunks(&encryption, stored),
            Err(SqliteError::ObjectBodyInconsistent)
        ));

        let mut stored = encrypt_value_chunks(b"abcdef", 3, &encryption)?
            .into_iter()
            .map(StoredBlobChunk::from)
            .collect::<Vec<_>>();
        stored[0].size += 1;
        // The part size is authenticated, so this now fails to decrypt
        // before the plaintext length check runs.
        assert!(decrypt_stored_chunks(&encryption, stored).is_err());

        Ok(())
    }

    /// Rejects chunks moved within or between parts, or read against another part size.
    #[test]
    fn chunks_only_decrypt_at_their_position() -> Result<(), String> {
        let encryption = new_active_sse(&[8; 32], "bucket", "key")?;
        let position = ChunkPosition {
            part_number: 2,
            chunk_number: 3,
            part_size: 100,
        };
        let chunk = encrypt_chunk(&encryption, position, b"chunk")?;
        let open = |position| {
            decrypt_chunk(
                &encryption,
                position,
                &chunk.nonce,
                &chunk.tag,
                &chunk.ciphertext,
            )
        };
        assert_eq!(open(position)?, b"chunk");
        for moved in [
            ChunkPosition {
                part_number: 1,
                ..position
            },
            ChunkPosition {
                chunk_number: 2,
                ..position
            },
            ChunkPosition {
                part_size: 99,
                ..position
            },
        ] {
            assert!(open(moved).is_err(), "{moved:?}");
        }
        Ok(())
    }

    /// Rejects a wrapped data key moved to another object's row.
    #[test]
    fn wrapped_data_keys_only_open_for_their_object() -> Result<(), String> {
        let master_key = [9; MASTER_KEY_LEN];
        let encryption = new_active_sse(&master_key, "bucket", "key")?;
        let opened =
            active_sse_from_stored(&master_key, encryption.stored.clone(), "bucket", "key")?;
        assert_eq!(opened.data_key, encryption.data_key);
        for (bucket, key) in [("bucket", "other"), ("other", "key")] {
            assert!(
                active_sse_from_stored(&master_key, encryption.stored.clone(), bucket, key)
                    .is_err()
            );
        }
        Ok(())
    }
}
