//! S3 SSE-S3 envelope crypto, request handling, response mapping, and encrypted body encoding.

use std::ops::Not;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::StreamExt;
use hyper::http::HeaderMap;
use md5::{Digest, Md5};
use s3s::{S3Error, S3ErrorCode, S3Result, dto::ServerSideEncryption};

use crate::sqlite::{EncodedBlobChunk, Sqlite, SqliteError, SqliteResult, StoredBlobChunk};

use super::{
    ChecksumAccumulator, ChecksumInput, ChecksumPolicy, ChecksumTypeKind, InternalError,
    MultipartChecksumConfig, PartChecksum, StoredChecksum, StreamingBlob, finish_part_checksum,
    finish_payload_checksum, hex, part_checksum_accumulator, payload_checksum_accumulator,
    s3_error_from_sqlite, s3_error_with_message, validate_content_md5,
};

pub(crate) const SSE_S3_ALGORITHM_CODE: i64 = 1;

const MASTER_KEY_LEN: usize = 32;
const DATA_KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// Stored object/upload envelope encryption metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredSse {
    pub(crate) nonce: Vec<u8>,
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) tag: Vec<u8>,
}

/// Encryption state for a new write or existing multipart upload.
#[derive(Clone, Debug)]
pub(crate) struct ActiveSse {
    pub(crate) stored: StoredSse,
    data_key: [u8; DATA_KEY_LEN],
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

/// Creates a new data key and wraps it with the configured master key.
pub(crate) fn new_active_sse(master_key: &[u8; MASTER_KEY_LEN]) -> Result<ActiveSse, String> {
    let mut data_key = [0; DATA_KEY_LEN];
    getrandom::fill(&mut data_key).map_err(|err| err.to_string())?;

    let mut nonce = [0; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|err| err.to_string())?;

    let encrypted = encrypt_bytes(master_key, &nonce, &data_key)?;
    let (ciphertext, tag) = split_ciphertext_tag(encrypted)?;
    Ok(ActiveSse {
        stored: StoredSse {
            nonce: nonce.to_vec(),
            ciphertext,
            tag,
        },
        data_key,
    })
}

/// Unwraps a stored object/upload data key.
pub(crate) fn active_sse_from_stored(
    master_key: &[u8; MASTER_KEY_LEN],
    stored: StoredSse,
) -> Result<ActiveSse, String> {
    let nonce = nonce_array(&stored.nonce)?;
    let encrypted = join_ciphertext_tag(&stored.ciphertext, &stored.tag);
    let data_key = decrypt_bytes(master_key, &nonce, &encrypted)?;
    let data_key = data_key
        .try_into()
        .map_err(|bytes: Vec<u8>| format!("invalid SSE data key length: {}", bytes.len()))?;
    Ok(ActiveSse { stored, data_key })
}

/// Encrypts one object storage chunk.
pub(crate) fn encrypt_chunk(
    active: &ActiveSse,
    plaintext: &[u8],
) -> Result<EncryptedChunk, String> {
    let mut nonce = [0; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|err| err.to_string())?;
    let encrypted = encrypt_bytes(&active.data_key, &nonce, plaintext)?;
    let (ciphertext, tag) = split_ciphertext_tag(encrypted)?;
    Ok(EncryptedChunk {
        ciphertext,
        nonce: nonce.to_vec(),
        tag,
    })
}

/// Decrypts one object storage chunk.
pub(crate) fn decrypt_chunk(
    active: &ActiveSse,
    nonce: &[u8],
    tag: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    let nonce = nonce_array(nonce)?;
    let encrypted = join_ciphertext_tag(ciphertext, tag);
    decrypt_bytes(&active.data_key, &nonce, &encrypted)
}

/// Encrypts bytes with AES-256-GCM and returns ciphertext plus tag.
fn encrypt_bytes(
    key: &[u8; MASTER_KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|err| err.to_string())?;
    cipher
        .encrypt(Nonce::from_slice(nonce), plaintext)
        .map_err(|err| err.to_string())
}

/// Decrypts bytes produced by `encrypt_bytes`.
fn decrypt_bytes(
    key: &[u8; MASTER_KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|err| err.to_string())?;
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
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
const SSE_S3_ALGORITHM: &str = "AES256";
const UNSUPPORTED_SSE_HEADERS: &[&str] = &[
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-context",
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
    if headers.contains_key(SSE_HEADER) {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "server-side encryption headers are not valid for this operation",
        ));
    }
    validate_no_unsupported_sse_headers(headers)
}

/// Rejects SSE variants this backend does not implement.
pub(super) fn validate_no_unsupported_sse_headers(headers: &HeaderMap) -> S3Result<()> {
    if UNSUPPORTED_SSE_HEADERS
        .iter()
        .any(|header| headers.contains_key(*header))
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "only SSE-S3 with AES256 is supported",
        ));
    }
    Ok(())
}

/// Validates object-write SSE settings and returns whether AES256 was requested.
pub(super) fn requested_sse_s3(
    server_side_encryption: Option<&ServerSideEncryption>,
    has_kms_settings: bool,
    has_sse_customer_settings: bool,
    has_bucket_key_setting: bool,
) -> S3Result<bool> {
    if has_sse_customer_settings || has_kms_settings || has_bucket_key_setting {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "only SSE-S3 with AES256 is supported",
        ));
    }

    match server_side_encryption {
        Some(algorithm) if algorithm.as_str() == SSE_S3_ALGORITHM => Ok(true),
        Some(_) => Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "server-side encryption must be AES256",
        )),
        None => Ok(false),
    }
}

/// Resolves explicit SSE-S3 and bucket default encryption for an object write.
pub(super) async fn resolve_write_encryption(
    sqlite: &Sqlite,
    bucket: &str,
    explicit_sse_s3: bool,
) -> S3Result<Option<ActiveSse>> {
    let use_sse_s3 = if explicit_sse_s3 {
        true
    } else {
        let connection = sqlite
            .get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .read(|connection| {
                let transaction = connection.transaction()?;
                Sqlite::get_bucket_default_sse_s3(&transaction)
            })
            .await
            .map_err(s3_error_from_sqlite)?
    };

    if use_sse_s3.not() {
        return Ok(None);
    }

    let master_key = sqlite.config.sse_s3_master_key().ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidRequest, "SSE-S3 is not configured")
    })?;
    new_active_sse(&master_key)
        .map(Some)
        .map_err(|_| s3_error_with_message(InternalError, "SSE-S3 encryption failed"))
}

/// Returns the response enum for encrypted object outputs.
pub(super) fn sse_s3_output(encrypted: bool) -> Option<ServerSideEncryption> {
    encrypted.then(|| ServerSideEncryption::from_static(ServerSideEncryption::AES256))
}

/// Returns the configured SSE-S3 master key or a protocol error.
pub(super) fn require_sse_s3_master_key(sqlite: &Sqlite) -> S3Result<[u8; 32]> {
    sqlite.config.sse_s3_master_key().ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidRequest, "SSE-S3 is not configured")
    })
}

/// Encrypted object body chunks plus checksums needed for `PutObject`.
#[derive(Debug)]
pub(super) struct EncryptedObjectBody {
    pub(super) chunks: Vec<EncodedBlobChunk>,
    pub(super) size: i64,
    pub(super) md5: String,
    pub(super) checksum: StoredChecksum,
}

/// Encrypted multipart part chunks plus checksums needed for `UploadPart`.
#[derive(Debug)]
pub(super) struct EncryptedPartBody {
    pub(super) chunks: Vec<EncodedBlobChunk>,
    pub(super) size: i64,
    pub(super) md5: String,
    pub(super) checksum: Option<PartChecksum>,
}

struct EncryptedChunks {
    chunks: Vec<EncodedBlobChunk>,
    size: i64,
}

/// Reads a `PutObject` body into encrypted storage chunks without retaining plaintext.
pub(super) async fn read_object_body_encrypted(
    body: StreamingBlob,
    content_md5_present: bool,
    content_md5: Option<&str>,
    checksum_input: ChecksumInput,
    chunk_size: usize,
    encryption: &ActiveSse,
) -> S3Result<EncryptedObjectBody> {
    let mut md5_hash = Md5::new();
    let mut checksum_accumulator =
        payload_checksum_accumulator(&checksum_input, ChecksumPolicy::DefaultCrc64NvmeFullObject)?;
    let encrypted = encrypt_streaming_body(body, chunk_size, encryption, |bytes| {
        md5_hash.update(bytes);
        checksum_accumulator.update(bytes);
    })
    .await?;
    let md5_bytes = md5_hash.finalize();
    let md5 = hex(md5_bytes);

    validate_content_md5(content_md5_present, content_md5, md5_bytes.as_ref())?;
    let checksum = finish_payload_checksum(
        &checksum_input,
        checksum_accumulator,
        ChecksumTypeKind::FullObject,
    )?;

    Ok(EncryptedObjectBody {
        chunks: encrypted.chunks,
        size: encrypted.size,
        md5,
        checksum,
    })
}

/// Reads an `UploadPart` body into encrypted storage chunks without retaining plaintext.
pub(super) async fn read_part_body_encrypted(
    body: StreamingBlob,
    checksum_input: &ChecksumInput,
    upload_checksum: Option<MultipartChecksumConfig>,
    content_md5_present: bool,
    content_md5: Option<&str>,
    chunk_size: usize,
    encryption: &ActiveSse,
) -> S3Result<EncryptedPartBody> {
    let mut explicit_accumulator = part_checksum_accumulator(checksum_input)?;
    let mut configured_accumulator = explicit_accumulator
        .is_none()
        .then(|| upload_checksum.map(|checksum| ChecksumAccumulator::new(checksum.algorithm)))
        .flatten();
    let mut md5_hash = Md5::new();
    let encrypted = encrypt_streaming_body(body, chunk_size, encryption, |bytes| {
        md5_hash.update(bytes);
        if let Some(accumulator) = &mut explicit_accumulator {
            accumulator.update(bytes);
        }
        if let Some(accumulator) = &mut configured_accumulator {
            accumulator.update(bytes);
        }
    })
    .await?;
    let md5_bytes = md5_hash.finalize();
    let md5 = hex(md5_bytes);

    validate_content_md5(content_md5_present, content_md5, md5_bytes.as_ref())?;
    let explicit_checksum = finish_part_checksum(checksum_input, explicit_accumulator)?;
    let checksum =
        explicit_checksum.or_else(|| configured_accumulator.map(ChecksumAccumulator::finish_part));

    Ok(EncryptedPartBody {
        chunks: encrypted.chunks,
        size: encrypted.size,
        md5,
        checksum,
    })
}

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
    for plaintext in value.chunks(chunk_size) {
        push_encrypted_chunk(&mut chunks, &mut next_offset, plaintext, encryption)?;
    }
    Ok(chunks)
}

/// Decrypts stored SSE-S3 chunks into plaintext bytes.
pub(super) fn decrypt_stored_chunks(
    encryption: &ActiveSse,
    chunks: Vec<StoredBlobChunk>,
) -> SqliteResult<Vec<u8>> {
    let mut value = Vec::new();
    let mut expected_offset = 0_i64;
    for chunk in chunks {
        if chunk.offset != expected_offset {
            return Err(SqliteError::ObjectBodyInconsistent);
        }
        let (Some(nonce), Some(tag)) = (chunk.sse_nonce.as_deref(), chunk.sse_tag.as_deref())
        else {
            return Err(SqliteError::ObjectBodyInconsistent);
        };
        let plaintext = decrypt_chunk(encryption, nonce, tag, &chunk.body)
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

/// Encrypts a streaming request body into ordered storage chunks.
async fn encrypt_streaming_body(
    mut body: StreamingBlob,
    chunk_size: usize,
    encryption: &ActiveSse,
    mut observe_plaintext: impl FnMut(&[u8]),
) -> S3Result<EncryptedChunks> {
    if chunk_size == 0 {
        return Err(S3Error::with_message(
            InternalError,
            "object blob chunk size must be greater than zero",
        ));
    }

    let mut chunks = Vec::new();
    let mut buffer = Vec::new();
    let mut total_size = 0_i64;
    let mut next_offset = 0_i64;

    while let Some(result) = body.next().await {
        let bytes = result.map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
        observe_plaintext(bytes.as_ref());
        total_size = add_body_len(total_size, bytes.len())?;

        let mut remaining = bytes.as_ref();
        while !remaining.is_empty() {
            if buffer.is_empty() && remaining.len() >= chunk_size {
                let (chunk, rest) = remaining.split_at(chunk_size);
                push_encrypted_chunk(&mut chunks, &mut next_offset, chunk, encryption)?;
                remaining = rest;
                continue;
            }

            let available = chunk_size
                .checked_sub(buffer.len())
                .ok_or_else(|| S3Error::with_message(InternalError, "upload chunk overflow"))?;
            let take = available.min(remaining.len());
            buffer.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];

            if buffer.len() == chunk_size {
                push_encrypted_chunk(&mut chunks, &mut next_offset, &buffer, encryption)?;
                buffer.clear();
            }
        }
    }

    if !buffer.is_empty() {
        push_encrypted_chunk(&mut chunks, &mut next_offset, &buffer, encryption)?;
    }
    if next_offset != total_size {
        return Err(S3Error::with_message(
            InternalError,
            "encrypted upload size mismatch",
        ));
    }

    Ok(EncryptedChunks {
        chunks,
        size: total_size,
    })
}

/// Encrypts and records one plaintext storage chunk at the next object offset.
fn push_encrypted_chunk(
    chunks: &mut Vec<EncodedBlobChunk>,
    next_offset: &mut i64,
    plaintext: &[u8],
    encryption: &ActiveSse,
) -> S3Result<()> {
    let chunk_number = i32::try_from(chunks.len() + 1).map_err(S3Error::internal_error)?;
    let size = i64::try_from(plaintext.len()).map_err(S3Error::internal_error)?;
    let encrypted = encrypt_chunk(encryption, plaintext)
        .map_err(|_| S3Error::with_message(InternalError, "SSE-S3 encryption failed"))?;
    chunks.push(EncodedBlobChunk {
        chunk_number,
        offset: *next_offset,
        size,
        body: encrypted.ciphertext,
        sse_nonce: Some(encrypted.nonce),
        sse_tag: Some(encrypted.tag),
    });
    *next_offset = next_offset
        .checked_add(size)
        .ok_or_else(|| S3Error::with_message(InternalError, "upload chunk offset overflow"))?;

    Ok(())
}

/// Adds a streamed body fragment length to the plaintext object size.
fn add_body_len(total_size: i64, len: usize) -> S3Result<i64> {
    let len = i64::try_from(len).map_err(S3Error::internal_error)?;
    total_size
        .checked_add(len)
        .ok_or_else(|| S3Error::with_message(InternalError, "upload body size overflow"))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::{StreamExt as _, stream};
    use hyper::http::HeaderValue;
    use s3s::S3Error;

    use super::*;
    use crate::s3::{ChecksumAlgorithmKind, ChecksumTypeKind};

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
                format!("{error:?}").contains("only SSE-S3 with AES256 is supported"),
                "error should explain unsupported SSE header {header}"
            );
        }
    }

    #[tokio::test]
    async fn object_body_encrypts_chunks_that_decrypt() -> S3Result<()> {
        let encryption =
            new_active_sse(&[7; 32]).map_err(|err| S3Error::with_message(InternalError, err))?;
        let object = read_object_body_encrypted(
            streaming_blob(&[b"ab", b"cdef", b"ghi"]),
            false,
            None,
            ChecksumInput::default(),
            3,
            &encryption,
        )
        .await?;

        assert_eq!(object.size, 9);
        assert_eq!(object.chunks.len(), 3);
        let plaintext = decrypt_stored_chunks(
            &encryption,
            object
                .chunks
                .into_iter()
                .map(StoredBlobChunk::from)
                .collect(),
        )
        .map_err(S3Error::internal_error)?;
        assert_eq!(plaintext, b"abcdefghi");

        Ok(())
    }

    #[tokio::test]
    async fn part_body_encrypts_and_computes_configured_checksum() -> S3Result<()> {
        let encryption =
            new_active_sse(&[8; 32]).map_err(|err| S3Error::with_message(InternalError, err))?;
        let part = read_part_body_encrypted(
            streaming_blob(&[b"hello", b" ", b"world"]),
            &ChecksumInput::default(),
            Some(MultipartChecksumConfig::new(
                ChecksumAlgorithmKind::Sha256,
                ChecksumTypeKind::Composite,
            )),
            false,
            None,
            4,
            &encryption,
        )
        .await?;

        let Some(checksum) = part.checksum else {
            return Err(S3Error::with_message(
                InternalError,
                "configured part checksum is missing",
            ));
        };
        assert_eq!(checksum.algorithm(), ChecksumAlgorithmKind::Sha256);
        assert_eq!(part.size, 11);
        assert_eq!(part.chunks.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn object_body_rejects_bad_content_md5() -> S3Result<()> {
        let encryption =
            new_active_sse(&[9; 32]).map_err(|err| S3Error::with_message(InternalError, err))?;
        let result = read_object_body_encrypted(
            streaming_blob(&[b"abc"]),
            true,
            Some("AAAAAAAAAAAAAAAAAAAAAA=="),
            ChecksumInput::default(),
            3,
            &encryption,
        )
        .await;

        assert!(result.is_err());
        Ok(())
    }

    fn streaming_blob(chunks: &[&'static [u8]]) -> StreamingBlob {
        let chunks = chunks
            .iter()
            .map(|chunk| Ok::<_, S3Error>(Bytes::from_static(chunk)))
            .collect::<Vec<_>>();
        StreamingBlob::wrap::<_, S3Error>(stream::iter(chunks).fuse())
    }
}
