//! SSE-S3 envelope encryption helpers.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};

pub(crate) const SSE_S3_ALGORITHM_CODE: i64 = 1;
pub(crate) const SSE_S3_ALGORITHM: &str = "AES256";

const MASTER_KEY_LEN: usize = 32;
const DATA_KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// Stored object/upload envelope encryption metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredSse {
    pub(crate) key_nonce: Vec<u8>,
    pub(crate) key_ciphertext: Vec<u8>,
    pub(crate) key_tag: Vec<u8>,
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
    let (key_ciphertext, key_tag) = split_ciphertext_tag(encrypted)?;
    Ok(ActiveSse {
        stored: StoredSse {
            key_nonce: nonce.to_vec(),
            key_ciphertext,
            key_tag,
        },
        data_key,
    })
}

/// Unwraps a stored object/upload data key.
pub(crate) fn active_sse_from_stored(
    master_key: &[u8; MASTER_KEY_LEN],
    stored: StoredSse,
) -> Result<ActiveSse, String> {
    let nonce = nonce_array(&stored.key_nonce)?;
    let encrypted = join_ciphertext_tag(&stored.key_ciphertext, &stored.key_tag);
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
