//! SSE-S3 request body chunk encryption for object writes.

use futures::StreamExt;
use md5::{Digest, Md5};
use s3s::{S3Error, S3ErrorCode::InternalError, S3Result};

use crate::{
    checksum::{ChecksumAccumulator, MultipartChecksumConfig},
    sqlite::{EncodedBlobChunk, PartChecksum, StoredChecksum},
    sse,
};

use super::{
    ChecksumInput, ChecksumPolicy, ChecksumTypeKind, StreamingBlob, finish_part_checksum,
    finish_payload_checksum, hex, part_checksum_accumulator, payload_checksum_accumulator,
    validate_content_md5,
};

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
    encryption: &sse::ActiveSse,
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
    encryption: &sse::ActiveSse,
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

/// Encrypts a streaming request body into ordered storage chunks.
async fn encrypt_streaming_body(
    mut body: StreamingBlob,
    chunk_size: usize,
    encryption: &sse::ActiveSse,
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
    encryption: &sse::ActiveSse,
) -> S3Result<()> {
    let chunk_number = i32::try_from(chunks.len() + 1).map_err(S3Error::internal_error)?;
    let size = i64::try_from(plaintext.len()).map_err(S3Error::internal_error)?;
    let encrypted = sse::encrypt_chunk(encryption, plaintext)
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

    use crate::checksum::{ChecksumAlgorithmKind, ChecksumTypeKind};

    use super::*;

    #[tokio::test]
    async fn object_body_encrypts_chunks_that_decrypt() -> S3Result<()> {
        let encryption = sse::new_active_sse(&[7; 32]).expect("test SSE key should be valid");
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
        let mut plaintext = Vec::new();
        for chunk in object.chunks {
            assert_ne!(chunk.body, b"abc");
            let nonce = chunk.sse_nonce.as_deref().expect("encrypted chunk nonce");
            let tag = chunk.sse_tag.as_deref().expect("encrypted chunk tag");
            plaintext.extend(
                sse::decrypt_chunk(&encryption, nonce, tag, &chunk.body)
                    .expect("encrypted chunk should decrypt"),
            );
        }
        assert_eq!(plaintext, b"abcdefghi");

        Ok(())
    }

    #[tokio::test]
    async fn part_body_encrypts_and_computes_configured_checksum() -> S3Result<()> {
        let encryption = sse::new_active_sse(&[8; 32]).expect("test SSE key should be valid");
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

        let checksum = part.checksum.expect("configured part checksum");
        assert_eq!(checksum.algorithm(), ChecksumAlgorithmKind::Sha256);
        assert_eq!(part.size, 11);
        assert_eq!(part.chunks.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn object_body_rejects_bad_content_md5() {
        let encryption = sse::new_active_sse(&[9; 32]).expect("test SSE key should be valid");
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
    }

    fn streaming_blob(chunks: &[&'static [u8]]) -> StreamingBlob {
        let chunks = chunks
            .iter()
            .map(|chunk| Ok::<_, S3Error>(Bytes::from_static(chunk)))
            .collect::<Vec<_>>();
        StreamingBlob::wrap::<_, S3Error>(stream::iter(chunks).fuse())
    }
}
