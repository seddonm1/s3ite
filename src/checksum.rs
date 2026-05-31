use std::ops::Not;

use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};

pub(crate) const CHECKSUM_CRC32: &str = "CRC32";
pub(crate) const CHECKSUM_CRC32C: &str = "CRC32C";
pub(crate) const CHECKSUM_CRC64NVME: &str = "CRC64NVME";
pub(crate) const CHECKSUM_SHA1: &str = "SHA1";
pub(crate) const CHECKSUM_SHA256: &str = "SHA256";
pub(crate) const CHECKSUM_TYPE_COMPOSITE: &str = "COMPOSITE";
pub(crate) const CHECKSUM_TYPE_FULL_OBJECT: &str = "FULL_OBJECT";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChecksumAlgorithmKind {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithmKind {
    pub(crate) fn from_name(algorithm: &str) -> Option<Self> {
        let kind = match algorithm {
            CHECKSUM_CRC32 => Self::Crc32,
            CHECKSUM_CRC32C => Self::Crc32c,
            CHECKSUM_CRC64NVME => Self::Crc64Nvme,
            CHECKSUM_SHA1 => Self::Sha1,
            CHECKSUM_SHA256 => Self::Sha256,
            _ => return None,
        };
        Some(kind)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => CHECKSUM_CRC32,
            Self::Crc32c => CHECKSUM_CRC32C,
            Self::Crc64Nvme => CHECKSUM_CRC64NVME,
            Self::Sha1 => CHECKSUM_SHA1,
            Self::Sha256 => CHECKSUM_SHA256,
        }
    }
}

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

pub(crate) fn checksum_value(kind: ChecksumAlgorithmKind, value: &[u8]) -> String {
    match kind {
        ChecksumAlgorithmKind::Crc32 => encode_base64(Crc32::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Crc32c => encode_base64(Crc32c::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Crc64Nvme => encode_base64(Crc64Nvme::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Sha1 => encode_base64(Sha1::checksum(value).as_ref()),
        ChecksumAlgorithmKind::Sha256 => encode_base64(Sha256::checksum(value).as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::decode_base64;

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
}
