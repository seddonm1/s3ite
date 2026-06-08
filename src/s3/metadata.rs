//! S3 request metadata normalization helpers.

use std::{collections::HashMap, ops::Not};

/// Stores HTTP content encodings while hiding S3's transport-only aws-chunked marker.
pub(super) fn normalize_content_encoding(content_encoding: Option<String>) -> Option<String> {
    let content_encoding = content_encoding?;
    let values = content_encoding
        .split(',')
        .filter_map(|value| {
            let value = value.trim();
            (value.is_empty().not() && value.eq_ignore_ascii_case("aws-chunked").not())
                .then(|| value.to_string())
        })
        .collect::<Vec<_>>();

    values.is_empty().not().then(|| values.join(", "))
}

/// Decodes SDK-encoded UTF-8 metadata values before storing them.
pub(super) fn normalize_metadata(
    metadata: Option<HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    metadata.map(|metadata| {
        metadata
            .into_iter()
            .map(|(key, value)| (key, decode_rfc2047_metadata_value(&value)))
            .collect()
    })
}

/// Decodes a single RFC 2047 metadata word, preserving invalid values as-is.
fn decode_rfc2047_metadata_value(value: &str) -> String {
    decode_rfc2047_word(value).unwrap_or_else(|| value.to_string())
}

/// Decodes one supported RFC 2047 encoded word from SDK metadata.
fn decode_rfc2047_word(value: &str) -> Option<String> {
    let encoded = value.strip_prefix("=?")?.strip_suffix("?=")?;
    let mut parts = encoded.splitn(3, '?');
    let charset = parts.next()?;
    let encoding = parts.next()?;
    let payload = parts.next()?;
    if charset.eq_ignore_ascii_case("utf-8").not() && charset.eq_ignore_ascii_case("us-ascii").not()
    {
        return None;
    }

    let bytes = match encoding {
        encoding if encoding.eq_ignore_ascii_case("b") => {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .ok()?
        }
        encoding if encoding.eq_ignore_ascii_case("q") => decode_rfc2047_q(payload)?,
        _ => return None,
    };
    String::from_utf8(bytes).ok()
}

/// Decodes the RFC 2047 quoted-printable subset used by S3 SDK metadata.
fn decode_rfc2047_q(payload: &str) -> Option<Vec<u8>> {
    let mut bytes = Vec::with_capacity(payload.len());
    let mut iter = payload.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'_' => bytes.push(b' '),
            b'=' => {
                let high = iter.next()?;
                let low = iter.next()?;
                let high = hex_digit(high)?;
                let low = hex_digit(low)?;
                bytes.push((high << 4) | low);
            }
            byte => bytes.push(byte),
        }
    }
    Some(bytes)
}

/// Converts one ASCII hexadecimal digit into its numeric value.
fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_content_encoding_values() {
        assert_eq!(normalize_content_encoding(None), None);
        assert_eq!(
            normalize_content_encoding(Some("gzip, aws-chunked, br".to_string())),
            Some("gzip, br".to_string())
        );
        assert_eq!(
            normalize_content_encoding(Some(" gzip , , br ".to_string())),
            Some("gzip, br".to_string())
        );
        assert_eq!(
            normalize_content_encoding(Some("AWS-CHUNKED, aws-chunked".to_string())),
            None
        );
    }

    #[test]
    fn normalizes_rfc2047_metadata_values() {
        let metadata = HashMap::from([
            (
                "base64".to_string(),
                "=?UTF-8?B?SGVsbG8gV29ybGTDqQ==?=".to_string(),
            ),
            (
                "quoted".to_string(),
                "=?utf-8?q?Hello_World=C3=A9?=".to_string(),
            ),
            ("plain".to_string(), "Hello World".to_string()),
            ("invalid".to_string(), "=?UTF-8?B?not base64!?=".to_string()),
        ]);

        let normalized = normalize_metadata(Some(metadata)).expect("metadata remains present");
        assert_eq!(
            normalized.get("base64").map(String::as_str),
            Some("Hello World\u{e9}")
        );
        assert_eq!(
            normalized.get("quoted").map(String::as_str),
            Some("Hello World\u{e9}")
        );
        assert_eq!(
            normalized.get("plain").map(String::as_str),
            Some("Hello World")
        );
        assert_eq!(
            normalized.get("invalid").map(String::as_str),
            Some("=?UTF-8?B?not base64!?=")
        );
        assert_eq!(normalize_metadata(None), None);
    }
}
