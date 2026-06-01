use std::{collections::HashSet, fmt::Write as _, ops::Not};

use s3s::{
    S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        CommonPrefix, DeleteMarkerEntry, ETag, EncodingType, ListObjectVersionsInput,
        ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
        ListObjectsV2Output, Object, ObjectVersion, ObjectVersionStorageClass, Owner,
    },
};

use crate::{
    database::BucketConnection,
    sqlite::{ObjectSummary, Sqlite},
};

use super::{
    CONTINUATION_TOKEN_PREFIX, LIST_BATCH_SIZE, hex, owner, s3_error, s3_error_from_sqlite,
    s3_error_with_message,
};

/// Returns the common prefix produced by applying an S3 delimiter to a key.
pub(super) fn common_prefix_for_key(
    key: &str,
    prefix: &str,
    delimiter: Option<&str>,
) -> Option<String> {
    let delimiter = delimiter.filter(|delimiter| delimiter.is_empty().not())?;
    let rest = key.strip_prefix(prefix)?;
    let delimiter_index = rest.find(delimiter)?;
    Some(format!(
        "{}{}",
        prefix,
        &rest[..delimiter_index + delimiter.len()]
    ))
}

/// Encodes a listing continuation token from the last scanned key.
pub(super) fn encode_continuation_token(key: &str) -> String {
    format!("{CONTINUATION_TOKEN_PREFIX}{}", hex(key.as_bytes()))
}

/// Decodes a hexadecimal string into bytes.
pub(super) fn decode_hex(value: &str) -> Option<Vec<u8>> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }

    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        decoded.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Some(decoded)
}

/// Decodes and validates a listing continuation token.
pub(super) fn decode_continuation_token(token: &str) -> S3Result<String> {
    let encoded = token
        .strip_prefix(CONTINUATION_TOKEN_PREFIX)
        .ok_or_else(|| s3_error(S3ErrorCode::InvalidToken))?;
    let decoded = decode_hex(encoded).ok_or_else(|| s3_error(S3ErrorCode::InvalidToken))?;
    String::from_utf8(decoded).map_err(|_| s3_error(S3ErrorCode::InvalidToken))
}

/// URL-encodes a list response value when requested by the client.
pub(super) fn encode_list_value(value: String, encode: bool) -> String {
    if encode.not() {
        return value;
    }

    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(char::from(byte));
            }
            _ => {
                let _ = write!(&mut encoded, "%{byte:02X}");
            }
        }
    }
    encoded
}

/// Decodes S3 list input values that clients encode for XML-safe control bytes.
pub(super) fn decode_list_input_value(value: String) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Some(hex) = decode_hex(&value[index + 1..index + 3])
            && matches!(hex.as_slice(), [b'\n' | b'\r' | b'\t'])
        {
            decoded.push(hex[0]);
            index += 3;
            continue;
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8(decoded).unwrap_or(value)
}

/// Returns whether list output should use S3 URL encoding.
pub(super) fn is_url_encoding(encoding_type: Option<&EncodingType>) -> bool {
    encoding_type.is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL)
}

/// Normalizes a delimiter echo for list output.
pub(super) fn output_delimiter(delimiter: Option<String>) -> Option<String> {
    delimiter.and_then(|delimiter| delimiter.is_empty().not().then_some(delimiter))
}

/// Returns a bounded S3 listing limit.
pub(super) fn list_limit(limit: Option<i32>, default: i32, max: i32) -> S3Result<i32> {
    let limit = limit.unwrap_or(default);
    if limit <= 0 {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "listing limit must be greater than zero",
        ));
    }
    Ok(limit.min(max))
}

/// Converts a stored object summary into an S3 list object.
pub(super) fn object_from_summary(
    summary: ObjectSummary,
    encode_keys: bool,
    owner: Option<&Owner>,
) -> Object {
    Object {
        key: Some(encode_list_value(summary.key, encode_keys)),
        last_modified: Some(summary.last_modified.into()),
        size: Some(summary.size),
        e_tag: summary.md5.map(ETag::Strong),
        owner: owner.cloned(),
        ..Default::default()
    }
}

/// Converts a stored object summary into an S3 object-version entry.
pub(super) fn object_version_from_summary(
    summary: ObjectSummary,
    encode_keys: bool,
    owner: Option<&Owner>,
) -> ObjectVersion {
    ObjectVersion {
        e_tag: summary.md5.map(ETag::Strong),
        is_latest: Some(summary.is_latest),
        key: Some(encode_list_value(summary.key, encode_keys)),
        last_modified: Some(summary.last_modified.into()),
        owner: owner.cloned(),
        size: Some(summary.size),
        storage_class: Some(ObjectVersionStorageClass::from_static(
            ObjectVersionStorageClass::STANDARD,
        )),
        version_id: Some(summary.version_id),
        ..Default::default()
    }
}

/// Converts a stored delete-marker summary into an S3 version-list entry.
pub(super) fn delete_marker_from_summary(
    summary: ObjectSummary,
    encode_keys: bool,
    owner: Option<&Owner>,
) -> DeleteMarkerEntry {
    DeleteMarkerEntry {
        is_latest: Some(summary.is_latest),
        key: Some(encode_list_value(summary.key, encode_keys)),
        last_modified: Some(summary.last_modified.into()),
        owner: owner.cloned(),
        version_id: Some(summary.version_id),
    }
}

pub(super) struct KeyListing {
    keys: Vec<ObjectSummary>,
    common_prefixes: Vec<CommonPrefix>,
    key_count: usize,
    next_marker: Option<String>,
    is_truncated: bool,
}

pub(super) struct ObjectVersionListing {
    versions: Vec<ObjectSummary>,
    delete_markers: Vec<ObjectSummary>,
    common_prefixes: Vec<CommonPrefix>,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<String>,
    is_truncated: bool,
}

impl Sqlite {
    /// Lists keys and common prefixes using S3 prefix, delimiter, and marker semantics.
    async fn list_keys(
        &self,
        bucket: &str,
        prefix_filter: &str,
        delimiter_filter: Option<&str>,
        mut after_key: Option<String>,
        max_keys: usize,
    ) -> S3Result<KeyListing> {
        let connection = self
            .try_get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let mut keys = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_common_prefixes = HashSet::new();
        let mut key_count = 0usize;
        let marker_filter = after_key.clone();
        let mut next_marker = None;
        let mut is_truncated = false;
        let list_batch_len = usize::try_from(LIST_BATCH_SIZE).map_err(S3Error::internal_error)?;

        if max_keys > 0 {
            'pages: loop {
                let prefix = prefix_filter.to_string();
                let after_key_query = after_key.clone();
                let summaries = connection
                    .read(move |connection| {
                        let transaction = connection.transaction()?;
                        Self::try_list_objects(
                            &transaction,
                            Some(&prefix),
                            after_key_query.as_deref(),
                            LIST_BATCH_SIZE,
                        )
                    })
                    .await
                    .map_err(s3_error_from_sqlite)?;

                if summaries.is_empty() {
                    break;
                }

                let summary_count = summaries.len();
                for summary in summaries {
                    if prefix_filter.is_empty().not()
                        && summary.key.starts_with(prefix_filter).not()
                    {
                        break 'pages;
                    }

                    if let Some(common_prefix) =
                        common_prefix_for_key(&summary.key, prefix_filter, delimiter_filter)
                    {
                        if marker_filter
                            .as_ref()
                            .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                        {
                            after_key = Some(summary.key);
                            continue;
                        }

                        if seen_common_prefixes.contains(&common_prefix) {
                            after_key = Some(summary.key);
                            continue;
                        }

                        if key_count == max_keys {
                            is_truncated = true;
                            break 'pages;
                        }

                        seen_common_prefixes.insert(common_prefix.clone());
                        next_marker = Some(common_prefix.clone());
                        common_prefixes.push(CommonPrefix {
                            prefix: Some(common_prefix),
                        });
                        key_count += 1;
                        after_key = Some(summary.key);
                        continue;
                    }

                    if key_count == max_keys {
                        is_truncated = true;
                        break 'pages;
                    }

                    after_key = Some(summary.key.clone());
                    next_marker = Some(summary.key.clone());
                    keys.push(summary);
                    key_count += 1;
                }

                if summary_count < list_batch_len {
                    break;
                }
            }
        }

        Ok(KeyListing {
            keys,
            common_prefixes,
            key_count,
            next_marker,
            is_truncated,
        })
    }

    /// Lists object versions and common prefixes using S3 version marker semantics.
    async fn list_versions(
        &self,
        bucket: &str,
        prefix_filter: &str,
        delimiter_filter: Option<&str>,
        mut after_key: Option<String>,
        mut after_version_id: Option<String>,
        max_keys: usize,
    ) -> S3Result<ObjectVersionListing> {
        let connection = self
            .try_get_connection(bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_common_prefixes = HashSet::new();
        let marker_filter = after_key.clone();
        let mut next_key_marker = None;
        let mut next_version_id_marker = None;
        let mut count = 0usize;
        let mut is_truncated = false;
        let list_batch_len = usize::try_from(LIST_BATCH_SIZE).map_err(S3Error::internal_error)?;

        if max_keys > 0 {
            'pages: loop {
                let prefix = prefix_filter.to_string();
                let after_key_query = after_key.clone();
                let after_version_id_query = after_version_id.clone();
                let summaries = Self::list_version_page(
                    &connection,
                    prefix,
                    after_key_query,
                    after_version_id_query,
                )
                .await?;

                if summaries.is_empty() {
                    break;
                }

                let summary_count = summaries.len();
                for summary in summaries {
                    if prefix_filter.is_empty().not()
                        && summary.key.starts_with(prefix_filter).not()
                    {
                        break 'pages;
                    }

                    let scanned_key = summary.key.clone();
                    let scanned_version_id = summary.version_id.clone();
                    if let Some(common_prefix) =
                        common_prefix_for_key(&summary.key, prefix_filter, delimiter_filter)
                    {
                        if marker_filter
                            .as_ref()
                            .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                        {
                            after_key = Some(scanned_key);
                            after_version_id = Some(scanned_version_id);
                            continue;
                        }

                        if seen_common_prefixes.contains(&common_prefix) {
                            after_key = Some(scanned_key);
                            after_version_id = Some(scanned_version_id);
                            continue;
                        }

                        if count == max_keys {
                            is_truncated = true;
                            break 'pages;
                        }

                        seen_common_prefixes.insert(common_prefix.clone());
                        common_prefixes.push(CommonPrefix {
                            prefix: Some(common_prefix),
                        });
                        next_key_marker = Some(scanned_key.clone());
                        next_version_id_marker = Some(scanned_version_id.clone());
                        count += 1;
                        after_key = Some(scanned_key);
                        after_version_id = Some(scanned_version_id);
                        continue;
                    }

                    if count == max_keys {
                        is_truncated = true;
                        break 'pages;
                    }

                    next_key_marker = Some(scanned_key.clone());
                    next_version_id_marker = Some(scanned_version_id.clone());
                    after_key = Some(scanned_key);
                    after_version_id = Some(scanned_version_id);
                    if summary.is_delete_marker {
                        delete_markers.push(summary);
                    } else {
                        versions.push(summary);
                    }
                    count += 1;
                }

                if summary_count < list_batch_len {
                    break;
                }
            }
        }

        Ok(ObjectVersionListing {
            versions,
            delete_markers,
            common_prefixes,
            next_key_marker,
            next_version_id_marker,
            is_truncated,
        })
    }

    /// Reads one storage page for `ListObjectVersions`.
    async fn list_version_page(
        connection: &BucketConnection,
        prefix: String,
        after_key: Option<String>,
        after_version_id: Option<String>,
    ) -> S3Result<Vec<ObjectSummary>> {
        connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_list_object_versions(
                    &transaction,
                    Some(&prefix),
                    after_key.as_deref(),
                    after_version_id.as_deref(),
                    LIST_BATCH_SIZE,
                )
            })
            .await
            .map_err(s3_error_from_sqlite)
    }
}

/// Lists objects using the original `ListObjects` marker protocol.
#[tracing::instrument]
pub(super) async fn list_objects(
    sqlite: &Sqlite,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let ListObjectsInput {
        bucket,
        mut prefix,
        max_keys,
        mut marker,
        mut delimiter,
        encoding_type,
        ..
    } = req.input;

    prefix = prefix.map(decode_list_input_value);
    marker = marker.map(decode_list_input_value);
    delimiter = delimiter.map(decode_list_input_value);

    let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
    let max_keys_usize = usize::try_from(max_keys).map_err(S3Error::internal_error)?;
    let marker_output = Some(marker.clone().unwrap_or_default());
    let prefix_filter = prefix.clone().unwrap_or_default();
    let encode_keys = is_url_encoding(encoding_type.as_ref());
    let delimiter_filter = delimiter
        .as_deref()
        .filter(|delimiter| delimiter.is_empty().not())
        .map(str::to_string);
    let delimiter = output_delimiter(delimiter);
    let owner = owner();

    let listing = sqlite
        .list_keys(
            &bucket,
            &prefix_filter,
            delimiter_filter.as_deref(),
            marker.clone(),
            max_keys_usize,
        )
        .await?;
    let objects = listing
        .keys
        .into_iter()
        .map(|summary| object_from_summary(summary, encode_keys, Some(&owner)))
        .collect::<Vec<_>>();
    let next_marker = listing
        .is_truncated
        .then(|| {
            listing
                .next_marker
                .clone()
                .map(|marker| encode_list_value(marker, encode_keys))
        })
        .flatten();

    let output = ListObjectsOutput {
        contents: Some(objects),
        common_prefixes: delimiter.as_ref().map(|_| {
            listing
                .common_prefixes
                .into_iter()
                .map(|common_prefix| CommonPrefix {
                    prefix: common_prefix
                        .prefix
                        .map(|prefix| encode_list_value(prefix, encode_keys)),
                })
                .collect::<Vec<_>>()
        }),
        delimiter,
        encoding_type,
        name: Some(bucket),
        prefix,
        max_keys: Some(max_keys),
        is_truncated: Some(next_marker.is_some()),
        marker: marker_output,
        next_marker,
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

/// Lists all object versions and delete markers.
#[tracing::instrument]
pub(super) async fn list_object_versions(
    sqlite: &Sqlite,
    req: S3Request<ListObjectVersionsInput>,
) -> S3Result<S3Response<ListObjectVersionsOutput>> {
    let ListObjectVersionsInput {
        bucket,
        mut prefix,
        max_keys,
        mut key_marker,
        version_id_marker,
        mut delimiter,
        encoding_type,
        ..
    } = req.input;

    prefix = prefix.map(decode_list_input_value);
    key_marker = key_marker.map(decode_list_input_value);
    delimiter = delimiter.map(decode_list_input_value);

    let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
    let max_keys_usize = usize::try_from(max_keys).map_err(S3Error::internal_error)?;
    let key_marker_clone = key_marker.clone();
    let version_id_marker_clone = version_id_marker.clone();
    let prefix_filter = prefix.clone().unwrap_or_default();
    let encode_keys = is_url_encoding(encoding_type.as_ref());
    let delimiter_filter = delimiter
        .as_deref()
        .filter(|delimiter| delimiter.is_empty().not())
        .map(str::to_string);
    let delimiter = output_delimiter(delimiter);
    let owner = owner();

    let listing = sqlite
        .list_versions(
            &bucket,
            &prefix_filter,
            delimiter_filter.as_deref(),
            key_marker.clone(),
            version_id_marker.clone(),
            max_keys_usize,
        )
        .await?;
    let versions = listing
        .versions
        .into_iter()
        .map(|summary| object_version_from_summary(summary, encode_keys, Some(&owner)))
        .collect::<Vec<_>>();
    let delete_markers = listing
        .delete_markers
        .into_iter()
        .map(|summary| delete_marker_from_summary(summary, encode_keys, Some(&owner)))
        .collect::<Vec<_>>();

    let next_key_marker = listing
        .next_key_marker
        .filter(|_| listing.is_truncated)
        .map(|marker| encode_list_value(marker, encode_keys));
    let next_version_id_marker = listing
        .next_version_id_marker
        .filter(|_| listing.is_truncated);

    let output = ListObjectVersionsOutput {
        common_prefixes: delimiter.as_ref().map(|_| {
            listing
                .common_prefixes
                .into_iter()
                .map(|common_prefix| CommonPrefix {
                    prefix: common_prefix
                        .prefix
                        .map(|prefix| encode_list_value(prefix, encode_keys)),
                })
                .collect::<Vec<_>>()
        }),
        delete_markers: Some(delete_markers),
        delimiter,
        encoding_type,
        is_truncated: Some(next_key_marker.is_some()),
        key_marker: key_marker_clone,
        max_keys: Some(max_keys),
        name: Some(bucket),
        next_key_marker,
        next_version_id_marker,
        prefix,
        version_id_marker: version_id_marker_clone,
        versions: Some(versions),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

/// Lists objects using the `ListObjectsV2` continuation-token protocol.
#[tracing::instrument]
pub(super) async fn list_objects_v2(
    sqlite: &Sqlite,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let ListObjectsV2Input {
        bucket,
        mut prefix,
        max_keys,
        mut start_after,
        mut delimiter,
        encoding_type,
        continuation_token,
        fetch_owner,
        ..
    } = req.input;

    prefix = prefix.map(decode_list_input_value);
    start_after = start_after.map(decode_list_input_value);
    delimiter = delimiter.map(decode_list_input_value);

    let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
    let max_keys_usize = usize::try_from(max_keys).map_err(S3Error::internal_error)?;
    let continuation_token_clone = continuation_token.clone();
    let prefix_filter = prefix.clone().unwrap_or_default();
    let encode_keys = is_url_encoding(encoding_type.as_ref());
    let delimiter_filter = delimiter
        .as_deref()
        .filter(|delimiter| delimiter.is_empty().not())
        .map(str::to_string);
    let delimiter = output_delimiter(delimiter);
    let owner = owner();
    let owner = fetch_owner.unwrap_or(false).then_some(owner);
    let after_key = match continuation_token
        .as_deref()
        .filter(|continuation_token| continuation_token.is_empty().not())
    {
        Some(continuation_token) => Some(decode_continuation_token(continuation_token)?),
        None => start_after.clone(),
    };

    let listing = sqlite
        .list_keys(
            &bucket,
            &prefix_filter,
            delimiter_filter.as_deref(),
            after_key,
            max_keys_usize,
        )
        .await?;
    let objects = listing
        .keys
        .into_iter()
        .map(|summary| object_from_summary(summary, encode_keys, owner.as_ref()))
        .collect::<Vec<_>>();

    let next_continuation_token = listing
        .is_truncated
        .then(|| {
            listing
                .next_marker
                .as_deref()
                .map(encode_continuation_token)
        })
        .flatten();

    let key_count = i32::try_from(listing.key_count).map_err(S3Error::internal_error)?;

    let output = ListObjectsV2Output {
        key_count: Some(key_count),
        max_keys: Some(max_keys),
        continuation_token: continuation_token_clone,
        is_truncated: Some(next_continuation_token.is_some()),
        contents: Some(objects),
        common_prefixes: delimiter.as_ref().map(|_| {
            listing
                .common_prefixes
                .into_iter()
                .map(|common_prefix| CommonPrefix {
                    prefix: common_prefix
                        .prefix
                        .map(|prefix| encode_list_value(prefix, encode_keys)),
                })
                .collect::<Vec<_>>()
        }),
        delimiter,
        encoding_type,
        name: Some(bucket),
        prefix,
        start_after,
        next_continuation_token,
        ..Default::default()
    };

    Ok(S3Response::new(output))
}
