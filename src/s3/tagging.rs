//! S3 tagging request handling and validation.

use std::collections::HashSet;

use super::{
    DeleteBucketTaggingInput, DeleteBucketTaggingOutput, DeleteObjectTaggingInput,
    DeleteObjectTaggingOutput, GetBucketTaggingInput, GetBucketTaggingOutput,
    GetObjectTaggingInput, GetObjectTaggingOutput, PutBucketTaggingInput, PutBucketTaggingOutput,
    PutObjectTaggingInput, PutObjectTaggingOutput, S3ErrorCode, S3Request, S3Response, S3Result,
    Sqlite, Tag, TagSet, Tagging, TaggingDirective, TaggingHeader, s3_error, s3_error_from_sqlite,
    s3_error_with_message, validate_expected_bucket_owner,
};

/// Maximum number of S3 tags allowed on one resource.
const MAX_TAGS: usize = 10;
/// Maximum S3 tag key length, counted in Unicode scalar values.
const MAX_TAG_KEY_CHARS: usize = 128;
/// Maximum S3 tag value length, counted in Unicode scalar values.
const MAX_TAG_VALUE_CHARS: usize = 256;

/// Normalized S3 tag persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredTag {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Parsed copy tagging behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CopyTaggingDirective {
    Copy,
    Replace(Vec<StoredTag>),
}

/// Converts a wire `Tagging` document into normalized stored tags.
pub(super) fn stored_tags_from_tagging(tagging: Tagging) -> S3Result<Vec<StoredTag>> {
    normalize_tags(
        tagging
            .tag_set
            .into_iter()
            .map(|tag| {
                let key = tag.key.ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
                let value = tag
                    .value
                    .ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
                Ok(StoredTag { key, value })
            })
            .collect::<S3Result<Vec<_>>>()?,
    )
}

/// Parses an optional `x-amz-tagging` header.
pub(super) fn stored_tags_from_header(header: Option<TaggingHeader>) -> S3Result<Vec<StoredTag>> {
    let Some(header) = header else {
        return Ok(Vec::new());
    };
    if header.is_empty() {
        return Ok(Vec::new());
    }

    normalize_tags(
        header
            .split('&')
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                let key = percent_decode_tag_component(key)?;
                let value = percent_decode_tag_component(value)?;
                Ok(StoredTag { key, value })
            })
            .collect::<S3Result<Vec<_>>>()?,
    )
}

/// Converts stored tags into the S3 output shape.
pub(super) fn tag_set_from_stored(tags: Vec<StoredTag>) -> TagSet {
    tags.into_iter()
        .map(|tag| Tag {
            key: Some(tag.key),
            value: Some(tag.value),
        })
        .collect()
}

/// Deletes tags from an existing bucket.
pub(super) async fn delete_bucket_tagging(
    sqlite: &Sqlite,
    req: S3Request<DeleteBucketTaggingInput>,
) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
    let DeleteBucketTaggingInput {
        bucket,
        expected_bucket_owner,
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            Sqlite::replace_bucket_tags(&transaction, &[])?;
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteBucketTaggingOutput::default()))
}

/// Reads tags from an existing bucket.
pub(super) async fn get_bucket_tagging(
    sqlite: &Sqlite,
    req: S3Request<GetBucketTaggingInput>,
) -> S3Result<S3Response<GetBucketTaggingOutput>> {
    let GetBucketTaggingInput {
        bucket,
        expected_bucket_owner,
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let tags = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            Sqlite::get_bucket_tags(&transaction)
        })
        .await
        .map_err(s3_error_from_sqlite)?;
    if tags.is_empty() {
        return Err(s3_error(S3ErrorCode::NoSuchTagSet));
    }

    Ok(S3Response::new(GetBucketTaggingOutput {
        tag_set: tag_set_from_stored(tags),
    }))
}

/// Replaces tags on an existing bucket.
pub(super) async fn put_bucket_tagging(
    sqlite: &Sqlite,
    req: S3Request<PutBucketTaggingInput>,
) -> S3Result<S3Response<PutBucketTaggingOutput>> {
    let PutBucketTaggingInput {
        bucket,
        expected_bucket_owner,
        tagging,
        ..
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let tags = stored_tags_from_tagging(tagging)?;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            Sqlite::replace_bucket_tags(&transaction, &tags)?;
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(PutBucketTaggingOutput::default()))
}

/// Parses `CopyObject` tagging directive and replacement tags.
pub(super) fn copy_tagging_directive(
    directive: Option<&TaggingDirective>,
    tagging: Option<TaggingHeader>,
) -> S3Result<CopyTaggingDirective> {
    match directive.map(TaggingDirective::as_str) {
        None | Some(TaggingDirective::COPY) => Ok(CopyTaggingDirective::Copy),
        Some(TaggingDirective::REPLACE) => Ok(CopyTaggingDirective::Replace(
            stored_tags_from_header(tagging)?,
        )),
        Some(_) => Err(s3_error(S3ErrorCode::InvalidArgument)),
    }
}

/// Returns the version ID as S3 exposes it in object-tagging responses.
pub(super) fn visible_tagging_version_id(version_id: String) -> Option<String> {
    (version_id != "null").then_some(version_id)
}

/// Deletes tags from an existing object version.
pub(super) async fn delete_object_tagging(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectTaggingInput>,
) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    let DeleteObjectTaggingInput {
        bucket,
        expected_bucket_owner,
        key,
        version_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let version_id = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let target = match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::get_object_version_tagging_target(&transaction, &key, version_id)?
                        .ok_or(super::SqliteError::ObjectVersionNotFound)?
                }
                None => Sqlite::get_current_object_tagging_target(&transaction, &key)?
                    .ok_or(super::SqliteError::ObjectNotFound)?,
            };
            Sqlite::replace_object_tags(&transaction, target.sequence, &[])?;
            transaction.commit()?;
            Ok(target.version_id)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteObjectTaggingOutput {
        version_id: visible_tagging_version_id(version_id),
    }))
}

/// Reads tags from an existing object version.
pub(super) async fn get_object_tagging(
    sqlite: &Sqlite,
    req: S3Request<GetObjectTaggingInput>,
) -> S3Result<S3Response<GetObjectTaggingOutput>> {
    let GetObjectTaggingInput {
        bucket,
        expected_bucket_owner,
        key,
        version_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let (version_id, tags) = connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            let target = match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::get_object_version_tagging_target(&transaction, &key, version_id)?
                        .ok_or(super::SqliteError::ObjectVersionNotFound)?
                }
                None => Sqlite::get_current_object_tagging_target(&transaction, &key)?
                    .ok_or(super::SqliteError::ObjectNotFound)?,
            };
            let tags = Sqlite::get_object_tags(&transaction, target.sequence)?;
            Ok((target.version_id, tags))
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(GetObjectTaggingOutput {
        tag_set: tag_set_from_stored(tags),
        version_id: visible_tagging_version_id(version_id),
    }))
}

/// Replaces tags on an existing object version.
pub(super) async fn put_object_tagging(
    sqlite: &Sqlite,
    req: S3Request<PutObjectTaggingInput>,
) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    let PutObjectTaggingInput {
        bucket,
        expected_bucket_owner,
        key,
        tagging,
        version_id,
        ..
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let tags = stored_tags_from_tagging(tagging)?;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let version_id = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let target = match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::get_object_version_tagging_target(&transaction, &key, version_id)?
                        .ok_or(super::SqliteError::ObjectVersionNotFound)?
                }
                None => Sqlite::get_current_object_tagging_target(&transaction, &key)?
                    .ok_or(super::SqliteError::ObjectNotFound)?,
            };
            Sqlite::replace_object_tags(&transaction, target.sequence, &tags)?;
            transaction.commit()?;
            Ok(target.version_id)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(PutObjectTaggingOutput {
        version_id: visible_tagging_version_id(version_id),
    }))
}

fn normalize_tags(mut tags: Vec<StoredTag>) -> S3Result<Vec<StoredTag>> {
    if tags.len() > MAX_TAGS {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidTag,
            "tags cannot contain more than 10 tags",
        ));
    }

    let mut keys = HashSet::with_capacity(tags.len());
    for tag in &tags {
        if tag.key.chars().count() > MAX_TAG_KEY_CHARS
            || tag.value.chars().count() > MAX_TAG_VALUE_CHARS
        {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidTag,
                "tag keys cannot exceed 128 characters and tag values cannot exceed 256 characters",
            ));
        }
        if !keys.insert(tag.key.as_str()) {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidTag,
                "tags cannot contain duplicate keys",
            ));
        }
    }

    tags.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(tags)
}

fn percent_decode_tag_component(component: &str) -> S3Result<String> {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let Some(hex) = bytes.get(index + 1..index + 3) else {
                    return Err(s3_error(S3ErrorCode::InvalidTag));
                };
                decoded.push(hex_pair(hex).ok_or_else(|| s3_error(S3ErrorCode::InvalidTag))?);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| s3_error(S3ErrorCode::InvalidTag))
}

fn hex_pair(input: &[u8]) -> Option<u8> {
    let [high, low] = input else {
        return None;
    };
    let high = hex_digit(*high)?;
    let low = hex_digit(*low)?;
    Some((high << 4) | low)
}

fn hex_digit(input: u8) -> Option<u8> {
    match input {
        b'0'..=b'9' => Some(input - b'0'),
        b'a'..=b'f' => Some(input - b'a' + 10),
        b'A'..=b'F' => Some(input - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_error_code<T>(result: S3Result<T>, code: &S3ErrorCode) {
        match result {
            Ok(_) => panic!("expected {code:?}"),
            Err(err) => assert_eq!(err.code(), code),
        }
    }

    fn require_ok<T>(result: S3Result<T>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }

    fn tagging(tags: impl IntoIterator<Item = (&'static str, &'static str)>) -> Tagging {
        Tagging {
            tag_set: tags
                .into_iter()
                .map(|(key, value)| Tag {
                    key: Some(key.to_string()),
                    value: Some(value.to_string()),
                })
                .collect(),
        }
    }

    #[test]
    fn dto_tags_normalize_and_sort() {
        let tags = require_ok(stored_tags_from_tagging(tagging([
            ("b", "two"),
            ("a", "one"),
        ])));
        assert_eq!(
            tags,
            vec![
                StoredTag {
                    key: "a".to_string(),
                    value: "one".to_string(),
                },
                StoredTag {
                    key: "b".to_string(),
                    value: "two".to_string(),
                },
            ]
        );
    }

    #[test]
    fn dto_tags_reject_missing_fields() {
        assert_error_code(
            stored_tags_from_tagging(Tagging {
                tag_set: vec![Tag {
                    key: Some("key".to_string()),
                    value: None,
                }],
            }),
            &S3ErrorCode::MalformedXML,
        );
    }

    #[test]
    fn tags_reject_duplicate_keys_and_count_limit() {
        assert_error_code(
            stored_tags_from_tagging(tagging([("a", "one"), ("a", "two")])),
            &S3ErrorCode::InvalidTag,
        );
        assert_error_code(
            stored_tags_from_tagging(tagging([
                ("0", "v"),
                ("1", "v"),
                ("2", "v"),
                ("3", "v"),
                ("4", "v"),
                ("5", "v"),
                ("6", "v"),
                ("7", "v"),
                ("8", "v"),
                ("9", "v"),
                ("10", "v"),
            ])),
            &S3ErrorCode::InvalidTag,
        );
    }

    #[test]
    fn header_tags_percent_decode_and_sort() {
        let tags = require_ok(stored_tags_from_header(Some(
            "space=a+b&encoded=%2Fpath".to_string(),
        )));
        assert_eq!(
            tags,
            vec![
                StoredTag {
                    key: "encoded".to_string(),
                    value: "/path".to_string(),
                },
                StoredTag {
                    key: "space".to_string(),
                    value: "a b".to_string(),
                },
            ]
        );
    }

    #[test]
    fn header_tags_accept_missing_value_and_reject_bad_percent_encoding() {
        assert_eq!(
            require_ok(stored_tags_from_header(Some("missing-value".to_string()))),
            vec![StoredTag {
                key: "missing-value".to_string(),
                value: String::new(),
            }]
        );
        assert_error_code(
            stored_tags_from_header(Some("bad=%xx".to_string())),
            &S3ErrorCode::InvalidTag,
        );
        assert_error_code(
            stored_tags_from_header(Some("bad=%".to_string())),
            &S3ErrorCode::InvalidTag,
        );
    }

    #[test]
    fn copy_tagging_defaults_to_copy_and_accepts_replace() {
        assert_eq!(
            require_ok(copy_tagging_directive(
                None,
                Some("ignored=yes".to_string())
            )),
            CopyTaggingDirective::Copy
        );
        assert_eq!(
            require_ok(copy_tagging_directive(
                Some(&TaggingDirective::from_static(TaggingDirective::REPLACE)),
                Some("a=one".to_string()),
            )),
            CopyTaggingDirective::Replace(vec![StoredTag {
                key: "a".to_string(),
                value: "one".to_string(),
            }])
        );
        assert_error_code(
            copy_tagging_directive(
                Some(&TaggingDirective::from_static("BOGUS")),
                Some("a=one".to_string()),
            ),
            &S3ErrorCode::InvalidArgument,
        );
    }
}
