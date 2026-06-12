//! S3 bucket lifecycle request handling, validation, and rule evaluation.

use std::{collections::HashSet, ops::Not};

use time::{OffsetDateTime, format_description::well_known::Rfc2822};

use super::{
    DeleteBucketLifecycleInput, DeleteBucketLifecycleOutput, ExpirationStatus,
    GetBucketLifecycleConfigurationInput, GetBucketLifecycleConfigurationOutput,
    LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
    LifecycleRules, NoncurrentVersionExpiration, ObjectLifecycleExpiration,
    PutBucketLifecycleConfigurationInput, PutBucketLifecycleConfigurationOutput, S3ErrorCode,
    S3Request, S3Response, S3Result, Sqlite, StoredLifecycleAndFilter, StoredLifecycleExpiration,
    StoredLifecycleFilter, StoredLifecycleRule, StoredNoncurrentVersionExpiration, StoredTag, Tag,
    TagSet, Timestamp, s3_error, s3_error_from_sqlite, s3_error_with_message,
    validate_expected_bucket_owner,
};

use s3s::dto::{AbortIncompleteMultipartUpload, BucketLifecycleConfiguration};

const MAX_LIFECYCLE_RULES: usize = 1000;
const MAX_LIFECYCLE_RULE_ID_CHARS: usize = 255;
const MAX_NEWER_NONCURRENT_VERSIONS: i32 = 100;

/// Header value for lifecycle expiration on object reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleExpirationHeader {
    pub(crate) expires_at: OffsetDateTime,
    pub(crate) rule_id: String,
}

impl From<ObjectLifecycleExpiration> for LifecycleExpirationHeader {
    /// Converts storage-selected expiration metadata into the S3 response shape.
    fn from(expiration: ObjectLifecycleExpiration) -> Self {
        Self {
            expires_at: expiration.expires_at,
            rule_id: expiration.rule_id,
        }
    }
}

impl LifecycleExpirationHeader {
    /// Converts to the wire value used for `x-amz-expiration`.
    pub(crate) fn to_header_value(&self) -> S3Result<String> {
        let expiry_date = self
            .expires_at
            .format(&Rfc2822)
            .map_err(super::S3Error::internal_error)?;
        Ok(format!(
            "expiry-date=\"{expiry_date}\", rule-id=\"{}\"",
            percent_encode_rule_id(&self.rule_id)
        ))
    }
}

/// Deletes lifecycle rules from an existing bucket.
pub(super) async fn delete_bucket_lifecycle(
    sqlite: &Sqlite,
    req: S3Request<DeleteBucketLifecycleInput>,
) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
    let DeleteBucketLifecycleInput {
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
        .write(move |transaction| {
            Sqlite::replace_bucket_lifecycle_rules(transaction, &[])?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
}

/// Reads lifecycle rules from an existing bucket.
pub(super) async fn get_bucket_lifecycle_configuration(
    sqlite: &Sqlite,
    req: S3Request<GetBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
    let GetBucketLifecycleConfigurationInput {
        bucket,
        expected_bucket_owner,
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let rules = connection
        .read(Sqlite::get_bucket_lifecycle_rules)
        .await
        .map_err(s3_error_from_sqlite)?;
    if rules.is_empty() {
        return Err(s3_error(S3ErrorCode::NoSuchLifecycleConfiguration));
    }

    Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
        rules: Some(lifecycle_rules_from_stored(rules)),
        transition_default_minimum_object_size: None,
    }))
}

/// Replaces lifecycle rules on an existing bucket.
pub(super) async fn put_bucket_lifecycle_configuration(
    sqlite: &Sqlite,
    req: S3Request<PutBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
    let PutBucketLifecycleConfigurationInput {
        bucket,
        expected_bucket_owner,
        lifecycle_configuration,
        ..
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let rules = stored_lifecycle_rules_from_configuration(lifecycle_configuration)?;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            Sqlite::replace_bucket_lifecycle_rules(transaction, &rules)?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(
        PutBucketLifecycleConfigurationOutput::default(),
    ))
}

pub(crate) fn stored_lifecycle_rules_from_configuration(
    configuration: Option<BucketLifecycleConfiguration>,
) -> S3Result<Vec<StoredLifecycleRule>> {
    let configuration = configuration.ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
    validate_rule_count(configuration.rules.len())?;
    let rules = configuration
        .rules
        .into_iter()
        .map(stored_lifecycle_rule_from_dto)
        .collect::<S3Result<Vec<_>>>()?;
    validate_unique_rule_ids(&rules)?;
    Ok(rules)
}

fn validate_rule_count(count: usize) -> S3Result<()> {
    if count == 0 {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Lifecycle configuration must contain at least one rule",
        ));
    }
    if count > MAX_LIFECYCLE_RULES {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Lifecycle configuration cannot contain more than 1000 rules",
        ));
    }
    Ok(())
}

fn validate_unique_rule_ids(rules: &[StoredLifecycleRule]) -> S3Result<()> {
    let mut ids = HashSet::new();
    for id in rules.iter().filter_map(|rule| rule.id.as_deref()) {
        if ids.insert(id).not() {
            return Err(s3_error(S3ErrorCode::InvalidArgument));
        }
    }
    Ok(())
}

fn stored_lifecycle_rule_from_dto(rule: LifecycleRule) -> S3Result<StoredLifecycleRule> {
    if rule
        .transitions
        .as_ref()
        .is_some_and(|values| values.is_empty().not())
        || rule
            .noncurrent_version_transitions
            .as_ref()
            .is_some_and(|values| values.is_empty().not())
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Lifecycle transitions are not supported",
        ));
    }

    if rule
        .id
        .as_ref()
        .is_some_and(|id| id.chars().count() > MAX_LIFECYCLE_RULE_ID_CHARS)
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Lifecycle rule ID cannot be longer than 255 characters",
        ));
    }

    let enabled = match rule.status.as_str() {
        ExpirationStatus::ENABLED => true,
        ExpirationStatus::DISABLED => false,
        _ => return Err(s3_error(S3ErrorCode::MalformedXML)),
    };
    let filter = stored_filter_from_dto(rule.prefix, rule.filter)?;
    let expiration = rule
        .expiration
        .map(stored_expiration_from_dto)
        .transpose()?;
    let noncurrent_expiration = rule
        .noncurrent_version_expiration
        .as_ref()
        .map(stored_noncurrent_expiration_from_dto)
        .transpose()?;
    let abort_incomplete_multipart_days = rule
        .abort_incomplete_multipart_upload
        .and_then(|abort| abort.days_after_initiation)
        .map(validate_positive_days)
        .transpose()?;

    if expiration.is_none()
        && noncurrent_expiration.is_none()
        && abort_incomplete_multipart_days.is_none()
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Lifecycle rule must contain at least one supported action",
        ));
    }

    Ok(StoredLifecycleRule {
        id: rule.id,
        enabled,
        filter,
        expiration,
        noncurrent_expiration,
        abort_incomplete_multipart_days,
    })
}

fn stored_filter_from_dto(
    legacy_prefix: Option<String>,
    filter: Option<LifecycleRuleFilter>,
) -> S3Result<StoredLifecycleFilter> {
    match (legacy_prefix, filter) {
        (Some(prefix), None) => Ok(StoredLifecycleFilter::LegacyPrefix(prefix)),
        (None, None) | (Some(_), Some(_)) => Err(s3_error(S3ErrorCode::MalformedXML)),
        (None, Some(filter)) => stored_filter_element_from_dto(filter),
    }
}

fn stored_filter_element_from_dto(filter: LifecycleRuleFilter) -> S3Result<StoredLifecycleFilter> {
    let mut shape_count = 0;
    shape_count += usize::from(filter.and.is_some());
    shape_count += usize::from(filter.prefix.is_some());
    shape_count += usize::from(filter.tag.is_some());
    shape_count += usize::from(filter.object_size_greater_than.is_some());
    shape_count += usize::from(filter.object_size_less_than.is_some());

    if shape_count > 1 {
        return Err(s3_error(S3ErrorCode::MalformedXML));
    }

    if let Some(and) = filter.and {
        return Ok(StoredLifecycleFilter::And(stored_and_filter_from_dto(and)?));
    }
    if let Some(prefix) = filter.prefix {
        return Ok(StoredLifecycleFilter::Prefix(prefix));
    }
    if let Some(tag) = filter.tag {
        return Ok(StoredLifecycleFilter::Tag(stored_tag_from_dto(tag)?));
    }
    if let Some(size) = filter.object_size_greater_than {
        return Ok(StoredLifecycleFilter::SizeGreaterThan(validate_size(size)?));
    }
    if let Some(size) = filter.object_size_less_than {
        return Ok(StoredLifecycleFilter::SizeLessThan(validate_size(size)?));
    }
    Ok(StoredLifecycleFilter::Empty)
}

fn stored_and_filter_from_dto(and: LifecycleRuleAndOperator) -> S3Result<StoredLifecycleAndFilter> {
    let tags = and
        .tags
        .unwrap_or_default()
        .into_iter()
        .map(stored_tag_from_dto)
        .collect::<S3Result<Vec<_>>>()?;
    Ok(StoredLifecycleAndFilter {
        prefix: and.prefix,
        tags,
        object_size_greater_than: and
            .object_size_greater_than
            .map(validate_size)
            .transpose()?,
        object_size_less_than: and.object_size_less_than.map(validate_size).transpose()?,
    })
}

fn stored_tag_from_dto(tag: Tag) -> S3Result<StoredTag> {
    Ok(StoredTag {
        key: tag.key.ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?,
        value: tag
            .value
            .ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?,
    })
}

fn stored_expiration_from_dto(
    expiration: LifecycleExpiration,
) -> S3Result<StoredLifecycleExpiration> {
    let has_days = expiration.days.is_some();
    let has_date = expiration.date.is_some();
    let delete_marker = expiration.expired_object_delete_marker.unwrap_or(false);
    if usize::from(has_days) + usize::from(has_date) + usize::from(delete_marker) != 1 {
        return Err(s3_error(S3ErrorCode::MalformedXML));
    }

    Ok(StoredLifecycleExpiration {
        days: expiration.days.map(validate_positive_days).transpose()?,
        date: expiration.date.map(Into::into),
        expired_object_delete_marker: delete_marker,
    })
}

fn stored_noncurrent_expiration_from_dto(
    expiration: &NoncurrentVersionExpiration,
) -> S3Result<StoredNoncurrentVersionExpiration> {
    let noncurrent_days = expiration
        .noncurrent_days
        .ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))
        .and_then(validate_positive_days)?;
    let newer_noncurrent_versions = expiration
        .newer_noncurrent_versions
        .map(validate_newer_noncurrent_versions)
        .transpose()?;
    Ok(StoredNoncurrentVersionExpiration {
        noncurrent_days,
        newer_noncurrent_versions,
    })
}

fn validate_positive_days(days: i32) -> S3Result<i32> {
    if days <= 0 {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Lifecycle days must be a positive integer",
        ));
    }
    Ok(days)
}

fn validate_newer_noncurrent_versions(count: i32) -> S3Result<i32> {
    if !(1..=MAX_NEWER_NONCURRENT_VERSIONS).contains(&count) {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "NewerNoncurrentVersions must be between 1 and 100",
        ));
    }
    Ok(count)
}

fn validate_size(size: i64) -> S3Result<i64> {
    if size < 0 {
        return Err(s3_error(S3ErrorCode::MalformedXML));
    }
    Ok(size)
}

fn lifecycle_rules_from_stored(rules: Vec<StoredLifecycleRule>) -> LifecycleRules {
    rules.into_iter().map(lifecycle_rule_from_stored).collect()
}

fn lifecycle_rule_from_stored(rule: StoredLifecycleRule) -> LifecycleRule {
    let (prefix, filter) = filter_to_dto(rule.filter);
    LifecycleRule {
        abort_incomplete_multipart_upload: rule.abort_incomplete_multipart_days.map(|days| {
            AbortIncompleteMultipartUpload {
                days_after_initiation: Some(days),
            }
        }),
        expiration: rule.expiration.as_ref().map(expiration_to_dto),
        filter,
        id: rule.id,
        noncurrent_version_expiration: rule
            .noncurrent_expiration
            .as_ref()
            .map(noncurrent_expiration_to_dto),
        noncurrent_version_transitions: None,
        prefix,
        status: ExpirationStatus::from_static(if rule.enabled {
            ExpirationStatus::ENABLED
        } else {
            ExpirationStatus::DISABLED
        }),
        transitions: None,
    }
}

fn filter_to_dto(filter: StoredLifecycleFilter) -> (Option<String>, Option<LifecycleRuleFilter>) {
    match filter {
        StoredLifecycleFilter::LegacyPrefix(prefix) => (Some(prefix), None),
        StoredLifecycleFilter::Empty => (None, Some(LifecycleRuleFilter::default())),
        StoredLifecycleFilter::Prefix(prefix) => (
            None,
            Some(LifecycleRuleFilter {
                prefix: Some(prefix),
                ..Default::default()
            }),
        ),
        StoredLifecycleFilter::Tag(tag) => (
            None,
            Some(LifecycleRuleFilter {
                tag: Some(tag_to_dto(tag)),
                ..Default::default()
            }),
        ),
        StoredLifecycleFilter::SizeGreaterThan(size) => (
            None,
            Some(LifecycleRuleFilter {
                object_size_greater_than: Some(size),
                ..Default::default()
            }),
        ),
        StoredLifecycleFilter::SizeLessThan(size) => (
            None,
            Some(LifecycleRuleFilter {
                object_size_less_than: Some(size),
                ..Default::default()
            }),
        ),
        StoredLifecycleFilter::And(and) => (
            None,
            Some(LifecycleRuleFilter {
                and: Some(and_filter_to_dto(and)),
                ..Default::default()
            }),
        ),
    }
}

fn and_filter_to_dto(and: StoredLifecycleAndFilter) -> LifecycleRuleAndOperator {
    LifecycleRuleAndOperator {
        object_size_greater_than: and.object_size_greater_than,
        object_size_less_than: and.object_size_less_than,
        prefix: and.prefix,
        tags: and
            .tags
            .is_empty()
            .not()
            .then(|| and.tags.into_iter().map(tag_to_dto).collect::<TagSet>()),
    }
}

fn tag_to_dto(tag: StoredTag) -> Tag {
    Tag {
        key: Some(tag.key),
        value: Some(tag.value),
    }
}

fn expiration_to_dto(expiration: &StoredLifecycleExpiration) -> LifecycleExpiration {
    LifecycleExpiration {
        date: expiration.date.map(Timestamp::from),
        days: expiration.days,
        expired_object_delete_marker: expiration.expired_object_delete_marker.then_some(true),
    }
}

fn noncurrent_expiration_to_dto(
    expiration: &StoredNoncurrentVersionExpiration,
) -> NoncurrentVersionExpiration {
    NoncurrentVersionExpiration {
        newer_noncurrent_versions: expiration.newer_noncurrent_versions,
        noncurrent_days: Some(expiration.noncurrent_days),
    }
}

fn percent_encode_rule_id(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                [char::from(byte), '\0', '\0']
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                [
                    '%',
                    char::from(HEX[usize::from(byte >> 4)]),
                    char::from(HEX[usize::from(byte & 0x0f)]),
                ]
            }
        })
        .filter(|ch| *ch != '\0')
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::{lifecycle_rule_matches_object, object_lifecycle_expiration};
    use s3s::dto::Transition;

    fn enabled_rule(expiration: Option<StoredLifecycleExpiration>) -> StoredLifecycleRule {
        StoredLifecycleRule {
            id: Some("rule id".to_string()),
            enabled: true,
            filter: StoredLifecycleFilter::Prefix("logs/".to_string()),
            expiration,
            noncurrent_expiration: None,
            abort_incomplete_multipart_days: None,
        }
    }

    fn stored_rule(id: &str, filter: StoredLifecycleFilter) -> StoredLifecycleRule {
        StoredLifecycleRule {
            id: Some(id.to_string()),
            enabled: true,
            filter,
            expiration: Some(StoredLifecycleExpiration {
                days: Some(1),
                date: None,
                expired_object_delete_marker: false,
            }),
            noncurrent_expiration: None,
            abort_incomplete_multipart_days: None,
        }
    }

    fn dto_rule() -> LifecycleRule {
        LifecycleRule {
            abort_incomplete_multipart_upload: None,
            expiration: Some(LifecycleExpiration {
                date: None,
                days: Some(1),
                expired_object_delete_marker: None,
            }),
            filter: Some(LifecycleRuleFilter::default()),
            id: Some("rule".to_string()),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            transitions: None,
        }
    }

    fn lifecycle_config(rules: Vec<LifecycleRule>) -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration { rules }
    }

    #[test]
    fn lifecycle_filter_matching_supports_prefix_tag_size_and_and() {
        let tags = vec![StoredTag {
            key: "class".to_string(),
            value: "tmp".to_string(),
        }];
        let mut rule = enabled_rule(None);
        assert!(lifecycle_rule_matches_object(&rule, "logs/one", 10, &tags,));
        rule.filter = StoredLifecycleFilter::Tag(tags[0].clone());
        assert!(lifecycle_rule_matches_object(&rule, "any", 10, &tags));
        rule.filter = StoredLifecycleFilter::And(StoredLifecycleAndFilter {
            prefix: Some("logs/".to_string()),
            tags: tags.clone(),
            object_size_greater_than: Some(1),
            object_size_less_than: Some(20),
        });
        assert!(lifecycle_rule_matches_object(&rule, "logs/one", 10, &tags,));
    }

    #[test]
    fn lifecycle_expiration_header_selects_earliest_matching_rule() -> S3Result<()> {
        let now = OffsetDateTime::UNIX_EPOCH;
        let rules = vec![enabled_rule(Some(StoredLifecycleExpiration {
            days: Some(2),
            date: None,
            expired_object_delete_marker: false,
        }))];

        let expiration = object_lifecycle_expiration(&rules, "logs/one", 10, now, &[])
            .ok_or(s3_error(S3ErrorCode::InternalError))?;
        let header = LifecycleExpirationHeader::from(expiration);
        assert_eq!(header.expires_at, now + time::Duration::days(2));
        let header_value = header.to_header_value()?;
        assert!(header_value.contains("rule-id=\"rule%20id\""));
        Ok(())
    }

    #[test]
    fn lifecycle_validation_rejects_days_zero_and_transitions() -> S3Result<()> {
        let err = stored_expiration_from_dto(LifecycleExpiration {
            days: Some(0),
            date: None,
            expired_object_delete_marker: None,
        })
        .err()
        .ok_or(s3_error(S3ErrorCode::InternalError))?;
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        let err = stored_lifecycle_rule_from_dto(LifecycleRule {
            abort_incomplete_multipart_upload: None,
            expiration: None,
            filter: Some(LifecycleRuleFilter::default()),
            id: None,
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            transitions: Some(vec![Transition::default()]),
        })
        .err()
        .ok_or(s3_error(S3ErrorCode::InternalError))?;
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        Ok(())
    }

    #[test]
    fn lifecycle_validation_rejects_rule_counts_and_malformed_shapes() {
        assert!(
            stored_lifecycle_rules_from_configuration(Some(lifecycle_config(Vec::new()))).is_err()
        );

        let too_many_rules = std::iter::repeat_with(dto_rule)
            .take(MAX_LIFECYCLE_RULES + 1)
            .collect();
        assert!(
            stored_lifecycle_rules_from_configuration(Some(lifecycle_config(too_many_rules)))
                .is_err()
        );
        assert!(stored_lifecycle_rules_from_configuration(None).is_err());
        assert!(
            stored_lifecycle_rule_from_dto(LifecycleRule {
                expiration: None,
                ..dto_rule()
            })
            .is_err()
        );
        assert!(
            stored_lifecycle_rule_from_dto(LifecycleRule {
                id: Some("x".repeat(MAX_LIFECYCLE_RULE_ID_CHARS + 1)),
                ..dto_rule()
            })
            .is_err()
        );
        assert!(
            stored_lifecycle_rule_from_dto(LifecycleRule {
                status: ExpirationStatus::from_static("Unknown"),
                ..dto_rule()
            })
            .is_err()
        );
        assert!(
            stored_filter_from_dto(
                Some("legacy/".to_string()),
                Some(LifecycleRuleFilter::default())
            )
            .is_err()
        );
        assert!(
            stored_filter_element_from_dto(LifecycleRuleFilter {
                prefix: Some("logs/".to_string()),
                tag: Some(Tag {
                    key: Some("class".to_string()),
                    value: Some("tmp".to_string()),
                }),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            stored_tag_from_dto(Tag {
                key: None,
                value: Some("tmp".to_string()),
            })
            .is_err()
        );
        assert!(
            stored_expiration_from_dto(LifecycleExpiration {
                days: Some(1),
                date: Some(OffsetDateTime::UNIX_EPOCH.into()),
                expired_object_delete_marker: None,
            })
            .is_err()
        );
        assert!(
            stored_noncurrent_expiration_from_dto(&NoncurrentVersionExpiration {
                newer_noncurrent_versions: None,
                noncurrent_days: None,
            })
            .is_err()
        );
        assert!(validate_newer_noncurrent_versions(0).is_err());
        assert!(validate_newer_noncurrent_versions(MAX_NEWER_NONCURRENT_VERSIONS + 1).is_err());
        assert!(validate_size(-1).is_err());
    }

    #[test]
    fn lifecycle_rules_from_stored_covers_status_and_actions() {
        let rules = lifecycle_rules_from_stored(vec![
            StoredLifecycleRule {
                id: Some("legacy".to_string()),
                enabled: false,
                filter: StoredLifecycleFilter::LegacyPrefix("old/".to_string()),
                expiration: None,
                noncurrent_expiration: Some(StoredNoncurrentVersionExpiration {
                    noncurrent_days: 5,
                    newer_noncurrent_versions: Some(2),
                }),
                abort_incomplete_multipart_days: Some(7),
            },
            StoredLifecycleRule {
                id: Some("empty".to_string()),
                enabled: true,
                filter: StoredLifecycleFilter::Empty,
                expiration: Some(StoredLifecycleExpiration {
                    days: None,
                    date: Some(OffsetDateTime::UNIX_EPOCH),
                    expired_object_delete_marker: false,
                }),
                noncurrent_expiration: None,
                abort_incomplete_multipart_days: None,
            },
        ]);

        assert_eq!(rules[0].status.as_str(), ExpirationStatus::DISABLED);
        assert_eq!(rules[0].prefix.as_deref(), Some("old/"));
        assert_eq!(
            rules[0]
                .abort_incomplete_multipart_upload
                .as_ref()
                .and_then(|abort| abort.days_after_initiation),
            Some(7)
        );
        assert_eq!(
            rules[0]
                .noncurrent_version_expiration
                .as_ref()
                .and_then(|expiration| expiration.newer_noncurrent_versions),
            Some(2)
        );
        assert!(
            rules[1]
                .filter
                .as_ref()
                .is_some_and(|filter| *filter == LifecycleRuleFilter::default())
        );
    }

    #[test]
    fn lifecycle_rules_from_stored_covers_filter_shapes() {
        let rules = lifecycle_rules_from_stored(vec![
            StoredLifecycleRule {
                id: Some("tag".to_string()),
                enabled: true,
                filter: StoredLifecycleFilter::Tag(StoredTag {
                    key: "class".to_string(),
                    value: "tmp".to_string(),
                }),
                expiration: Some(StoredLifecycleExpiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: true,
                }),
                noncurrent_expiration: None,
                abort_incomplete_multipart_days: None,
            },
            stored_rule("min-size", StoredLifecycleFilter::SizeGreaterThan(10)),
            stored_rule("max-size", StoredLifecycleFilter::SizeLessThan(100)),
            stored_rule(
                "and",
                StoredLifecycleFilter::And(StoredLifecycleAndFilter {
                    prefix: Some("logs/".to_string()),
                    tags: vec![StoredTag {
                        key: "class".to_string(),
                        value: "tmp".to_string(),
                    }],
                    object_size_greater_than: Some(1),
                    object_size_less_than: Some(20),
                }),
            ),
        ]);

        assert_eq!(
            rules[0]
                .filter
                .as_ref()
                .and_then(|filter| filter.tag.as_ref())
                .and_then(|tag| tag.key.as_deref()),
            Some("class")
        );
        assert_eq!(
            rules[1]
                .filter
                .as_ref()
                .and_then(|filter| filter.object_size_greater_than),
            Some(10)
        );
        assert_eq!(
            rules[2]
                .filter
                .as_ref()
                .and_then(|filter| filter.object_size_less_than),
            Some(100)
        );
        assert_eq!(
            rules[3]
                .filter
                .as_ref()
                .and_then(|filter| filter.and.as_ref())
                .and_then(|and| and.prefix.as_deref()),
            Some("logs/")
        );
    }
}
