//! S3 bucket lifecycle request handling, validation, and rule evaluation.

use std::{collections::HashSet, ops::Not};

use time::{OffsetDateTime, format_description::well_known::Rfc2822};

use super::{
    DeleteBucketLifecycleInput, DeleteBucketLifecycleOutput, ExpirationStatus,
    GetBucketLifecycleConfigurationInput, GetBucketLifecycleConfigurationOutput,
    LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
    LifecycleRules, NoncurrentVersionExpiration, PutBucketLifecycleConfigurationInput,
    PutBucketLifecycleConfigurationOutput, S3ErrorCode, S3Request, S3Response, S3Result, Sqlite,
    StoredTag, Tag, TagSet, Timestamp, s3_error, s3_error_from_sqlite, s3_error_with_message,
    validate_expected_bucket_owner,
};

use s3s::dto::{AbortIncompleteMultipartUpload, BucketLifecycleConfiguration};

const MAX_LIFECYCLE_RULES: usize = 1000;
const MAX_LIFECYCLE_RULE_ID_CHARS: usize = 255;
const MAX_NEWER_NONCURRENT_VERSIONS: i32 = 100;

/// Normalized S3 bucket lifecycle rule persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLifecycleRule {
    pub(crate) id: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) filter: StoredLifecycleFilter,
    pub(crate) expiration: Option<StoredLifecycleExpiration>,
    pub(crate) noncurrent_expiration: Option<StoredNoncurrentVersionExpiration>,
    pub(crate) abort_incomplete_multipart_days: Option<i32>,
}

/// Normalized lifecycle filter persisted by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StoredLifecycleFilter {
    Empty,
    LegacyPrefix(String),
    Prefix(String),
    Tag(StoredTag),
    SizeGreaterThan(i64),
    SizeLessThan(i64),
    And(StoredLifecycleAndFilter),
}

/// Normalized lifecycle `And` filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLifecycleAndFilter {
    pub(crate) prefix: Option<String>,
    pub(crate) tags: Vec<StoredTag>,
    pub(crate) object_size_greater_than: Option<i64>,
    pub(crate) object_size_less_than: Option<i64>,
}

/// Normalized current-version expiration action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLifecycleExpiration {
    pub(crate) days: Option<i32>,
    pub(crate) date: Option<OffsetDateTime>,
    pub(crate) expired_object_delete_marker: bool,
}

/// Normalized noncurrent-version expiration action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredNoncurrentVersionExpiration {
    pub(crate) noncurrent_days: i32,
    pub(crate) newer_noncurrent_versions: Option<i32>,
}

/// Header value for lifecycle expiration on object reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleExpirationHeader {
    pub(crate) expires_at: OffsetDateTime,
    pub(crate) rule_id: String,
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

/// Returns whether a lifecycle rule matches the supplied object attributes.
#[must_use]
pub(crate) fn lifecycle_rule_matches_object(
    rule: &StoredLifecycleRule,
    key: &str,
    size: i64,
    tags: &[StoredTag],
) -> bool {
    lifecycle_filter_matches_object(&rule.filter, key, size, tags)
}

fn lifecycle_filter_matches_object(
    filter: &StoredLifecycleFilter,
    key: &str,
    size: i64,
    tags: &[StoredTag],
) -> bool {
    match filter {
        StoredLifecycleFilter::Empty => true,
        StoredLifecycleFilter::LegacyPrefix(prefix) | StoredLifecycleFilter::Prefix(prefix) => {
            key.starts_with(prefix)
        }
        StoredLifecycleFilter::Tag(tag) => tags.iter().any(|stored| stored == tag),
        StoredLifecycleFilter::SizeGreaterThan(min) => size > *min,
        StoredLifecycleFilter::SizeLessThan(max) => size < *max,
        StoredLifecycleFilter::And(and) => {
            and.prefix
                .as_ref()
                .is_none_or(|prefix| key.starts_with(prefix))
                && and.object_size_greater_than.is_none_or(|min| size > min)
                && and.object_size_less_than.is_none_or(|max| size < max)
                && and
                    .tags
                    .iter()
                    .all(|tag| tags.iter().any(|stored| stored == tag))
        }
    }
}

/// Returns the eligibility time for a current-version expiration action.
pub(crate) fn current_expiration_time(
    expiration: &StoredLifecycleExpiration,
    last_modified: OffsetDateTime,
) -> Option<OffsetDateTime> {
    if expiration.expired_object_delete_marker {
        return None;
    }
    expiration.date.or_else(|| {
        expiration
            .days
            .map(|days| last_modified + time::Duration::days(i64::from(days)))
    })
}

/// Returns the eligibility time for a noncurrent-version expiration action.
#[must_use]
pub(crate) fn noncurrent_expiration_time(
    expiration: &StoredNoncurrentVersionExpiration,
    last_modified: OffsetDateTime,
) -> OffsetDateTime {
    last_modified + time::Duration::days(i64::from(expiration.noncurrent_days))
}

/// Selects the lifecycle expiration header for an object when one applies.
#[must_use]
pub(crate) fn lifecycle_expiration_header(
    rules: &[StoredLifecycleRule],
    key: &str,
    size: i64,
    last_modified: OffsetDateTime,
    tags: &[StoredTag],
) -> Option<LifecycleExpirationHeader> {
    rules
        .iter()
        .filter(|rule| rule.enabled && lifecycle_rule_matches_object(rule, key, size, tags))
        .filter_map(|rule| {
            let expiration = rule.expiration.as_ref()?;
            let expires_at = current_expiration_time(expiration, last_modified)?;
            Some(LifecycleExpirationHeader {
                expires_at,
                rule_id: rule.id.clone().unwrap_or_default(),
            })
        })
        .min_by_key(|header| header.expires_at)
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

    #[test]
    fn lifecycle_filter_matching_supports_prefix_tag_size_and_and() {
        let tags = vec![StoredTag {
            key: "class".to_string(),
            value: "tmp".to_string(),
        }];
        assert!(lifecycle_filter_matches_object(
            &StoredLifecycleFilter::Prefix("logs/".to_string()),
            "logs/one",
            10,
            &tags,
        ));
        assert!(lifecycle_filter_matches_object(
            &StoredLifecycleFilter::Tag(tags[0].clone()),
            "any",
            10,
            &tags,
        ));
        assert!(lifecycle_filter_matches_object(
            &StoredLifecycleFilter::And(StoredLifecycleAndFilter {
                prefix: Some("logs/".to_string()),
                tags: tags.clone(),
                object_size_greater_than: Some(1),
                object_size_less_than: Some(20),
            }),
            "logs/one",
            10,
            &tags,
        ));
    }

    #[test]
    fn lifecycle_expiration_header_selects_earliest_matching_rule() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let rules = vec![enabled_rule(Some(StoredLifecycleExpiration {
            days: Some(2),
            date: None,
            expired_object_delete_marker: false,
        }))];

        let Some(header) = lifecycle_expiration_header(&rules, "logs/one", 10, now, &[]) else {
            panic!("expiration header");
        };
        assert_eq!(header.expires_at, now + time::Duration::days(2));
        let Ok(header_value) = header.to_header_value() else {
            panic!("expiration header should format");
        };
        assert!(header_value.contains("rule-id=\"rule%20id\""));
    }

    #[test]
    fn lifecycle_validation_rejects_days_zero_and_transitions() {
        let Err(err) = stored_expiration_from_dto(LifecycleExpiration {
            days: Some(0),
            date: None,
            expired_object_delete_marker: None,
        }) else {
            panic!("days=0 is invalid");
        };
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        let Err(err) = stored_lifecycle_rule_from_dto(LifecycleRule {
            abort_incomplete_multipart_upload: None,
            expiration: None,
            filter: Some(LifecycleRuleFilter::default()),
            id: None,
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            transitions: Some(vec![Transition::default()]),
        }) else {
            panic!("transitions are unsupported");
        };
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }
}
