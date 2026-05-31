use std::ops::Not;

use s3s::dto::{ETag, ETagCondition};
use time::OffsetDateTime;

use crate::sqlite::{KeyMetadata, ObjectTagCondition, ObjectWriteConditions, SqliteError};

#[derive(Clone, Debug)]
pub(super) struct ObjectReadConditions {
    pub(super) etag_matches: Option<ETagCondition>,
    pub(super) modified_since: Option<OffsetDateTime>,
    pub(super) etag_none_matches: Option<ETagCondition>,
    pub(super) unmodified_since: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ObjectReadConditionFailure {
    NotModified,
    PreconditionFailed,
}

#[derive(Clone, Debug)]
pub(super) struct ObjectDeleteConditions {
    pub(super) etag_matches: Option<ETagCondition>,
    pub(super) last_modified_time: Option<OffsetDateTime>,
    pub(super) size: Option<i64>,
}

/// Converts S3 write condition headers into storage-neutral object conditions.
#[must_use]
pub(super) fn object_write_conditions(
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
) -> ObjectWriteConditions {
    ObjectWriteConditions {
        if_match: if_match.map(strong_object_tag_condition),
        if_none_match: if_none_match.map(weak_object_tag_condition),
    }
}

/// Converts an `If-Match` value into a strong storage tag condition.
#[must_use]
pub(super) fn strong_object_tag_condition(condition: ETagCondition) -> ObjectTagCondition {
    match condition {
        ETagCondition::Any => ObjectTagCondition::Any,
        ETagCondition::ETag(ETag::Strong(md5)) => ObjectTagCondition::Md5(md5),
        ETagCondition::ETag(ETag::Weak(_)) => ObjectTagCondition::Never,
    }
}

/// Converts an `If-None-Match` value into a weak storage tag condition.
#[must_use]
pub(super) fn weak_object_tag_condition(condition: ETagCondition) -> ObjectTagCondition {
    match condition {
        ETagCondition::Any => ObjectTagCondition::Any,
        ETagCondition::ETag(etag) => ObjectTagCondition::Md5(etag.into_value()),
    }
}

/// Drops sub-second precision to match HTTP date validator precision.
#[must_use]
pub(super) fn http_date_time(value: OffsetDateTime) -> OffsetDateTime {
    match value.replace_nanosecond(0) {
        Ok(value) => value,
        Err(_) => value,
    }
}

/// Returns whether an S3 `If-Match` style condition matches stored object metadata.
#[must_use]
pub(super) fn strong_etag_condition_matches(condition: &ETagCondition, md5: Option<&str>) -> bool {
    match condition {
        ETagCondition::Any => md5.is_some(),
        ETagCondition::ETag(ETag::Strong(value)) => md5 == Some(value.as_str()),
        ETagCondition::ETag(ETag::Weak(_)) => false,
    }
}

/// Returns whether an S3 `If-None-Match` style condition matches stored object metadata.
#[must_use]
pub(super) fn weak_etag_condition_matches(condition: &ETagCondition, md5: Option<&str>) -> bool {
    match condition {
        ETagCondition::Any => md5.is_some(),
        ETagCondition::ETag(etag) => md5 == Some(etag.value()),
    }
}

/// Applies GET/HEAD conditional request semantics to loaded object metadata.
pub(super) fn validate_read_conditions(
    metadata: &KeyMetadata,
    conditions: &ObjectReadConditions,
) -> Result<(), ObjectReadConditionFailure> {
    let md5 = metadata.md5.as_deref();
    if let Some(condition) = conditions.etag_matches.as_ref()
        && strong_etag_condition_matches(condition, md5).not()
    {
        return Err(ObjectReadConditionFailure::PreconditionFailed);
    }

    if let Some(condition) = conditions.etag_none_matches.as_ref() {
        if weak_etag_condition_matches(condition, md5) {
            return Err(ObjectReadConditionFailure::NotModified);
        }
    } else if let Some(since) = conditions.modified_since
        && http_date_time(metadata.last_modified) <= http_date_time(since)
    {
        return Err(ObjectReadConditionFailure::NotModified);
    }

    if conditions.etag_matches.is_none()
        && let Some(since) = conditions.unmodified_since
        && http_date_time(metadata.last_modified) > http_date_time(since)
    {
        return Err(ObjectReadConditionFailure::PreconditionFailed);
    }

    Ok(())
}

/// Applies copy-source conditional request semantics to loaded source metadata.
pub(super) fn validate_copy_source_conditions(
    metadata: &KeyMetadata,
    conditions: &ObjectReadConditions,
) -> Result<(), SqliteError> {
    let md5 = metadata.md5.as_deref();
    if let Some(condition) = conditions.etag_matches.as_ref()
        && strong_etag_condition_matches(condition, md5).not()
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if let Some(condition) = conditions.etag_none_matches.as_ref() {
        if weak_etag_condition_matches(condition, md5) {
            return Err(SqliteError::PreconditionFailed);
        }
    } else if let Some(since) = conditions.modified_since
        && http_date_time(metadata.last_modified) <= http_date_time(since)
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if conditions.etag_matches.is_none()
        && let Some(since) = conditions.unmodified_since
        && http_date_time(metadata.last_modified) > http_date_time(since)
    {
        return Err(SqliteError::PreconditionFailed);
    }

    Ok(())
}

/// Applies delete conditional request semantics inside a write transaction.
pub(super) fn validate_delete_conditions(
    metadata: Option<&KeyMetadata>,
    conditions: &ObjectDeleteConditions,
) -> Result<(), SqliteError> {
    let Some(metadata) = metadata else {
        return Ok(());
    };

    if let Some(condition) = conditions.etag_matches.as_ref()
        && match condition {
            ETagCondition::Any => false,
            ETagCondition::ETag(_) => {
                strong_etag_condition_matches(condition, metadata.md5.as_deref()).not()
            }
        }
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if let Some(last_modified_time) = conditions.last_modified_time
        && http_date_time(metadata.last_modified) != http_date_time(last_modified_time)
    {
        return Err(SqliteError::PreconditionFailed);
    }

    if let Some(size) = conditions.size
        && metadata.size != size
    {
        return Err(SqliteError::PreconditionFailed);
    }

    Ok(())
}
