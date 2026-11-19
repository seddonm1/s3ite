//! S3 object delete request handling.

use crate::sqlite::{KeyMetadata, ObjectDeleteOutcome};
use crate::telemetry::audit_deletion;

use super::{
    DeleteError, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
    DeletedObject, ETagCondition, Not, OffsetDateTime, S3ErrorCode, S3Request, S3Response,
    S3Result, Sqlite, SqliteError, delete_requires_object_lock_check, s3_error_from_sqlite,
    s3_error_with_message, strong_etag_condition_matches, validate_expected_bucket_owner,
    validate_object_lock_delete,
};
use s3s::dto::Delete;

type RequestedDeletion = (String, Option<String>, Option<ETagCondition>);

/// Rejects conditions that AWS restricts to directory buckets.
fn validate_delete_condition_fields(has_time: bool, has_size: bool) -> S3Result<()> {
    if has_time || has_size {
        // AWS DeleteObject and ObjectIdentifier limit these fields to directory
        // buckets. They document no specific code for a general-purpose bucket.
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "Last-modified-time and size delete conditions require a directory bucket",
        ));
    }
    Ok(())
}

/// Applies an `ETag` precondition to metadata selected in the write transaction.
fn validate_delete_conditions(
    metadata: Option<&KeyMetadata>,
    condition: Option<&ETagCondition>,
    current: bool,
) -> Result<(), SqliteError> {
    // AWS documents a current delete marker separately from a missing row.
    // The caller applies its missing-key rules. Preserve explicit-version
    // conditional behavior because AWS does not define the combined request.
    if let (Some(metadata), Some(condition)) = (metadata, condition)
        && match condition {
            ETagCondition::Any => current && metadata.is_delete_marker,
            ETagCondition::ETag(_) => {
                !strong_etag_condition_matches(condition, metadata.md5.as_deref())
            }
        }
    {
        return Err(SqliteError::PreconditionFailed);
    }
    Ok(())
}

/// Extracts per-key `ETag` conditions and rejects directory-only XML fields.
fn requested_deletions(delete: Delete) -> S3Result<Vec<RequestedDeletion>> {
    let deletions = delete
        .objects
        .into_iter()
        .map(|object| {
            validate_delete_condition_fields(
                object.last_modified_time.is_some(),
                object.size.is_some(),
            )?;
            let condition = object.e_tag.map(|value| {
                // AWS conditional-deletes uses a bare * as the bulk wildcard.
                if value.as_strong() == Some("*") {
                    ETagCondition::Any
                } else {
                    ETagCondition::ETag(value)
                }
            });
            Ok((object.key, object.version_id, condition))
        })
        .collect::<S3Result<Vec<_>>>()?;
    if deletions.len() > 1000 {
        return Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "DeleteObjects supports at most 1000 keys",
        ));
    }
    Ok(deletions)
}

/// Returns a per-key error for an Object Lock denial.
fn access_denied_delete_error(key: String, version_id: Option<String>) -> DeleteError {
    DeleteError {
        key: Some(key),
        version_id,
        code: Some("AccessDenied".to_string()),
        message: Some("Access Denied".to_string()),
    }
}

/// Preserves the requested version on a failed conditional bulk delete.
fn precondition_failed_delete_error(key: String, version_id: Option<String>) -> DeleteError {
    DeleteError {
        key: Some(key),
        version_id,
        code: Some("PreconditionFailed".to_string()),
        message: Some("At least one precondition failed".to_string()),
    }
}

/// Deletes an object by key.
pub(super) async fn delete_object(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let DeleteObjectInput {
        bucket,
        expected_bucket_owner,
        bypass_governance_retention,
        key,
        version_id,
        if_match,
        if_match_last_modified_time,
        if_match_size,
        ..
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    validate_delete_condition_fields(
        if_match_last_modified_time.is_some(),
        if_match_size.is_some(),
    )?;
    let bypass_governance = bypass_governance_retention.unwrap_or(false);

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let outcome = connection
        .write(move |transaction| {
            let versioning_state = Sqlite::get_bucket_versioning_state(transaction)?;
            let metadata = match version_id.as_deref() {
                Some(version_id) => Sqlite::get_version_metadata(transaction, &key, version_id)?,
                None => Sqlite::get_current_metadata(transaction, &key)?,
            };
            // AWS conditional deletes: "If the object doesn't exist when
            // evaluating either of the preconditions, S3 rejects the request
            // and returns a Not Found error response."
            if version_id.is_none() && metadata.is_none() && if_match.is_some() {
                return Err(SqliteError::ObjectNotFound);
            }
            validate_delete_conditions(metadata.as_ref(), if_match.as_ref(), version_id.is_none())?;
            if let Some(metadata) = metadata.as_ref()
                && delete_requires_object_lock_check(versioning_state, version_id.as_deref())
                && let Err(err) = validate_object_lock_delete(
                    metadata,
                    bypass_governance,
                    OffsetDateTime::now_utc(),
                )
            {
                return Ok(Err(err));
            }
            Ok(Ok(Sqlite::delete_object(
                transaction,
                &key,
                version_id.as_deref(),
            )?))
        })
        .await
        .map_err(s3_error_from_sqlite)??;

    Ok(S3Response::new(DeleteObjectOutput {
        delete_marker: outcome.is_delete_marker.then_some(true),
        version_id: outcome.version_id,
        ..Default::default()
    }))
}

/// Emits per-key outcomes only after the writer confirms the transaction committed.
fn audit_bulk_results(deleted_entries: &[(String, ObjectDeleteOutcome)], errors: &[DeleteError]) {
    for (key, outcome) in deleted_entries {
        audit_deletion(
            key,
            outcome.version_id.as_deref(),
            outcome.delete_marker_version_id.as_deref(),
            None,
        );
    }
    for error in errors {
        if let Some(key) = &error.key {
            audit_deletion(
                key,
                error.version_id.as_deref(),
                None,
                error.code.as_deref(),
            );
        }
    }
}

/// Deletes a batch of objects and returns S3 multi-delete results.
pub(super) async fn delete_objects(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let DeleteObjectsInput {
        bucket,
        delete,
        bypass_governance_retention,
        expected_bucket_owner,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let quiet = delete.quiet.unwrap_or(false);
    let bypass_governance = bypass_governance_retention.unwrap_or(false);
    let requested_deletions = requested_deletions(delete)?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let (deleted_entries, errors) = connection
        .write(move |transaction| {
            let versioning_state = Sqlite::get_bucket_versioning_state(transaction)?;
            let mut deleted_entries = Vec::new();
            let mut errors = Vec::new();

            for (key, version_id, conditions) in requested_deletions {
                let metadata = match version_id.as_deref() {
                    Some(version_id) => {
                        Sqlite::get_version_metadata(transaction, &key, version_id)?
                    }
                    None => Sqlite::get_current_metadata(transaction, &key)?,
                };
                // Conditional deletes places missing conditional keys in the
                // per-object error results. Preserve explicit VersionId handling.
                if version_id.is_none() && metadata.is_none() && conditions.is_some() {
                    errors.push(DeleteError {
                        key: Some(key),
                        version_id,
                        code: Some("NoSuchKey".to_string()),
                        message: Some("The specified key does not exist".to_string()),
                    });
                    continue;
                }
                match validate_delete_conditions(
                    metadata.as_ref(),
                    conditions.as_ref(),
                    version_id.is_none(),
                ) {
                    Ok(()) => {
                        if let Some(metadata) = metadata.as_ref()
                            && delete_requires_object_lock_check(
                                versioning_state,
                                version_id.as_deref(),
                            )
                            && validate_object_lock_delete(
                                metadata,
                                bypass_governance,
                                OffsetDateTime::now_utc(),
                            )
                            .is_err()
                        {
                            errors.push(access_denied_delete_error(key, version_id));
                            continue;
                        }
                        let outcome =
                            Sqlite::delete_object(transaction, &key, version_id.as_deref())?;
                        deleted_entries.push((key, outcome));
                    }
                    Err(SqliteError::PreconditionFailed) => {
                        errors.push(precondition_failed_delete_error(key, version_id));
                    }
                    Err(err) => return Err(err),
                }
            }

            Ok((deleted_entries, errors))
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    audit_bulk_results(&deleted_entries, &errors);
    let output = DeleteObjectsOutput {
        deleted: quiet.not().then(|| {
            deleted_entries
                .into_iter()
                .map(
                    |(key, outcome): (String, ObjectDeleteOutcome)| DeletedObject {
                        key: Some(key),
                        version_id: outcome.version_id,
                        delete_marker: outcome.is_delete_marker.then_some(true),
                        delete_marker_version_id: outcome.delete_marker_version_id,
                    },
                )
                .collect::<Vec<_>>()
        }),
        errors: errors.is_empty().not().then_some(errors),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sqlite::StoredObjectLock;
    use s3s::dto::{ETag, ETagCondition};
    use time::Duration;

    /// Builds object metadata for conditional deletion tests.
    fn metadata(md5: Option<&str>, last_modified: OffsetDateTime, size: i64) -> KeyMetadata {
        KeyMetadata {
            version_id: "version".to_string(),
            size,
            metadata: None,
            content_type: None,
            content_encoding: None,
            content_disposition: None,
            content_language: None,
            cache_control: None,
            expires: None,
            last_modified,
            md5: md5.map(str::to_string),
            checksum: None,
            stored_sse: None,
            object_lock: StoredObjectLock::default(),
            is_latest: true,
            is_delete_marker: false,
        }
    }

    /// Covers existing, missing, and delete-marker metadata for `ETag` conditions.
    #[test]
    fn applies_delete_conditions() {
        let last_modified = OffsetDateTime::UNIX_EPOCH + Duration::seconds(10);
        let mut object = metadata(Some("md5"), last_modified, 5);
        let matched = ETagCondition::ETag(ETag::Strong("md5".to_string()));
        let mismatch = ETagCondition::ETag(ETag::Strong("other".to_string()));
        let wildcard = ETagCondition::Any;

        assert!(validate_delete_conditions(None, Some(&mismatch), true).is_ok());
        assert!(validate_delete_conditions(Some(&object), Some(&matched), true).is_ok());
        assert!(validate_delete_conditions(Some(&object), Some(&wildcard), true).is_ok());
        assert!(matches!(
            validate_delete_conditions(Some(&object), Some(&mismatch), true),
            Err(SqliteError::PreconditionFailed)
        ));

        object.is_delete_marker = true;
        assert!(matches!(
            validate_delete_conditions(Some(&object), Some(&wildcard), true),
            Err(SqliteError::PreconditionFailed)
        ));
        assert!(validate_delete_conditions(Some(&object), Some(&wildcard), false).is_ok());
    }
}
