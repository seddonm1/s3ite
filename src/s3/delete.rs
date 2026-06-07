//! S3 object delete request handling.

use super::{
    DeleteError, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
    DeletedObject, ETagCondition, KeyMetadata, Not, ObjectDeleteOutcome, OffsetDateTime,
    S3ErrorCode, S3Request, S3Response, S3Result, Sqlite, SqliteError, http_date_time,
    s3_error_from_sqlite, s3_error_with_message, strong_etag_condition_matches,
};

#[derive(Clone, Debug)]
struct ObjectDeleteConditions {
    etag_matches: Option<ETagCondition>,
    last_modified_time: Option<OffsetDateTime>,
    size: Option<i64>,
}

impl ObjectDeleteConditions {
    /// Returns whether the delete request supplied at least one precondition.
    #[must_use]
    fn has_any(&self) -> bool {
        self.etag_matches.is_some() || self.last_modified_time.is_some() || self.size.is_some()
    }
}

/// Applies delete conditional request semantics inside a write transaction.
fn validate_delete_conditions(
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

/// Deletes an object by key.
pub(super) async fn delete_object(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let DeleteObjectInput {
        bucket,
        key,
        version_id,
        if_match,
        if_match_last_modified_time,
        if_match_size,
        ..
    } = req.input;
    let conditions = ObjectDeleteConditions {
        etag_matches: if_match,
        last_modified_time: if_match_last_modified_time.map(Into::into),
        size: if_match_size,
    };

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let outcome = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let metadata = match version_id.as_deref() {
                Some(version_id) => Sqlite::get_version_metadata(&transaction, &key, version_id)?,
                None => Sqlite::get_current_metadata(&transaction, &key)?,
            };
            validate_delete_conditions(metadata.as_ref(), &conditions)?;
            let outcome = if version_id.is_none() && metadata.is_none() && conditions.has_any() {
                ObjectDeleteOutcome {
                    version_id: None,
                    is_delete_marker: false,
                    delete_marker_version_id: None,
                }
            } else {
                Sqlite::delete_object(&transaction, &key, version_id.as_deref())?
            };
            transaction.commit()?;
            Ok(outcome)
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteObjectOutput {
        delete_marker: outcome.is_delete_marker.then_some(true),
        version_id: outcome.version_id,
        ..Default::default()
    }))
}

/// Deletes a batch of objects and returns S3 multi-delete results.
pub(super) async fn delete_objects(
    sqlite: &Sqlite,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let DeleteObjectsInput { bucket, delete, .. } = req.input;

    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    let quiet = delete.quiet.unwrap_or(false);
    let requested_deletions = delete
        .objects
        .into_iter()
        .map(|object| {
            (
                object.key,
                object.version_id,
                ObjectDeleteConditions {
                    etag_matches: object.e_tag.map(ETagCondition::ETag),
                    last_modified_time: object.last_modified_time.map(Into::into),
                    size: object.size,
                },
            )
        })
        .collect::<Vec<_>>();
    if requested_deletions.len() > 1000 {
        return Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "DeleteObjects supports at most 1000 keys",
        ));
    }

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let (deleted_entries, errors) = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let mut deleted_entries = Vec::new();
            let mut errors = Vec::new();

            for (key, version_id, conditions) in requested_deletions {
                let metadata = match version_id.as_deref() {
                    Some(version_id) => {
                        Sqlite::get_version_metadata(&transaction, &key, version_id)?
                    }
                    None => Sqlite::get_current_metadata(&transaction, &key)?,
                };
                match validate_delete_conditions(metadata.as_ref(), &conditions) {
                    Ok(()) => {
                        let outcome =
                            if version_id.is_none() && metadata.is_none() && conditions.has_any() {
                                ObjectDeleteOutcome {
                                    version_id: None,
                                    is_delete_marker: false,
                                    delete_marker_version_id: None,
                                }
                            } else {
                                Sqlite::delete_object(&transaction, &key, version_id.as_deref())?
                            };
                        deleted_entries.push((key, outcome));
                    }
                    Err(SqliteError::PreconditionFailed) => errors.push(DeleteError {
                        key: Some(key),
                        code: Some("PreconditionFailed".to_string()),
                        message: Some("At least one precondition failed".to_string()),
                        ..Default::default()
                    }),
                    Err(err) => return Err(err),
                }
            }

            transaction.commit()?;
            Ok((deleted_entries, errors))
        })
        .await
        .map_err(s3_error_from_sqlite)?;

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

    use s3s::dto::{ETag, ETagCondition};
    use time::Duration;

    fn metadata(md5: Option<&str>, last_modified: OffsetDateTime, size: i64) -> KeyMetadata {
        KeyMetadata {
            version_id: "version".to_string(),
            size,
            metadata: None,
            content_type: None,
            content_encoding: None,
            cache_control: None,
            expires: None,
            last_modified,
            md5: md5.map(str::to_string),
            checksum: None,
            encryption: None,
            is_latest: true,
            is_delete_marker: false,
        }
    }

    fn delete_conditions(
        etag_matches: Option<ETagCondition>,
        last_modified_time: Option<OffsetDateTime>,
        size: Option<i64>,
    ) -> ObjectDeleteConditions {
        ObjectDeleteConditions {
            etag_matches,
            last_modified_time,
            size,
        }
    }

    fn assert_precondition_failed(result: &Result<(), SqliteError>) {
        assert!(matches!(result, Err(SqliteError::PreconditionFailed)));
    }

    #[test]
    fn applies_delete_conditions() {
        let last_modified = OffsetDateTime::UNIX_EPOCH + Duration::seconds(10);
        let object = metadata(Some("md5"), last_modified, 5);

        assert!(
            validate_delete_conditions(
                None,
                &delete_conditions(
                    Some(ETagCondition::ETag(ETag::Strong("other".to_string()))),
                    Some(last_modified + Duration::seconds(1)),
                    Some(9),
                ),
            )
            .is_ok()
        );
        assert!(
            validate_delete_conditions(
                Some(&object),
                &delete_conditions(
                    Some(ETagCondition::ETag(ETag::Strong("md5".to_string()))),
                    Some(last_modified),
                    Some(5),
                ),
            )
            .is_ok()
        );
        assert!(
            validate_delete_conditions(
                Some(&object),
                &delete_conditions(Some(ETagCondition::Any), None, None),
            )
            .is_ok()
        );
        assert_precondition_failed(&validate_delete_conditions(
            Some(&object),
            &delete_conditions(
                Some(ETagCondition::ETag(ETag::Strong("other".to_string()))),
                None,
                None,
            ),
        ));
        assert_precondition_failed(&validate_delete_conditions(
            Some(&object),
            &delete_conditions(None, Some(last_modified + Duration::seconds(1)), None),
        ));
        assert_precondition_failed(&validate_delete_conditions(
            Some(&object),
            &delete_conditions(None, None, Some(9)),
        ));
    }
}
