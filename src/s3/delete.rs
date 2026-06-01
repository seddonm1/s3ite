//! S3 object delete request handling.

use super::{
    DeleteError, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
    DeletedObject, ETagCondition, Not, ObjectDeleteConditions, ObjectDeleteOutcome, S3ErrorCode,
    S3Request, S3Response, S3Result, Sqlite, SqliteError, s3_error_from_sqlite,
    s3_error_with_message, validate_delete_conditions,
};

/// Deletes an object by key.
#[tracing::instrument]
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
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let outcome = connection
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let metadata = match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                }
                None => Sqlite::try_get_current_metadata(&transaction, &key)?,
            };
            validate_delete_conditions(metadata.as_ref(), &conditions)?;
            let outcome = if version_id.is_none() && metadata.is_none() && conditions.has_any() {
                ObjectDeleteOutcome {
                    version_id: None,
                    is_delete_marker: false,
                    delete_marker_version_id: None,
                }
            } else {
                Sqlite::try_delete_object(&transaction, &key, version_id.as_deref())?
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
#[tracing::instrument]
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
        .try_get_connection(&bucket)
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
                        Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                    }
                    None => Sqlite::try_get_current_metadata(&transaction, &key)?,
                };
                match validate_delete_conditions(metadata.as_ref(), &conditions) {
                    Ok(()) => {
                        let outcome = if version_id.is_none()
                            && metadata.is_none()
                            && conditions.has_any()
                        {
                            ObjectDeleteOutcome {
                                version_id: None,
                                is_delete_marker: false,
                                delete_marker_version_id: None,
                            }
                        } else {
                            Sqlite::try_delete_object(&transaction, &key, version_id.as_deref())?
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
