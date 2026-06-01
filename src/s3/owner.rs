use super::{
    BucketCannedACL, GetBucketAclInput, GetBucketAclOutput, GetObjectAclInput, GetObjectAclOutput,
    Grant, Grantee, Not, OWNER_DISPLAY_NAME, OWNER_ID, ObjectCannedACL, Owner, Permission,
    PutBucketAclInput, PutBucketAclOutput, PutObjectAclInput, PutObjectAclOutput, S3Error,
    S3ErrorCode, S3Request, S3Response, S3Result, Sqlite, SqliteError, Type, s3_error_from_sqlite,
    s3_error_with_message,
};

/// Returns the canonical owner used by s3ite ACL and listing responses.
pub(super) fn owner() -> Owner {
    Owner {
        display_name: Some(OWNER_DISPLAY_NAME.to_string()),
        id: Some(OWNER_ID.to_string()),
    }
}

/// Owner-equivalent bucket ACL request fields.
#[derive(Clone, Copy, Debug)]
pub(super) struct BucketAclRequest<'a> {
    pub(super) acl: Option<&'a BucketCannedACL>,
    pub(super) has_access_control_policy: bool,
    pub(super) grant_header_count: usize,
}

/// Owner-equivalent object ACL request fields.
#[derive(Clone, Copy, Debug)]
pub(super) struct ObjectAclRequest<'a> {
    pub(super) acl: Option<&'a ObjectCannedACL>,
    pub(super) has_access_control_policy: bool,
    pub(super) grant_header_count: usize,
}

/// Returns an S3 error for ACL inputs that would require an ACL grant model.
fn acl_not_supported() -> S3Error {
    s3_error_with_message(
        S3ErrorCode::AccessControlListNotSupported,
        "ACL grants and public ACLs are not supported by s3ite's owner-only auth model",
    )
}

/// Returns the fixed owner `FULL_CONTROL` grant used by synthetic ACL reads.
fn owner_full_control_grant() -> Grant {
    let owner = owner();
    Grant {
        grantee: Some(Grantee {
            display_name: owner.display_name,
            email_address: None,
            id: owner.id,
            type_: Type::from_static(Type::CANONICAL_USER),
            uri: None,
        }),
        permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
    }
}

/// Returns whether a canned bucket ACL is owner-equivalent in the Level 1 model.
fn is_owner_bucket_acl(acl: &BucketCannedACL) -> bool {
    acl.as_str() == BucketCannedACL::PRIVATE
}

/// Returns whether a canned object ACL is owner-equivalent in the Level 1 model.
fn is_owner_object_acl(acl: &ObjectCannedACL) -> bool {
    matches!(
        acl.as_str(),
        ObjectCannedACL::PRIVATE
            | ObjectCannedACL::BUCKET_OWNER_FULL_CONTROL
            | ObjectCannedACL::BUCKET_OWNER_READ
    )
}

/// Validates that a bucket ACL request is an owner-only no-op.
pub(super) fn validate_bucket_acl(request: &BucketAclRequest<'_>) -> S3Result<()> {
    if request.has_access_control_policy || request.grant_header_count > 0 {
        return Err(acl_not_supported());
    }

    if let Some(acl) = request.acl
        && is_owner_bucket_acl(acl).not()
    {
        return Err(acl_not_supported());
    }

    Ok(())
}

/// Validates that an object ACL request is an owner-only no-op.
pub(super) fn validate_object_acl(request: &ObjectAclRequest<'_>) -> S3Result<()> {
    if request.has_access_control_policy || request.grant_header_count > 0 {
        return Err(acl_not_supported());
    }

    if let Some(acl) = request.acl
        && is_owner_object_acl(acl).not()
    {
        return Err(acl_not_supported());
    }

    Ok(())
}

/// Returns the fixed owner ACL for an existing bucket.
#[tracing::instrument]
pub(super) async fn get_bucket_acl(
    sqlite: &Sqlite,
    req: S3Request<GetBucketAclInput>,
) -> S3Result<S3Response<GetBucketAclOutput>> {
    let GetBucketAclInput { bucket, .. } = req.input;
    sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let output = GetBucketAclOutput {
        grants: Some(vec![owner_full_control_grant()]),
        owner: Some(owner()),
    };
    Ok(S3Response::new(output))
}

/// Returns the fixed owner ACL for an existing object.
#[tracing::instrument]
pub(super) async fn get_object_acl(
    sqlite: &Sqlite,
    req: S3Request<GetObjectAclInput>,
) -> S3Result<S3Response<GetObjectAclOutput>> {
    let GetObjectAclInput {
        bucket,
        key,
        version_id,
        ..
    } = req.input;

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                        .filter(|metadata| metadata.is_delete_marker.not())
                        .ok_or(SqliteError::ObjectVersionNotFound)?
                }
                None => Sqlite::try_get_metadata(&transaction, &key)?
                    .ok_or(SqliteError::ObjectNotFound)?,
            };
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    let owner = owner();
    let output = GetObjectAclOutput {
        grants: Some(vec![owner_full_control_grant()]),
        owner: Some(owner),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

/// Accepts owner-equivalent bucket ACL writes as no-ops.
#[tracing::instrument]
pub(super) async fn put_bucket_acl(
    sqlite: &Sqlite,
    req: S3Request<PutBucketAclInput>,
) -> S3Result<S3Response<PutBucketAclOutput>> {
    let PutBucketAclInput {
        acl,
        access_control_policy,
        bucket,
        grant_full_control,
        grant_read,
        grant_read_acp,
        grant_write,
        grant_write_acp,
        ..
    } = req.input;

    sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    validate_bucket_acl(&BucketAclRequest {
        acl: acl.as_ref(),
        has_access_control_policy: access_control_policy.is_some(),
        grant_header_count: [
            grant_full_control.is_some(),
            grant_read.is_some(),
            grant_read_acp.is_some(),
            grant_write.is_some(),
            grant_write_acp.is_some(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count(),
    })?;

    Ok(S3Response::new(PutBucketAclOutput::default()))
}

/// Accepts owner-equivalent object ACL writes as no-ops.
#[tracing::instrument]
pub(super) async fn put_object_acl(
    sqlite: &Sqlite,
    req: S3Request<PutObjectAclInput>,
) -> S3Result<S3Response<PutObjectAclOutput>> {
    let PutObjectAclInput {
        acl,
        access_control_policy,
        bucket,
        grant_full_control,
        grant_read,
        grant_read_acp,
        grant_write,
        grant_write_acp,
        key,
        version_id,
        ..
    } = req.input;

    let connection = sqlite
        .try_get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            match version_id.as_deref() {
                Some(version_id) => {
                    Sqlite::try_get_version_metadata(&transaction, &key, version_id)?
                        .filter(|metadata| metadata.is_delete_marker.not())
                        .ok_or(SqliteError::ObjectVersionNotFound)?
                }
                None => Sqlite::try_get_metadata(&transaction, &key)?
                    .ok_or(SqliteError::ObjectNotFound)?,
            };
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    validate_object_acl(&ObjectAclRequest {
        acl: acl.as_ref(),
        has_access_control_policy: access_control_policy.is_some(),
        grant_header_count: [
            grant_full_control.is_some(),
            grant_read.is_some(),
            grant_read_acp.is_some(),
            grant_write.is_some(),
            grant_write_acp.is_some(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count(),
    })?;

    Ok(S3Response::new(PutObjectAclOutput::default()))
}
