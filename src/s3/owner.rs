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
pub(super) async fn get_bucket_acl(
    sqlite: &Sqlite,
    req: S3Request<GetBucketAclInput>,
) -> S3Result<S3Response<GetBucketAclOutput>> {
    let GetBucketAclInput { bucket, .. } = req.input;
    sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    let output = GetBucketAclOutput {
        grants: Some(vec![owner_full_control_grant()]),
        owner: Some(owner()),
    };
    Ok(S3Response::new(output))
}

/// Returns the fixed owner ACL for an existing object.
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
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            match version_id.as_deref() {
                Some(version_id) => Sqlite::get_version_metadata(&transaction, &key, version_id)?
                    .filter(|metadata| metadata.is_delete_marker.not())
                    .ok_or(SqliteError::ObjectVersionNotFound)?,
                None => {
                    Sqlite::get_metadata(&transaction, &key)?.ok_or(SqliteError::ObjectNotFound)?
                }
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
    sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(PutBucketAclOutput::default()))
}

/// Accepts owner-equivalent object ACL writes as no-ops.
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

    sqlite
        .validate_mutable_bucket(&bucket)
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

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |connection| {
            let transaction = connection.transaction()?;
            match version_id.as_deref() {
                Some(version_id) => Sqlite::get_version_metadata(&transaction, &key, version_id)?
                    .filter(|metadata| metadata.is_delete_marker.not())
                    .ok_or(SqliteError::ObjectVersionNotFound)?,
                None => {
                    Sqlite::get_metadata(&transaction, &key)?.ok_or(SqliteError::ObjectNotFound)?
                }
            };
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(PutObjectAclOutput::default()))
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

    #[test]
    fn returns_canonical_owner_and_full_control_grant() {
        let expected_owner = owner();
        assert_eq!(
            expected_owner.display_name.as_deref(),
            Some(OWNER_DISPLAY_NAME)
        );
        assert_eq!(expected_owner.id.as_deref(), Some(OWNER_ID));

        let grant = owner_full_control_grant();
        let Some(grantee) = grant.grantee else {
            panic!("expected grantee");
        };
        assert_eq!(grantee.display_name.as_deref(), Some(OWNER_DISPLAY_NAME));
        assert_eq!(grantee.id.as_deref(), Some(OWNER_ID));
        assert_eq!(grantee.type_.as_str(), Type::CANONICAL_USER);
        assert_eq!(
            grant.permission.as_ref().map(Permission::as_str),
            Some(Permission::FULL_CONTROL)
        );
    }

    #[test]
    fn validates_owner_only_bucket_acls() {
        assert!(
            validate_bucket_acl(&BucketAclRequest {
                acl: None,
                has_access_control_policy: false,
                grant_header_count: 0,
            })
            .is_ok()
        );

        let private = BucketCannedACL::from_static(BucketCannedACL::PRIVATE);
        assert!(
            validate_bucket_acl(&BucketAclRequest {
                acl: Some(&private),
                has_access_control_policy: false,
                grant_header_count: 0,
            })
            .is_ok()
        );

        let public = BucketCannedACL::from_static(BucketCannedACL::PUBLIC_READ);
        assert_error_code(
            validate_bucket_acl(&BucketAclRequest {
                acl: Some(&public),
                has_access_control_policy: false,
                grant_header_count: 0,
            }),
            &S3ErrorCode::AccessControlListNotSupported,
        );
        assert_error_code(
            validate_bucket_acl(&BucketAclRequest {
                acl: None,
                has_access_control_policy: true,
                grant_header_count: 0,
            }),
            &S3ErrorCode::AccessControlListNotSupported,
        );
        assert_error_code(
            validate_bucket_acl(&BucketAclRequest {
                acl: None,
                has_access_control_policy: false,
                grant_header_count: 1,
            }),
            &S3ErrorCode::AccessControlListNotSupported,
        );
    }

    #[test]
    fn validates_owner_only_object_acls() {
        assert!(
            validate_object_acl(&ObjectAclRequest {
                acl: None,
                has_access_control_policy: false,
                grant_header_count: 0,
            })
            .is_ok()
        );

        for canned_acl in [
            ObjectCannedACL::PRIVATE,
            ObjectCannedACL::BUCKET_OWNER_FULL_CONTROL,
            ObjectCannedACL::BUCKET_OWNER_READ,
        ] {
            let acl = ObjectCannedACL::from_static(canned_acl);
            assert!(
                validate_object_acl(&ObjectAclRequest {
                    acl: Some(&acl),
                    has_access_control_policy: false,
                    grant_header_count: 0,
                })
                .is_ok(),
                "{canned_acl}"
            );
        }

        for canned_acl in [
            ObjectCannedACL::PUBLIC_READ,
            ObjectCannedACL::AUTHENTICATED_READ,
            ObjectCannedACL::AWS_EXEC_READ,
        ] {
            let acl = ObjectCannedACL::from_static(canned_acl);
            assert_error_code(
                validate_object_acl(&ObjectAclRequest {
                    acl: Some(&acl),
                    has_access_control_policy: false,
                    grant_header_count: 0,
                }),
                &S3ErrorCode::AccessControlListNotSupported,
            );
        }

        assert_error_code(
            validate_object_acl(&ObjectAclRequest {
                acl: None,
                has_access_control_policy: true,
                grant_header_count: 0,
            }),
            &S3ErrorCode::AccessControlListNotSupported,
        );
        assert_error_code(
            validate_object_acl(&ObjectAclRequest {
                acl: None,
                has_access_control_policy: false,
                grant_header_count: 1,
            }),
            &S3ErrorCode::AccessControlListNotSupported,
        );
    }
}
