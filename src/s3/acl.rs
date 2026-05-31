use super::{
    GetObjectAclInput, GetObjectAclOutput, Grant, Grantee, Not, Permission, S3Request, S3Response,
    S3Result, Sqlite, SqliteError, Type, owner, s3_error_from_sqlite,
};

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
        grants: Some(vec![Grant {
            grantee: Some(Grantee {
                display_name: owner.display_name.clone(),
                email_address: None,
                id: owner.id.clone(),
                type_: Type::from_static(Type::CANONICAL_USER),
                uri: None,
            }),
            permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
        }]),
        owner: Some(owner),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}
