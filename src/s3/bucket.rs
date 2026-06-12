use super::{
    Bucket, BucketAclRequest, BucketVersioningState, BucketVersioningStatus, CreateBucketInput,
    CreateBucketOutput, DeleteBucketEncryptionInput, DeleteBucketEncryptionOutput,
    DeleteBucketInput, DeleteBucketOutput, GetBucketEncryptionInput, GetBucketEncryptionOutput,
    GetBucketLocationInput, GetBucketLocationOutput, GetBucketVersioningInput,
    GetBucketVersioningOutput, HeadBucketInput, HeadBucketOutput, InternalError, ListBucketsInput,
    ListBucketsOutput, MFADelete, Not, PutBucketEncryptionInput, PutBucketEncryptionOutput,
    PutBucketVersioningInput, PutBucketVersioningOutput, S3Error, S3ErrorCode, S3Request,
    S3Response, S3Result, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Sqlite, Timestamp,
    decode_continuation_token, encode_continuation_token, fs, list_limit,
    require_sse_s3_master_key, s3_error, s3_error_from_sqlite, s3_error_with_message,
    validate_bucket_acl, validate_expected_bucket_owner,
};

/// Returns whether a bucket name satisfies S3 DNS-compatible naming rules.
pub(super) fn is_valid_bucket_name(bucket: &str) -> bool {
    let bytes = bucket.as_bytes();
    if !(3..=63).contains(&bytes.len()) {
        return false;
    }
    if looks_like_ipv4_address(bucket) {
        return false;
    }
    if matches!(bytes.first(), Some(b'.' | b'-')) || matches!(bytes.last(), Some(b'.' | b'-')) {
        return false;
    }

    for pair in bytes.windows(2) {
        if matches!(pair, b".." | b".-" | b"-.") {
            return false;
        }
    }

    bytes
        .iter()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-'))
}

/// Returns whether a bucket name has an IPv4-address shape.
fn looks_like_ipv4_address(bucket: &str) -> bool {
    let mut parts = bucket.split('.');
    if parts.clone().count() != 4 {
        return false;
    }
    parts.all(|part| {
        part.parse::<u8>()
            .is_ok_and(|value| part == value.to_string())
    })
}

/// Creates a bucket database if it does not already exist.
pub(super) async fn create_bucket(
    sqlite: &Sqlite,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    let CreateBucketInput {
        acl,
        bucket,
        grant_full_control,
        grant_read,
        grant_read_acp,
        grant_write,
        grant_write_acp,
        object_lock_enabled_for_bucket,
        ..
    } = req.input;

    if is_valid_bucket_name(&bucket).not() {
        return Err(s3_error(S3ErrorCode::InvalidBucketName));
    }
    validate_bucket_acl(&BucketAclRequest {
        acl: acl.as_ref(),
        has_access_control_policy: false,
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

    let created = sqlite
        .create_bucket(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    if created && object_lock_enabled_for_bucket.unwrap_or(false) {
        let connection = sqlite
            .get_connection(&bucket)
            .await
            .map_err(s3_error_from_sqlite)?;
        connection
            .write(|transaction| {
                Sqlite::enable_bucket_object_lock(transaction)?;
                Ok(())
            })
            .await
            .map_err(s3_error_from_sqlite)?;
    }

    let output = CreateBucketOutput {
        location: Some(format!("/{bucket}")),
    };
    Ok(S3Response::new(output))
}

/// Deletes an empty bucket database and its `SQLite` sidecar files.
pub(super) async fn delete_bucket(
    sqlite: &Sqlite,
    req: S3Request<DeleteBucketInput>,
) -> S3Result<S3Response<DeleteBucketOutput>> {
    let DeleteBucketInput {
        bucket,
        expected_bucket_owner,
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .delete_bucket(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteBucketOutput {}))
}

/// Returns the default bucket location for an existing bucket.
pub(super) async fn get_bucket_location(
    sqlite: &Sqlite,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    let GetBucketLocationInput {
        bucket,
        expected_bucket_owner,
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    if sqlite.buckets.read().await.contains_key(&bucket).not() {
        return Err(s3_error(S3ErrorCode::NoSuchBucket));
    }

    let file_path = sqlite
        .get_bucket_path(&bucket)
        .map_err(s3_error_from_sqlite)?;
    if file_path.exists().not() {
        return Err(s3_error(S3ErrorCode::NoSuchBucket));
    }

    let output = GetBucketLocationOutput::default();
    Ok(S3Response::new(output))
}

/// Returns the bucket versioning state.
pub(super) async fn get_bucket_versioning(
    sqlite: &Sqlite,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>> {
    let GetBucketVersioningInput {
        bucket,
        expected_bucket_owner,
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let state = connection
        .read(Sqlite::get_bucket_versioning_state)
        .await
        .map_err(s3_error_from_sqlite)?;

    let status = match state {
        BucketVersioningState::NeverConfigured => None,
        BucketVersioningState::Enabled => Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        BucketVersioningState::Suspended => Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::SUSPENDED,
        )),
    };

    Ok(S3Response::new(GetBucketVersioningOutput {
        status,
        ..Default::default()
    }))
}

/// Deletes the bucket's default SSE-S3 configuration.
pub(super) async fn delete_bucket_encryption(
    sqlite: &Sqlite,
    req: S3Request<DeleteBucketEncryptionInput>,
) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
    let DeleteBucketEncryptionInput {
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
        .write(|transaction| {
            Sqlite::put_bucket_default_sse_s3(transaction, false)?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(DeleteBucketEncryptionOutput {}))
}

/// Returns the bucket's default SSE-S3 configuration.
pub(super) async fn get_bucket_encryption(
    sqlite: &Sqlite,
    req: S3Request<GetBucketEncryptionInput>,
) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
    let GetBucketEncryptionInput {
        bucket,
        expected_bucket_owner,
    } = req.input;
    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    let enabled = connection
        .read(Sqlite::get_bucket_default_sse_s3)
        .await
        .map_err(s3_error_from_sqlite)?;

    if enabled.not() {
        let code = "ServerSideEncryptionConfigurationNotFoundError"
            .parse()
            .unwrap_or_else(|err| match err {});
        return Err(s3_error(code));
    }

    Ok(S3Response::new(GetBucketEncryptionOutput {
        server_side_encryption_configuration: Some(sse_s3_bucket_encryption_configuration()),
    }))
}

/// Updates the bucket's default encryption configuration.
pub(super) async fn put_bucket_encryption(
    sqlite: &Sqlite,
    req: S3Request<PutBucketEncryptionInput>,
) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
    let PutBucketEncryptionInput {
        bucket,
        expected_bucket_owner,
        server_side_encryption_configuration,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    validate_sse_s3_bucket_encryption_configuration(&server_side_encryption_configuration)?;
    require_sse_s3_master_key(sqlite)?;

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(|transaction| {
            Sqlite::put_bucket_default_sse_s3(transaction, true)?;
            Ok(())
        })
        .await
        .map_err(s3_error_from_sqlite)?;

    Ok(S3Response::new(PutBucketEncryptionOutput {}))
}

/// Builds the bucket encryption DTO for the supported SSE-S3 default.
fn sse_s3_bucket_encryption_configuration() -> ServerSideEncryptionConfiguration {
    ServerSideEncryptionConfiguration {
        rules: vec![ServerSideEncryptionRule {
            apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                kms_master_key_id: None,
                sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
            }),
            bucket_key_enabled: None,
        }],
    }
}

/// Validates that a bucket encryption request enables only SSE-S3 AES256.
fn validate_sse_s3_bucket_encryption_configuration(
    configuration: &ServerSideEncryptionConfiguration,
) -> S3Result<()> {
    let [rule] = configuration.rules.as_slice() else {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "bucket encryption must contain exactly one rule",
        ));
    };

    if rule.bucket_key_enabled.unwrap_or(false) {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "S3 bucket keys are not supported",
        ));
    }

    let Some(default) = rule.apply_server_side_encryption_by_default.as_ref() else {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "bucket encryption rule must specify an SSE algorithm",
        ));
    };
    if default.kms_master_key_id.is_some()
        || default.sse_algorithm.as_str() != ServerSideEncryption::AES256
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "bucket default encryption must be AES256",
        ));
    }

    Ok(())
}

/// Updates the bucket versioning state.
pub(super) async fn put_bucket_versioning(
    sqlite: &Sqlite,
    req: S3Request<PutBucketVersioningInput>,
) -> S3Result<S3Response<PutBucketVersioningOutput>> {
    let PutBucketVersioningInput {
        bucket,
        expected_bucket_owner,
        versioning_configuration,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;

    if versioning_configuration
        .mfa_delete
        .as_ref()
        .is_some_and(|mfa_delete| mfa_delete.as_str() == MFADelete::ENABLED)
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "MFA Delete is not supported",
        ));
    }

    let state = match versioning_configuration
        .status
        .as_ref()
        .map(BucketVersioningStatus::as_str)
    {
        Some(BucketVersioningStatus::ENABLED) => BucketVersioningState::Enabled,
        Some(BucketVersioningStatus::SUSPENDED) => BucketVersioningState::Suspended,
        _ => {
            return Err(s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "versioning status must be Enabled or Suspended",
            ));
        }
    };

    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            if state == BucketVersioningState::Suspended
                && Sqlite::get_bucket_object_lock_config(transaction)?.enabled
            {
                return Ok(Err(s3_error_with_message(
                    S3ErrorCode::InvalidBucketState,
                    "Object Lock buckets cannot suspend versioning",
                )));
            }
            Sqlite::put_bucket_versioning_state(transaction, state)?;
            Ok(Ok(()))
        })
        .await
        .map_err(s3_error_from_sqlite)??;

    Ok(S3Response::new(PutBucketVersioningOutput::default()))
}

/// Checks whether a bucket exists.
pub(super) async fn head_bucket(
    sqlite: &Sqlite,
    req: S3Request<HeadBucketInput>,
) -> S3Result<S3Response<HeadBucketOutput>> {
    let HeadBucketInput {
        bucket,
        expected_bucket_owner,
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    if sqlite.buckets.read().await.contains_key(&bucket).not() {
        return Err(s3_error(S3ErrorCode::NoSuchBucket));
    }

    let file_path = sqlite
        .get_bucket_path(&bucket)
        .map_err(s3_error_from_sqlite)?;
    if file_path.exists().not() {
        return Err(s3_error(S3ErrorCode::NoSuchBucket));
    }

    let output = HeadBucketOutput {
        access_point_alias: None,
        bucket_location_name: None,
        bucket_location_type: None,
        bucket_region: None,
    };

    Ok(S3Response::new(output))
}

/// Lists all open bucket databases.
pub(super) async fn list_buckets(
    sqlite: &Sqlite,
    req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    let ListBucketsInput {
        bucket_region: _,
        continuation_token,
        max_buckets,
        prefix,
    } = req.input;

    let limit = usize::try_from(list_limit(max_buckets, 10_000, 10_000)?)
        .map_err(S3Error::internal_error)?;
    let after_bucket = continuation_token
        .as_deref()
        .map(decode_continuation_token)
        .transpose()?;
    let prefix_filter = prefix
        .as_deref()
        .and_then(|prefix| prefix.is_empty().not().then_some(prefix));
    let mut names = sqlite
        .buckets
        .read()
        .await
        .keys()
        .filter(|name| prefix_filter.is_none_or(|prefix| name.starts_with(prefix)))
        .cloned()
        .collect::<Vec<_>>();
    names.sort_unstable();

    let start = after_bucket.as_ref().map_or(0, |after_bucket| {
        names.partition_point(|name| name.as_str() <= after_bucket.as_str())
    });
    let page_names = names
        .into_iter()
        .skip(start)
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let is_truncated = page_names.len() > limit;
    let mut buckets: Vec<Bucket> = Vec::with_capacity(page_names.len().min(limit));
    let mut last_bucket_name = None;

    for name in page_names.into_iter().take(limit) {
        let file_path = sqlite
            .get_bucket_path(&name)
            .map_err(s3_error_from_sqlite)?;
        let file_meta = fs::metadata(file_path)
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
        let created_or_modified_date = Timestamp::from(
            file_meta
                .created()
                .or_else(|_| file_meta.modified())
                .map_err(S3Error::internal_error)?,
        );

        let bucket = Bucket {
            bucket_region: None,
            creation_date: Some(created_or_modified_date),
            name: Some(name.clone()),
        };
        buckets.push(bucket);
        last_bucket_name = Some(name);
    }

    let output = ListBucketsOutput {
        continuation_token: is_truncated
            .then(|| last_bucket_name.as_deref().map(encode_continuation_token))
            .flatten(),
        buckets: Some(buckets),
        owner: None,
        prefix,
    };
    Ok(S3Response::new(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_dns_bucket_names() {
        for bucket in [
            "abc",
            "a.b-c",
            "a234567890123456789012345678901234567890123456789012345678901",
        ] {
            assert!(is_valid_bucket_name(bucket), "{bucket}");
        }

        for bucket in [
            "ab",
            "foo_bar",
            "foo-",
            "foo.",
            "foo..bar",
            "foo.-bar",
            "foo-.bar",
            "192.168.5.123",
        ] {
            assert!(!is_valid_bucket_name(bucket), "{bucket}");
        }
    }

    #[test]
    fn bucket_encryption_validation_rejects_unsupported_shapes() {
        assert!(
            validate_sse_s3_bucket_encryption_configuration(&ServerSideEncryptionConfiguration {
                rules: Vec::new()
            })
            .is_err()
        );
        assert!(
            validate_sse_s3_bucket_encryption_configuration(&ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                        kms_master_key_id: None,
                        sse_algorithm: ServerSideEncryption::from_static(
                            ServerSideEncryption::AES256
                        ),
                    },),
                    bucket_key_enabled: Some(true),
                }],
            },)
            .is_err()
        );
        assert!(
            validate_sse_s3_bucket_encryption_configuration(&ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: None,
                    bucket_key_enabled: None,
                }],
            },)
            .is_err()
        );
        assert!(
            validate_sse_s3_bucket_encryption_configuration(&ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                        kms_master_key_id: Some("key".to_string()),
                        sse_algorithm: ServerSideEncryption::from_static(
                            ServerSideEncryption::AES256
                        ),
                    },),
                    bucket_key_enabled: None,
                }],
            },)
            .is_err()
        );

        let configuration = sse_s3_bucket_encryption_configuration();
        assert!(validate_sse_s3_bucket_encryption_configuration(&configuration).is_ok());
    }
}
