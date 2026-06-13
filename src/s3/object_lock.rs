//! S3 Object Lock metadata, DTO conversion, and enforcement.

use time::{Duration, OffsetDateTime};

use crate::sqlite::{
    BucketVersioningState, KeyMetadata, StoredObjectLock, StoredObjectLockConfig,
    StoredObjectLockDefaultRetention, StoredObjectLockMode, StoredObjectRetention,
};

use super::{DefaultRetention, s3_error, s3_error_from_sqlite, s3_error_with_message};
use super::{
    GetObjectLegalHoldInput, GetObjectLegalHoldOutput, GetObjectLockConfigurationInput,
    GetObjectLockConfigurationOutput, GetObjectRetentionInput, GetObjectRetentionOutput,
    ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold, ObjectLockLegalHoldStatus,
    ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
    PutObjectLegalHoldInput, PutObjectLegalHoldOutput, PutObjectLockConfigurationInput,
    PutObjectLockConfigurationOutput, PutObjectRetentionInput, PutObjectRetentionOutput, S3Error,
    S3ErrorCode, S3Request, S3Response, S3Result, Sqlite, SqliteError, Timestamp,
    validate_expected_bucket_owner,
};

/// Parsed Object Lock headers for object writes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ObjectLockWriteHeaders {
    legal_hold: Option<bool>,
    retention: Option<StoredObjectRetention>,
}

impl ObjectLockWriteHeaders {
    /// Returns whether any Object Lock write header was supplied.
    #[must_use]
    pub(super) fn has_any(&self) -> bool {
        self.legal_hold.is_some() || self.retention.is_some()
    }
}

/// Parses an S3 write-request legal-hold status.
pub(super) fn object_lock_write_headers(
    legal_hold: Option<ObjectLockLegalHoldStatus>,
    mode: Option<ObjectLockMode>,
    retain_until: Option<Timestamp>,
) -> S3Result<ObjectLockWriteHeaders> {
    let legal_hold = legal_hold
        .map(|status| legal_hold_from_status(status.as_str()))
        .transpose()?;
    let retention = retention_from_write_headers(mode, retain_until)?;
    Ok(ObjectLockWriteHeaders {
        legal_hold,
        retention,
    })
}

/// Resolves Object Lock metadata for a newly written object version.
pub(super) fn object_lock_for_new_version(
    config: &StoredObjectLockConfig,
    headers: ObjectLockWriteHeaders,
    now: OffsetDateTime,
) -> S3Result<StoredObjectLock> {
    if headers.has_any() && !config.enabled {
        return Err(object_lock_not_enabled());
    }

    Ok(StoredObjectLock {
        legal_hold: headers.legal_hold,
        retention: match headers.retention {
            Some(retention) => Some(validate_new_retention(retention, now)?),
            None => default_retention(config, now)?,
        },
    })
}

/// Resolves Object Lock metadata for a copied object version.
pub(super) fn object_lock_for_copied_version(
    source: &StoredObjectLock,
    config: &StoredObjectLockConfig,
    headers: ObjectLockWriteHeaders,
    now: OffsetDateTime,
) -> S3Result<StoredObjectLock> {
    if headers.has_any() {
        object_lock_for_new_version(config, headers, now)
    } else if source.legal_hold.is_some() || source.retention.is_some() {
        Ok(source.clone())
    } else {
        object_lock_for_new_version(config, headers, now)
    }
}

/// Returns the S3 legal-hold response header value for stored metadata.
#[must_use]
pub(super) fn object_lock_legal_hold_status(
    lock: &StoredObjectLock,
) -> Option<ObjectLockLegalHoldStatus> {
    lock.legal_hold.map(|enabled| {
        ObjectLockLegalHoldStatus::from_static(if enabled {
            ObjectLockLegalHoldStatus::ON
        } else {
            ObjectLockLegalHoldStatus::OFF
        })
    })
}

/// Returns the S3 retention-mode response header value for stored metadata.
#[must_use]
pub(super) fn object_lock_mode(lock: &StoredObjectLock) -> Option<ObjectLockMode> {
    lock.retention.as_ref().map(|retention| {
        ObjectLockMode::from_static(match retention.mode {
            StoredObjectLockMode::Governance => ObjectLockMode::GOVERNANCE,
            StoredObjectLockMode::Compliance => ObjectLockMode::COMPLIANCE,
        })
    })
}

/// Returns the S3 retain-until response header value for stored metadata.
#[must_use]
pub(super) fn object_lock_retain_until_date(lock: &StoredObjectLock) -> Option<Timestamp> {
    lock.retention
        .as_ref()
        .map(|retention| retention.retain_until.into())
}

/// Enforces Object Lock permanent-delete restrictions.
pub(super) fn validate_object_lock_delete(
    metadata: &KeyMetadata,
    bypass_governance: bool,
    now: OffsetDateTime,
) -> S3Result<()> {
    if metadata
        .object_lock
        .legal_hold
        .is_some_and(|legal_hold| legal_hold)
    {
        return Err(s3_error_with_message(
            S3ErrorCode::AccessDenied,
            "object is protected by a legal hold",
        ));
    }

    let Some(retention) = metadata.object_lock.retention.as_ref() else {
        return Ok(());
    };
    if retention.retain_until <= now {
        return Ok(());
    }
    match retention.mode {
        StoredObjectLockMode::Compliance => Err(s3_error_with_message(
            S3ErrorCode::AccessDenied,
            "object is protected by compliance retention",
        )),
        StoredObjectLockMode::Governance if bypass_governance => Ok(()),
        StoredObjectLockMode::Governance => Err(s3_error_with_message(
            S3ErrorCode::AccessDenied,
            "object is protected by governance retention",
        )),
    }
}

/// Returns whether a delete request will permanently remove data rather than create only a marker.
#[must_use]
pub(super) fn delete_requires_object_lock_check(
    state: BucketVersioningState,
    version_id: Option<&str>,
) -> bool {
    version_id.is_some() || state != BucketVersioningState::Enabled
}

/// Loads a bucket's stored Object Lock configuration.
pub(super) async fn load_bucket_object_lock_config(
    sqlite: &Sqlite,
    bucket: &str,
) -> S3Result<StoredObjectLockConfig> {
    let connection = sqlite
        .get_connection(bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(Sqlite::get_bucket_object_lock_config)
        .await
        .map_err(s3_error_from_sqlite)
}

/// Returns a bucket's Object Lock configuration.
pub(super) async fn get_object_lock_configuration(
    sqlite: &Sqlite,
    req: S3Request<GetObjectLockConfigurationInput>,
) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
    let GetObjectLockConfigurationInput {
        bucket,
        expected_bucket_owner,
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    let config = load_bucket_object_lock_config(sqlite, &bucket).await?;
    if !config.enabled {
        return Err(s3_error(S3ErrorCode::ObjectLockConfigurationNotFoundError));
    }

    Ok(S3Response::new(GetObjectLockConfigurationOutput {
        object_lock_configuration: Some(config_to_dto(&config)),
    }))
}

/// Stores a bucket's Object Lock default retention configuration.
pub(super) async fn put_object_lock_configuration(
    sqlite: &Sqlite,
    req: S3Request<PutObjectLockConfigurationInput>,
) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
    let PutObjectLockConfigurationInput {
        bucket,
        expected_bucket_owner,
        object_lock_configuration,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let config = config_from_dto(object_lock_configuration)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            let current = Sqlite::get_bucket_object_lock_config(transaction)?;
            if !current.enabled
                && Sqlite::get_bucket_versioning_state(transaction)?
                    != BucketVersioningState::Enabled
            {
                return Ok(Err(s3_error_with_message(
                    S3ErrorCode::InvalidBucketState,
                    "Object Lock can only be configured after versioning is enabled",
                )));
            }
            Sqlite::put_bucket_object_lock_config(transaction, &config)?;
            Ok(Ok(()))
        })
        .await
        .map_err(s3_error_from_sqlite)??;

    Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
}

/// Returns an object's legal hold status.
pub(super) async fn get_object_legal_hold(
    sqlite: &Sqlite,
    req: S3Request<GetObjectLegalHoldInput>,
) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
    let GetObjectLegalHoldInput {
        bucket,
        expected_bucket_owner,
        key,
        version_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    let metadata = load_object_lock_metadata(sqlite, &bucket, &key, version_id.as_deref()).await?;
    Ok(S3Response::new(GetObjectLegalHoldOutput {
        legal_hold: Some(legal_hold_to_dto(&metadata.object_lock)),
    }))
}

/// Replaces an object's legal hold status.
pub(super) async fn put_object_legal_hold(
    sqlite: &Sqlite,
    req: S3Request<PutObjectLegalHoldInput>,
) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
    let PutObjectLegalHoldInput {
        bucket,
        expected_bucket_owner,
        key,
        legal_hold,
        version_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let legal_hold = legal_hold_from_dto(legal_hold)?;
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            if !Sqlite::get_bucket_object_lock_config(transaction)?.enabled {
                return Ok(Err(object_lock_not_enabled()));
            }
            let target = Sqlite::get_object_lock_target(transaction, &key, version_id.as_deref())?
                .ok_or_else(|| {
                    if version_id.is_some() {
                        SqliteError::ObjectVersionNotFound
                    } else {
                        SqliteError::ObjectNotFound
                    }
                })?;
            Sqlite::put_object_legal_hold(transaction, target.sequence, legal_hold)?;
            Ok(Ok(()))
        })
        .await
        .map_err(s3_error_from_sqlite)??;

    Ok(S3Response::new(PutObjectLegalHoldOutput::default()))
}

/// Returns an object's retention metadata.
pub(super) async fn get_object_retention(
    sqlite: &Sqlite,
    req: S3Request<GetObjectRetentionInput>,
) -> S3Result<S3Response<GetObjectRetentionOutput>> {
    let GetObjectRetentionInput {
        bucket,
        expected_bucket_owner,
        key,
        version_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    let metadata = load_object_lock_metadata(sqlite, &bucket, &key, version_id.as_deref()).await?;
    Ok(S3Response::new(GetObjectRetentionOutput {
        retention: metadata
            .object_lock
            .retention
            .as_ref()
            .map(retention_to_dto),
    }))
}

/// Replaces an object's retention metadata.
pub(super) async fn put_object_retention(
    sqlite: &Sqlite,
    req: S3Request<PutObjectRetentionInput>,
) -> S3Result<S3Response<PutObjectRetentionOutput>> {
    let PutObjectRetentionInput {
        bucket,
        bypass_governance_retention,
        expected_bucket_owner,
        key,
        retention,
        version_id,
        ..
    } = req.input;

    validate_expected_bucket_owner(expected_bucket_owner.as_deref())?;
    sqlite
        .validate_mutable_bucket(&bucket)
        .map_err(s3_error_from_sqlite)?;
    let retention = optional_retention_from_dto(retention)?;
    let bypass_governance = bypass_governance_retention.unwrap_or(false);
    let now = OffsetDateTime::now_utc();
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .write(move |transaction| {
            if !Sqlite::get_bucket_object_lock_config(transaction)?.enabled {
                return Ok(Err(object_lock_not_enabled()));
            }
            let metadata = match version_id.as_deref() {
                Some(version_id) => Sqlite::get_version_metadata(transaction, &key, version_id)?
                    .ok_or(SqliteError::ObjectVersionNotFound)?,
                None => {
                    Sqlite::get_metadata(transaction, &key)?.ok_or(SqliteError::ObjectNotFound)?
                }
            };
            if metadata.is_delete_marker {
                return Err(SqliteError::ObjectNotFound);
            }
            if let Err(err) = validate_retention_update(
                metadata.object_lock.retention.as_ref(),
                retention.as_ref(),
                bypass_governance,
                now,
            ) {
                return Ok(Err(err));
            }
            let target = Sqlite::get_object_lock_target(transaction, &key, version_id.as_deref())?
                .ok_or(SqliteError::ObjectNotFound)?;
            Sqlite::put_object_retention(transaction, target.sequence, retention.as_ref())?;
            Ok(Ok(()))
        })
        .await
        .map_err(s3_error_from_sqlite)??;

    Ok(S3Response::new(PutObjectRetentionOutput::default()))
}

fn require_bucket_object_lock_enabled(transaction: &rusqlite::Transaction<'_>) -> SqliteResult<()> {
    if Sqlite::get_bucket_object_lock_config(transaction)?.enabled {
        Ok(())
    } else {
        Err(SqliteError::Configuration(
            "Object Lock is not enabled for this bucket".to_string(),
        ))
    }
}

async fn load_object_lock_metadata(
    sqlite: &Sqlite,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<KeyMetadata> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    let version_id = version_id.map(str::to_string);
    let connection = sqlite
        .get_connection(&bucket)
        .await
        .map_err(s3_error_from_sqlite)?;
    connection
        .read(move |transaction| {
            require_bucket_object_lock_enabled(transaction)?;
            let metadata = match version_id.as_deref() {
                Some(version_id) => Sqlite::get_version_metadata(transaction, &key, version_id)?
                    .ok_or(SqliteError::ObjectVersionNotFound)?,
                None => {
                    Sqlite::get_metadata(transaction, &key)?.ok_or(SqliteError::ObjectNotFound)?
                }
            };
            if metadata.is_delete_marker {
                return Err(SqliteError::ObjectNotFound);
            }
            Ok(metadata)
        })
        .await
        .map_err(|err| {
            if matches!(err, SqliteError::Configuration(_)) {
                object_lock_not_enabled()
            } else {
                s3_error_from_sqlite(err)
            }
        })
}

fn object_lock_not_enabled() -> S3Error {
    s3_error_with_message(
        S3ErrorCode::InvalidRequest,
        "Object Lock is not enabled for this bucket",
    )
}

fn legal_hold_from_status(status: &str) -> S3Result<bool> {
    match status {
        ObjectLockLegalHoldStatus::ON => Ok(true),
        ObjectLockLegalHoldStatus::OFF => Ok(false),
        _ => Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "legal hold status must be ON or OFF",
        )),
    }
}

fn mode_from_object_lock_mode(mode: &ObjectLockMode) -> S3Result<StoredObjectLockMode> {
    match mode.as_str() {
        ObjectLockMode::GOVERNANCE => Ok(StoredObjectLockMode::Governance),
        ObjectLockMode::COMPLIANCE => Ok(StoredObjectLockMode::Compliance),
        _ => Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "object lock mode must be GOVERNANCE or COMPLIANCE",
        )),
    }
}

fn mode_from_retention_mode(mode: &ObjectLockRetentionMode) -> S3Result<StoredObjectLockMode> {
    match mode.as_str() {
        ObjectLockRetentionMode::GOVERNANCE => Ok(StoredObjectLockMode::Governance),
        ObjectLockRetentionMode::COMPLIANCE => Ok(StoredObjectLockMode::Compliance),
        _ => Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "object lock mode must be GOVERNANCE or COMPLIANCE",
        )),
    }
}

fn retention_from_write_headers(
    mode: Option<ObjectLockMode>,
    retain_until: Option<Timestamp>,
) -> S3Result<Option<StoredObjectRetention>> {
    match (mode, retain_until) {
        (None, None) => Ok(None),
        (Some(mode), Some(retain_until)) => Ok(Some(StoredObjectRetention {
            mode: mode_from_object_lock_mode(&mode)?,
            retain_until: retain_until.into(),
        })),
        _ => Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "object lock mode and retain-until date must be supplied together",
        )),
    }
}

fn validate_new_retention(
    retention: StoredObjectRetention,
    now: OffsetDateTime,
) -> S3Result<StoredObjectRetention> {
    if retention.retain_until <= now {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "retain-until date must be in the future",
        ));
    }
    Ok(retention)
}

fn default_retention(
    config: &StoredObjectLockConfig,
    now: OffsetDateTime,
) -> S3Result<Option<StoredObjectRetention>> {
    let Some(default) = config.default_retention.as_ref() else {
        return Ok(None);
    };
    let days = match (default.days, default.years) {
        (Some(days), None) => i64::from(days),
        (None, Some(years)) => i64::from(years)
            .checked_mul(365)
            .ok_or_else(|| s3_error(S3ErrorCode::InvalidArgument))?,
        _ => return Err(s3_error(S3ErrorCode::InvalidArgument)),
    };
    let retain_until = now
        .checked_add(Duration::days(days))
        .ok_or_else(|| s3_error(S3ErrorCode::InvalidArgument))?;
    Ok(Some(StoredObjectRetention {
        mode: default.mode,
        retain_until,
    }))
}

fn config_from_dto(
    configuration: Option<ObjectLockConfiguration>,
) -> S3Result<StoredObjectLockConfig> {
    let Some(configuration) = configuration else {
        return Err(s3_error(S3ErrorCode::MalformedXML));
    };
    if let Some(enabled) = configuration.object_lock_enabled.as_ref()
        && enabled.as_str() != ObjectLockEnabled::ENABLED
    {
        return Err(s3_error_with_message(
            S3ErrorCode::MalformedXML,
            "ObjectLockEnabled must be Enabled",
        ));
    }
    let default_retention = configuration
        .rule
        .map(|rule| {
            rule.default_retention.ok_or_else(|| {
                s3_error_with_message(
                    S3ErrorCode::InvalidRequest,
                    "Object Lock rule requires DefaultRetention",
                )
            })
        })
        .transpose()?
        .map(default_retention_from_dto)
        .transpose()?;
    Ok(StoredObjectLockConfig {
        enabled: true,
        default_retention,
    })
}

fn default_retention_from_dto(
    retention: DefaultRetention,
) -> S3Result<StoredObjectLockDefaultRetention> {
    let mode = retention.mode.ok_or_else(|| {
        s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "DefaultRetention requires a mode",
        )
    })?;
    let mode = mode_from_retention_mode(&mode)?;
    match (retention.days, retention.years) {
        (Some(days), None) if days > 0 => Ok(StoredObjectLockDefaultRetention {
            mode,
            days: Some(days),
            years: None,
        }),
        (None, Some(years)) if years > 0 => Ok(StoredObjectLockDefaultRetention {
            mode,
            days: None,
            years: Some(years),
        }),
        (Some(_), Some(_)) | (None, None) => Err(s3_error(S3ErrorCode::MalformedXML)),
        (Some(_), None) | (None, Some(_)) => Err(invalid_retention_period()),
    }
}

fn invalid_retention_period() -> S3Error {
    // s3s 0.13.0 does not expose InvalidRetentionPeriod as an S3ErrorCode.
    s3_error_with_message(
        S3ErrorCode::InvalidRequest,
        "InvalidRetentionPeriod: retention period must be positive",
    )
}

fn config_to_dto(config: &StoredObjectLockConfig) -> ObjectLockConfiguration {
    ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
        rule: config
            .default_retention
            .as_ref()
            .map(|default| ObjectLockRule {
                default_retention: Some(default_retention_to_dto(default)),
            }),
    }
}

fn default_retention_to_dto(default: &StoredObjectLockDefaultRetention) -> DefaultRetention {
    DefaultRetention {
        days: default.days,
        mode: Some(retention_mode_to_dto(default.mode)),
        years: default.years,
    }
}

fn legal_hold_from_dto(legal_hold: Option<ObjectLockLegalHold>) -> S3Result<bool> {
    let legal_hold = legal_hold.ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
    let status = legal_hold
        .status
        .ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
    legal_hold_from_status(status.as_str())
}

fn legal_hold_to_dto(lock: &StoredObjectLock) -> ObjectLockLegalHold {
    ObjectLockLegalHold {
        status: Some(ObjectLockLegalHoldStatus::from_static(
            if lock.legal_hold.unwrap_or(false) {
                ObjectLockLegalHoldStatus::ON
            } else {
                ObjectLockLegalHoldStatus::OFF
            },
        )),
    }
}

fn optional_retention_from_dto(
    retention: Option<ObjectLockRetention>,
) -> S3Result<Option<StoredObjectRetention>> {
    let Some(retention) = retention else {
        return Ok(None);
    };
    let mode = retention
        .mode
        .ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
    let retain_until = retention
        .retain_until_date
        .ok_or_else(|| s3_error(S3ErrorCode::MalformedXML))?;
    let retention = StoredObjectRetention {
        mode: mode_from_retention_mode(&mode)?,
        retain_until: retain_until.into(),
    };
    validate_new_retention(retention, OffsetDateTime::now_utc()).map(Some)
}

fn retention_to_dto(retention: &StoredObjectRetention) -> ObjectLockRetention {
    ObjectLockRetention {
        mode: Some(retention_mode_to_dto(retention.mode)),
        retain_until_date: Some(retention.retain_until.into()),
    }
}

fn retention_mode_to_dto(mode: StoredObjectLockMode) -> ObjectLockRetentionMode {
    ObjectLockRetentionMode::from_static(match mode {
        StoredObjectLockMode::Governance => ObjectLockRetentionMode::GOVERNANCE,
        StoredObjectLockMode::Compliance => ObjectLockRetentionMode::COMPLIANCE,
    })
}

fn validate_retention_update(
    current: Option<&StoredObjectRetention>,
    requested: Option<&StoredObjectRetention>,
    bypass_governance: bool,
    now: OffsetDateTime,
) -> S3Result<()> {
    let Some(current) = current else {
        return Ok(());
    };
    if current.retain_until <= now {
        return Ok(());
    }
    let shortening_or_removing = requested.is_none_or(|requested| {
        requested.mode != current.mode || requested.retain_until < current.retain_until
    });
    if !shortening_or_removing {
        return Ok(());
    }
    match current.mode {
        StoredObjectLockMode::Compliance => Err(s3_error_with_message(
            S3ErrorCode::AccessDenied,
            "active compliance retention cannot be shortened or removed",
        )),
        StoredObjectLockMode::Governance if bypass_governance => Ok(()),
        StoredObjectLockMode::Governance => Err(s3_error_with_message(
            S3ErrorCode::AccessDenied,
            "active governance retention requires bypass to shorten or remove",
        )),
    }
}

type SqliteResult<T> = Result<T, SqliteError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_with_lock(object_lock: StoredObjectLock) -> KeyMetadata {
        KeyMetadata {
            version_id: "version".to_string(),
            size: 0,
            metadata: None,
            content_type: None,
            content_encoding: None,
            cache_control: None,
            expires: None,
            last_modified: OffsetDateTime::UNIX_EPOCH,
            md5: None,
            checksum: None,
            stored_sse: None,
            object_lock,
            is_latest: true,
            is_delete_marker: false,
        }
    }

    #[test]
    fn default_retention_uses_days_or_years() -> S3Result<()> {
        let now = OffsetDateTime::UNIX_EPOCH;
        let config = StoredObjectLockConfig {
            enabled: true,
            default_retention: Some(StoredObjectLockDefaultRetention {
                mode: StoredObjectLockMode::Governance,
                days: Some(2),
                years: None,
            }),
        };
        let lock = object_lock_for_new_version(&config, ObjectLockWriteHeaders::default(), now)?;
        assert_eq!(
            lock.retention.map(|retention| retention.retain_until),
            Some(now + Duration::days(2))
        );

        let config = StoredObjectLockConfig {
            enabled: true,
            default_retention: Some(StoredObjectLockDefaultRetention {
                mode: StoredObjectLockMode::Compliance,
                days: None,
                years: Some(1),
            }),
        };
        let lock = object_lock_for_new_version(&config, ObjectLockWriteHeaders::default(), now)?;
        assert_eq!(
            lock.retention.map(|retention| retention.retain_until),
            Some(now + Duration::days(365))
        );
        Ok(())
    }

    #[test]
    fn explicit_headers_require_enabled_bucket() {
        let headers = ObjectLockWriteHeaders {
            legal_hold: Some(true),
            retention: None,
        };
        let Err(err) = object_lock_for_new_version(
            &StoredObjectLockConfig::default(),
            headers,
            OffsetDateTime::UNIX_EPOCH,
        ) else {
            panic!("disabled bucket should reject explicit lock headers");
        };
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn legal_hold_blocks_deletes() {
        let metadata = metadata_with_lock(StoredObjectLock {
            legal_hold: Some(true),
            retention: None,
        });
        let Err(err) = validate_object_lock_delete(&metadata, false, OffsetDateTime::UNIX_EPOCH)
        else {
            panic!("legal hold should block delete");
        };
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn governance_retention_honors_bypass() {
        let metadata = metadata_with_lock(StoredObjectLock {
            legal_hold: None,
            retention: Some(StoredObjectRetention {
                mode: StoredObjectLockMode::Governance,
                retain_until: OffsetDateTime::UNIX_EPOCH + Duration::days(1),
            }),
        });
        assert!(validate_object_lock_delete(&metadata, false, OffsetDateTime::UNIX_EPOCH).is_err());
        assert!(validate_object_lock_delete(&metadata, true, OffsetDateTime::UNIX_EPOCH).is_ok());
    }

    #[test]
    fn copied_versions_preserve_source_lock_or_apply_explicit_headers() -> S3Result<()> {
        let now = OffsetDateTime::UNIX_EPOCH;
        let config = StoredObjectLockConfig {
            enabled: true,
            default_retention: None,
        };
        let source = StoredObjectLock {
            legal_hold: Some(false),
            retention: Some(StoredObjectRetention {
                mode: StoredObjectLockMode::Compliance,
                retain_until: now + Duration::days(3),
            }),
        };

        assert_eq!(
            object_lock_for_copied_version(
                &source,
                &config,
                ObjectLockWriteHeaders::default(),
                now
            )?,
            source
        );

        let copied = object_lock_for_copied_version(
            &StoredObjectLock::default(),
            &config,
            ObjectLockWriteHeaders {
                legal_hold: Some(true),
                retention: None,
            },
            now,
        )?;
        assert_eq!(copied.legal_hold, Some(true));
        Ok(())
    }

    #[test]
    fn object_lock_response_helpers_cover_legal_hold_and_retention_modes() {
        let lock = StoredObjectLock {
            legal_hold: Some(false),
            retention: Some(StoredObjectRetention {
                mode: StoredObjectLockMode::Compliance,
                retain_until: OffsetDateTime::UNIX_EPOCH + Duration::days(1),
            }),
        };

        assert_eq!(
            object_lock_legal_hold_status(&lock)
                .as_ref()
                .map(ObjectLockLegalHoldStatus::as_str),
            Some(ObjectLockLegalHoldStatus::OFF)
        );
        assert_eq!(
            object_lock_mode(&lock).as_ref().map(ObjectLockMode::as_str),
            Some(ObjectLockMode::COMPLIANCE)
        );
        assert_eq!(
            object_lock_retain_until_date(&lock).map(OffsetDateTime::from),
            Some(OffsetDateTime::UNIX_EPOCH + Duration::days(1))
        );
        assert_eq!(
            legal_hold_to_dto(&lock)
                .status
                .as_ref()
                .map(ObjectLockLegalHoldStatus::as_str),
            Some(ObjectLockLegalHoldStatus::OFF)
        );
        assert_eq!(
            lock.retention
                .as_ref()
                .map(retention_to_dto)
                .and_then(|retention| retention.mode)
                .as_ref()
                .map(ObjectLockRetentionMode::as_str),
            Some(ObjectLockRetentionMode::COMPLIANCE)
        );
    }

    #[test]
    fn object_lock_write_and_config_validation_reject_invalid_shapes() -> S3Result<()> {
        assert!(legal_hold_from_status("BAD").is_err());
        assert!(mode_from_object_lock_mode(&ObjectLockMode::from_static("BAD")).is_err());
        assert!(mode_from_retention_mode(&ObjectLockRetentionMode::from_static("BAD")).is_err());
        assert!(
            retention_from_write_headers(
                Some(ObjectLockMode::from_static(ObjectLockMode::GOVERNANCE)),
                None
            )
            .is_err()
        );
        assert!(
            validate_new_retention(
                StoredObjectRetention {
                    mode: StoredObjectLockMode::Governance,
                    retain_until: OffsetDateTime::UNIX_EPOCH,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .is_err()
        );

        assert!(config_from_dto(None).is_err());
        assert!(
            config_from_dto(Some(ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static("Disabled")),
                rule: None,
            }))
            .is_err()
        );
        assert!(
            config_from_dto(Some(ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(
                    ObjectLockEnabled::ENABLED
                )),
                rule: Some(ObjectLockRule {
                    default_retention: None,
                }),
            }))
            .is_err()
        );

        assert!(default_retention_from_dto(DefaultRetention::default()).is_err());
        assert!(
            default_retention_from_dto(DefaultRetention {
                mode: Some(ObjectLockRetentionMode::from_static(
                    ObjectLockRetentionMode::GOVERNANCE
                )),
                days: Some(1),
                years: Some(1),
            })
            .is_err()
        );
        assert!(
            default_retention_from_dto(DefaultRetention {
                mode: Some(ObjectLockRetentionMode::from_static(
                    ObjectLockRetentionMode::GOVERNANCE
                )),
                days: Some(0),
                years: None,
            })
            .is_err()
        );

        assert!(legal_hold_from_dto(None).is_err());
        assert!(legal_hold_from_dto(Some(ObjectLockLegalHold::default())).is_err());
        assert!(optional_retention_from_dto(None)?.is_none());
        assert!(optional_retention_from_dto(Some(ObjectLockRetention::default())).is_err());
        assert!(
            optional_retention_from_dto(Some(ObjectLockRetention {
                mode: Some(ObjectLockRetentionMode::from_static(
                    ObjectLockRetentionMode::GOVERNANCE
                )),
                retain_until_date: Some(OffsetDateTime::UNIX_EPOCH.into()),
            }))
            .is_err()
        );

        Ok(())
    }

    #[test]
    fn retention_delete_and_update_validation_covers_active_and_expired_modes() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let expired = metadata_with_lock(StoredObjectLock {
            legal_hold: None,
            retention: Some(StoredObjectRetention {
                mode: StoredObjectLockMode::Compliance,
                retain_until: now,
            }),
        });
        assert!(validate_object_lock_delete(&expired, false, now).is_ok());
        assert!(
            validate_object_lock_delete(
                &metadata_with_lock(StoredObjectLock {
                    legal_hold: None,
                    retention: Some(StoredObjectRetention {
                        mode: StoredObjectLockMode::Compliance,
                        retain_until: now + Duration::days(1),
                    }),
                }),
                false,
                now,
            )
            .is_err()
        );

        let current = StoredObjectRetention {
            mode: StoredObjectLockMode::Governance,
            retain_until: now + Duration::days(2),
        };
        let extended = StoredObjectRetention {
            mode: StoredObjectLockMode::Governance,
            retain_until: now + Duration::days(3),
        };
        let shortened = StoredObjectRetention {
            mode: StoredObjectLockMode::Governance,
            retain_until: now + Duration::days(1),
        };
        assert!(validate_retention_update(None, None, false, now).is_ok());
        assert!(validate_retention_update(Some(&current), Some(&extended), false, now).is_ok());
        assert!(validate_retention_update(Some(&current), Some(&shortened), false, now).is_err());
        assert!(validate_retention_update(Some(&current), Some(&shortened), true, now).is_ok());

        let compliance = StoredObjectRetention {
            mode: StoredObjectLockMode::Compliance,
            retain_until: now + Duration::days(2),
        };
        assert!(validate_retention_update(Some(&compliance), None, true, now).is_err());
    }
}
