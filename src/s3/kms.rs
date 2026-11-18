//! AWS KMS data-key operations for S3 envelope encryption.

use std::collections::HashMap;

use aws_sdk_kms::{Client, error::ProvideErrorMetadata, primitives::Blob, types::DataKeySpec};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use s3s::{S3Error, S3ErrorCode, S3Result};

use super::{s3_error_with_message, sse::ActiveSse};
use crate::telemetry::kms_timer;
use crate::{Config, sqlite::StoredSse};

/// Requires the operator-configured service identity for AWS KMS operations.
pub(super) fn client(config: &Config) -> S3Result<&Client> {
    config.kms_client.as_ref().ok_or_else(|| {
        s3_error_with_message(S3ErrorCode::InvalidRequest, "SSE-KMS is not configured")
    })
}

/// Decodes the client-supplied context and binds it to the destination object.
fn encryption_context(
    bucket: &str,
    key: &str,
    encoded: Option<&str>,
) -> S3Result<HashMap<String, String>> {
    let mut context: HashMap<String, String> = if let Some(encoded) = encoded {
        let bytes = STANDARD.decode(encoded).map_err(|_| {
            s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "invalid base64 KMS encryption context",
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|_| {
            s3_error_with_message(
                S3ErrorCode::InvalidArgument,
                "KMS encryption context must be a JSON string map",
            )
        })?
    } else {
        HashMap::new()
    };
    let arn = format!("arn:aws:s3:::{bucket}/{key}");
    if context
        .insert("aws:s3:arn".into(), arn.clone())
        .is_some_and(|value| value != arn)
    {
        return Err(s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "KMS context does not match the destination object",
        ));
    }
    Ok(context)
}

/// Translates provider failures without disclosing provider response bodies.
fn provider_error(error: &impl ProvideErrorMetadata) -> S3Error {
    match error.code() {
        Some("AccessDeniedException") => {
            s3_error_with_message(S3ErrorCode::AccessDenied, "KMS access denied")
        }
        Some(
            "NotFoundException"
            | "DisabledException"
            | "KMSInvalidStateException"
            | "InvalidKeyUsageException",
        ) => s3_error_with_message(
            S3ErrorCode::InvalidArgument,
            "KMS key is unavailable for this operation",
        ),
        Some("InvalidCiphertextException" | "IncorrectKeyException") => s3_error_with_message(
            S3ErrorCode::InvalidRequest,
            "KMS could not decrypt the data key",
        ),
        _ => s3_error_with_message(S3ErrorCode::ServiceUnavailable, "KMS service unavailable"),
    }
}

/// Obtains a plaintext AES-256 data key and its durable, opaque KMS ciphertext.
pub(super) async fn generate(
    config: &Config,
    bucket: &str,
    key: &str,
    key_id: &str,
    encoded: Option<&str>,
) -> S3Result<ActiveSse> {
    let context = encryption_context(bucket, key, encoded)?;
    let call = client(config)?
        .generate_data_key()
        .key_id(key_id)
        .key_spec(DataKeySpec::Aes256)
        .set_encryption_context(Some(context.clone()))
        .send();
    let finished = kms_timer("GenerateDataKey");
    let result = call.await;
    finished(result.is_ok());
    let result = result.map_err(|err| provider_error(&err))?;
    let stored = StoredSse::Kms {
        key_id: result
            .key_id
            .filter(|id| !id.is_empty())
            .ok_or_else(invalid_response)?,
        ciphertext: result
            .ciphertext_blob
            .filter(|blob| !blob.as_ref().is_empty())
            .ok_or_else(invalid_response)?
            .into_inner(),
        context,
    };
    Ok(ActiveSse {
        stored,
        data_key: data_key(result.plaintext)?,
    })
}

/// Unwraps the exact key and context stored with the object or upload.
pub(super) async fn decrypt(
    config: &Config,
    key_id: &str,
    ciphertext: &[u8],
    context: &HashMap<String, String>,
) -> S3Result<[u8; 32]> {
    let call = client(config)?
        .decrypt()
        .key_id(key_id)
        .ciphertext_blob(Blob::new(ciphertext))
        .set_encryption_context(Some(context.clone()))
        .send();
    let finished = kms_timer("Decrypt");
    let result = call.await;
    finished(result.is_ok());
    let result = result.map_err(|err| provider_error(&err))?;
    data_key(result.plaintext)
}

/// Reports an incomplete or malformed successful provider response.
fn invalid_response() -> S3Error {
    s3_error_with_message(S3ErrorCode::InternalError, "invalid KMS data key response")
}

/// Validates that a KMS plaintext response contains exactly one AES-256 key.
fn data_key(blob: Option<Blob>) -> S3Result<[u8; 32]> {
    blob.and_then(|value| value.as_ref().try_into().ok())
        .ok_or_else(invalid_response)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks runtime disablement and rejects missing or truncated plaintext data keys.
    #[test]
    fn requires_a_configured_client_and_a_complete_data_key() {
        assert_eq!(
            client(&Config::default()).err().as_ref().map(S3Error::code),
            Some(&S3ErrorCode::InvalidRequest)
        );
        for blob in [None, Some(Blob::new(vec![0; 31]))] {
            assert_eq!(
                data_key(blob).err().as_ref().map(S3Error::code),
                Some(&S3ErrorCode::InternalError)
            );
        }
    }
}
