use std::ops::Not;

use crate::error::*;
use crate::sqlite::ContinuationToken;
use crate::sqlite::KeyValue;
use crate::sqlite::Multipart;
use crate::sqlite::Sqlite;
use crate::utils::*;

use bytes::Bytes;
use futures::stream;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3ErrorCode::InternalError;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use time::OffsetDateTime;

use tokio::fs;

use futures::TryStreamExt;
use md5::{Digest, Md5};
use tracing::debug;
use uuid::Uuid;

#[async_trait::async_trait]
impl S3 for Sqlite {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;

        if self.buckets.read().await.contains_key(&input.bucket) {
            return Err(s3_error!(BucketAlreadyExists));
        }

        let file_path = self.get_bucket_path(&input.bucket)?;
        if file_path.exists() {
            return Err(s3_error!(BucketAlreadyExists));
        }

        self.try_create_bucket(&input.bucket, file_path)
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let (bucket, key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                ..
            } => (bucket, key),
        };

        // verify source and target buckets exist
        let bucket_pool = self.try_get_bucket_pool(bucket).await?;

        let mut object = bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                Self::try_get_object(&transaction, &input.key)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                    .ok_or_else(|| s3_error!(NoSuchKey))
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| match err.code() {
                S3ErrorCode::NoSuchKey => err,
                _ => s3_error!(InternalError),
            })?;

        // replace key with target key
        object.key = key.to_string();

        let copy_object_result = CopyObjectResult {
            e_tag: object.md5.clone(),
            last_modified: Some(object.last_modified.into()),
            ..Default::default()
        };

        let bucket_pool = self.try_get_bucket_pool(&input.bucket).await?;
        bucket_pool
            .interact(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_put_object(&transaction, object)?;
                transaction.commit()
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let input = req.input;

        let mut guard = self.buckets.write().await;
        match guard.get(&input.bucket) {
            Some(connection) => {
                connection.close();
                fs::remove_file(self.get_bucket_path(&input.bucket)?)
                    .await
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                guard.remove(&input.bucket);
            }
            None => return Err(s3_error!(NoSuchBucket)),
        };

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;

        let bucket_pool = self.try_get_bucket_pool(&input.bucket).await?;

        bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                // if is directory
                if input.key.ends_with('/') {
                    let rows_affected = Self::try_delete_objects_like(&transaction, &input.key)
                        .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                    if rows_affected > 1 {
                        return Err(s3_error!(BucketNotEmpty));
                    }
                } else {
                    let rows_affected = Self::try_delete_object(&transaction, &input.key)
                        .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                    if rows_affected != 1 {
                        return Err(s3_error!(NoSuchKey));
                    }
                }

                transaction
                    .commit()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))??;

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let delete_keys = input
            .delete
            .objects
            .into_iter()
            .map(|object| object.key)
            .collect::<Vec<_>>();

        let bucket_pool = self.try_get_bucket_pool(&input.bucket).await?;

        let affected_keys = bucket_pool
            .interact(move |connection| {
                let transaction = connection.transaction()?;
                let affected_keys = Self::try_delete_objects(&transaction, &delete_keys)?;
                transaction.commit()?;
                rusqlite::Result::<_, rusqlite::Error>::Ok(affected_keys)
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        let output = DeleteObjectsOutput {
            deleted: Some(
                affected_keys
                    .into_iter()
                    .map(|key| DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>(),
            ),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;

        if self.buckets.read().await.contains_key(&input.bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let file_path = self.get_bucket_path(&input.bucket)?;
        if file_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;

        let bucket_pool = self.try_get_bucket_pool(&input.bucket).await?;

        let object = bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                Self::try_get_object(&transaction, &input.key)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                    .ok_or_else(|| s3_error!(NoSuchKey))
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))??;

        let content_length = match input.range {
            None => object.size,
            Some(range) => {
                let object_range = range.check(object.size)?;
                object_range.end - object_range.start
            }
        };
        let content_length_i64 = try_!(i64::try_from(content_length));

        let value = match input.range {
            Some(Range::Int { first, .. }) => {
                let first = try_!(usize::try_from(first));
                Bytes::copy_from_slice(&object.value.unwrap()[first..])
            }
            Some(Range::Suffix { length }) => {
                let first = try_!(usize::try_from(object.size - length));
                Bytes::copy_from_slice(&object.value.unwrap()[first..])
            }
            None => Bytes::copy_from_slice(&object.value.unwrap()),
        };

        let body = stream::once(async { Ok(value) });

        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap::<_, S3Error>(body)),
            content_length: content_length_i64,
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            e_tag: object.md5,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        if self.buckets.read().await.contains_key(&input.bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let file_path = self.get_bucket_path(&input.bucket)?;
        if file_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }

        Ok(S3Response::new(HeadBucketOutput {}))
    }

    #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;

        let bucket_pool = self.try_get_bucket_pool(&input.bucket).await?;

        let object = bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                Self::try_get_metadata(&transaction, &input.key)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                    .ok_or_else(|| s3_error!(NoSuchKey))
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| match err.code() {
                S3ErrorCode::NoSuchKey => err,
                _ => s3_error!(InternalError),
            })?;

        // TODO: detect content type
        let content_type = mime::APPLICATION_OCTET_STREAM;

        let output = HeadObjectOutput {
            content_length: try_!(i64::try_from(object.size)),
            content_type: Some(content_type),
            last_modified: Some(object.last_modified.into()),
            metadata: object.metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let mut buckets: Vec<Bucket> = Vec::new();

        for name in self.buckets.read().await.keys() {
            let file_path = self.get_bucket_path(name)?;
            let file_meta = fs::metadata(file_path)
                .await
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
            let created_or_modified_date =
                Timestamp::from(try_!(file_meta.created().or(file_meta.modified())));

            let bucket = Bucket {
                creation_date: Some(created_or_modified_date),
                name: Some(name.clone()),
            };
            buckets.push(bucket);
        }

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let marker = req.input.marker.clone();

        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| {
            let next_marker = v2
                .is_truncated
                .then(|| {
                    v2.contents.as_ref().and_then(|contents| {
                        contents.last().and_then(|last| last.key.as_ref().cloned())
                    })
                })
                .flatten();

            ListObjectsOutput {
                contents: v2.contents,
                delimiter: v2.delimiter,
                encoding_type: v2.encoding_type,
                name: v2.name,
                prefix: v2.prefix,
                max_keys: v2.max_keys,
                is_truncated: v2.is_truncated,
                marker,
                next_marker,
                ..Default::default()
            }
        }))
    }

    #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input {
            bucket,
            prefix,
            max_keys,
            start_after,
            delimiter,
            encoding_type,
            continuation_token,
            ..
        } = req.input;

        let max_keys = max_keys.unwrap_or(1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));

        let cotinuation_token_clone = continuation_token.clone();
        let continuation_token = continuation_token
            .map(|continuation_token| {
                let continuation_tokens = self
                    .continuation_tokens
                    .read()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                match continuation_tokens.get(&continuation_token) {
                    Some(continuation_token) => Ok(continuation_token.clone()),
                    None => Err(s3_error!(InvalidToken)),
                }
            })
            .transpose()?;

        let prefix_clone = prefix.clone();
        let start_after_clone = start_after.clone();
        let continuation_token_clone = continuation_token.clone();

        let bucket_pool = self.try_get_bucket_pool(&bucket).await?;
        let mut keys_values = bucket_pool
            .interact(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_list_objects(
                    &transaction,
                    prefix_clone,
                    &start_after_clone,
                    max_keys,
                    &continuation_token_clone,
                )
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        let is_truncated = keys_values.len() > max_keys_usize;
        keys_values.truncate(max_keys_usize);

        let objects = keys_values
            .into_iter()
            .map(|object| {
                Ok(Object {
                    key: Some(object.key),
                    last_modified: Some(object.last_modified.into()),
                    size: i64::try_from(object.size)?,
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let continuation_token = {
            let mut continuation_tokens = self
                .continuation_tokens
                .write()
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

            // clean up old tokens
            continuation_tokens.retain(|_, value| {
                (OffsetDateTime::now_utc() - value.last_modified).as_seconds_f32() < 300.0
            });

            let continuation_token = is_truncated.then(|| {
                let (token, mut offset) = continuation_token
                    .map_or((Uuid::new_v4().to_string(), 0), |continuation_token| {
                        (continuation_token.token, continuation_token.offset)
                    });
                offset += max_keys_usize;
                let continuation_token = ContinuationToken {
                    token: token.clone(),
                    last_modified: OffsetDateTime::now_utc(),
                    offset,
                };
                continuation_tokens.insert(token, continuation_token.clone());
                continuation_token
            });

            Result::<_, S3Error>::Ok(continuation_token)
        }?;

        let key_count = try_!(i32::try_from(objects.len()));

        let output = ListObjectsV2Output {
            key_count,
            max_keys,
            continuation_token: cotinuation_token_clone,
            is_truncated,
            contents: Some(objects),
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix,
            start_after,
            next_continuation_token: continuation_token
                .map(|continuation_token| continuation_token.token),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;

        if self.buckets.read().await.contains_key(&input.bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if is_valid.not() {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            metadata,
            content_length,
            content_md5,
            ..
        } = input;

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let bucket_pool = self.try_get_bucket_pool(&bucket).await?;

        // if is directory
        if key.ends_with('/') {
            if let Some(len) = content_length {
                if len > 0 {
                    return Err(s3_error!(
                        UnexpectedContent,
                        "Unexpected request body when creating a directory object."
                    ));
                }
            };

            bucket_pool
                .interact(move |connection| {
                    let transaction = connection.transaction()?;
                    Self::try_put_object(
                        &transaction,
                        KeyValue {
                            key,
                            value: None,
                            size: 0,
                            metadata,
                            last_modified: OffsetDateTime::now_utc(),
                            md5: None,
                        },
                    )?;
                    transaction.commit()
                })
                .await
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

            let output = PutObjectOutput::default();
            return Ok(S3Response::new(output));
        }

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        let size = copy_bytes(stream, &mut value).await?;
        let md5 = hex(md5_hash.finalize());

        // if provided verify content_md5
        if let Some(content_md5) = content_md5 {
            if content_md5 != md5 {
                return Err(s3_error!(BadDigest));
            }
        }

        debug!(path = %key, ?size, %md5, "write file");

        let md5_clone = md5.clone();
        bucket_pool
            .interact(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_put_object(
                    &transaction,
                    KeyValue {
                        key,
                        value: Some(value),
                        size,
                        metadata,
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5_clone),
                    },
                )?;
                transaction.commit()
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        let output = PutObjectOutput {
            e_tag: Some(md5),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput { bucket, key, .. } = req.input;

        let upload_id = Uuid::new_v4();

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let bucket_pool = self.try_get_bucket_pool(&bucket).await?;
        bucket_pool
            .interact(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_create_multipart_upload(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials,
                )?;
                transaction.commit()
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            bucket,
            key,
            upload_id,
            part_number,
            content_md5,
            ..
        } = req.input;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        copy_bytes(stream, &mut value).await?;
        let size = try_!(i64::try_from(value.len()));
        let md5 = hex(md5_hash.finalize());

        // if provided verify content_md5
        if let Some(content_md5) = content_md5 {
            if content_md5 != md5 {
                return Err(s3_error!(BadDigest));
            }
        }

        let md5_clone = md5.clone();
        let bucket_pool = self.try_get_bucket_pool(&bucket).await?;
        bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket,
                    &key,
                    req.credentials,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .not()
                {
                    return Err(s3_error!(AccessDenied));
                };

                Self::try_put_multipart(
                    &transaction,
                    Multipart {
                        upload_id,
                        part_number,
                        last_modified: OffsetDateTime::now_utc(),
                        value,
                        size,
                        md5: Some(md5_clone),
                    },
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                transaction
                    .commit()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| match err.code() {
                S3ErrorCode::AccessDenied => err,
                _ => s3_error!(InternalError),
            })?;

        let output = UploadPartOutput {
            e_tag: Some(md5),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let bucket_pool = self.try_get_bucket_pool(&bucket).await?;
        let parts = bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .not()
                {
                    return Err(s3_error!(AccessDenied));
                };

                let parts = Self::try_list_multipart(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                drop(transaction);

                Ok(parts)
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| match err.code() {
                S3ErrorCode::AccessDenied => err,
                _ => s3_error!(InternalError),
            })?;

        let parts = parts
            .into_iter()
            .map(|part| Part {
                last_modified: Some(part.last_modified.into()),
                part_number: part.part_number,
                size: part.size,
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id.to_string()),
            parts: Some(parts),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let Some(multipart_upload) = multipart_upload else { return Err(s3_error!(InvalidPart)) };

        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = part.part_number;
            cnt += 1;
            if part_number != cnt {
                return Err(s3_error!(InvalidRequest, "invalid part order"));
            }
        }

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let bucket_pool = self.try_get_bucket_pool(&bucket).await?;
        let md5 = bucket_pool
            .interact(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials,
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
                .not()
                {
                    return Err(s3_error!(AccessDenied));
                };

                let parts = Self::try_get_multiparts(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                let value = parts
                    .into_iter()
                    .map(|part| part.value)
                    .collect::<Vec<_>>()
                    .concat();
                let mut md5_hash = Md5::new();
                md5_hash.update(&value);
                let md5 = hex(md5_hash.finalize());
                let size = try_!(u64::try_from(value.len()));

                Self::try_put_object(
                    &transaction,
                    KeyValue {
                        key: key_clone,
                        value: Some(value),
                        size,
                        metadata: None,
                        last_modified: OffsetDateTime::now_utc(),
                        md5: Some(md5.clone()),
                    },
                )
                .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                Self::try_delete_multipart(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                transaction
                    .commit()
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                Ok(md5)
            })
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?
            .map_err(|err| match err.code() {
                S3ErrorCode::AccessDenied => err,
                _ => s3_error!(InternalError),
            })?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(md5),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
