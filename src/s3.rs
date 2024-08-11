use crate::error::*;
use crate::sqlite::ContinuationToken;
use crate::sqlite::KeyValue;
use crate::sqlite::Multipart;
use crate::sqlite::Sqlite;
use crate::utils::{base64, copy_bytes, hex};

use bytes::Bytes;
use futures::stream;
use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode::InternalError;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use std::ops::Not;
use time::OffsetDateTime;
use tokio::fs;
use tracing::debug;
use uuid::Uuid;

#[async_trait::async_trait]
impl S3 for Sqlite {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let CreateBucketInput { bucket, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        if self.buckets.read().await.contains_key(&bucket) {
            return Err(s3_error!(BucketAlreadyExists));
        }

        let file_path = self.get_bucket_path(&bucket)?;
        if file_path.exists() {
            return Err(s3_error!(BucketAlreadyExists));
        }

        self.try_create_bucket(&bucket, file_path)
            .await
            .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

        println!("created bucket");

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let CopyObjectInput {
            bucket: tgt_bucket,
            key: tgt_key,
            copy_source,
            ..
        } = req.input;

        self.validate_mutable_bucket(&tgt_bucket)?;

        let (src_bucket, src_key) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { bucket, key, .. } => (bucket, key),
        };

        // verify source and target buckets exist
        let connection = self.try_get_connection(&src_bucket).await?;

        let mut object = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Ok(Self::try_get_object(&transaction, &src_key)?
                    .ok_or_else(|| s3_error!(NoSuchKey))?)
            })
            .await?;

        // replace key with target key
        object.key = tgt_key.to_string();

        let copy_object_result = CopyObjectResult {
            e_tag: object.md5.clone(),
            last_modified: Some(object.last_modified.into()),
            ..Default::default()
        };

        let connection = self.try_get_connection(&tgt_bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_put_object(&transaction, object)?;
                Ok(transaction.commit()?)
            })
            .await?;

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
        let DeleteBucketInput { bucket, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let mut guard = self.buckets.write().await;
        match guard.get(&bucket) {
            Some(connection) => {
                connection.to_owned().close();
                let bucket_path = self.get_bucket_path(&bucket)?;
                fs::remove_file(&bucket_path)
                    .await
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;
                fs::remove_file(format!("{}-wal", bucket_path.to_string_lossy()))
                    .await
                    .ok();
                fs::remove_file(format!("{}-shm", bucket_path.to_string_lossy()))
                    .await
                    .ok();
                guard.remove(&bucket);
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
        let DeleteObjectInput { bucket, key, .. } = req.input;
        let connection = self.try_get_connection(&bucket).await?;

        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;

                // if is directory
                if key.ends_with('/') {
                    let rows_affected = Self::try_delete_objects_like(&transaction, &key)
                        .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                    if rows_affected > 1 {
                        return Err(s3_error!(BucketNotEmpty).into());
                    }
                } else {
                    let rows_affected = Self::try_delete_object(&transaction, &key)
                        .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                    if rows_affected != 1 {
                        return Err(s3_error!(NoSuchKey).into());
                    }
                }

                Ok(transaction.commit()?)
            })
            .await?;

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        let delete_keys = delete
            .objects
            .into_iter()
            .map(|object| object.key)
            .collect::<Vec<_>>();

        let connection = self.try_get_connection(&bucket).await?;

        let affected_keys = connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let affected_keys = Self::try_delete_objects(&transaction, &delete_keys)?;
                transaction.commit()?;
                Ok(affected_keys)
            })
            .await?;

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
        let GetBucketLocationInput { bucket, .. } = req.input;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let file_path = self.get_bucket_path(&bucket)?;
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
        let GetObjectInput {
            bucket, key, range, ..
        } = req.input;

        let connection = self.try_get_connection(&bucket).await?;

        let object =
            connection
                .read(move |connection| {
                    let transaction = connection.transaction()?;
                    Ok(Self::try_get_object(&transaction, &key)?
                        .ok_or_else(|| s3_error!(NoSuchKey))?)
                })
                .await?;

        let content_length = match range {
            None => object.size,
            Some(range) => {
                let object_range = range.check(object.size)?;
                object_range.end - object_range.start
            }
        };
        let content_length_i64 = try_!(i64::try_from(content_length));

        let value = match range {
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
            content_length: Some(content_length_i64),
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
        let HeadBucketInput { bucket, .. } = req.input;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        let file_path = self.get_bucket_path(&bucket)?;
        if file_path.exists().not() {
            return Err(s3_error!(NoSuchBucket));
        }

        Ok(S3Response::new(HeadBucketOutput {
            access_point_alias: None,
            bucket_location_name: None,
            bucket_location_type: None,
            bucket_region: None,
        }))
    }

    #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let HeadObjectInput { bucket, key, .. } = req.input;

        let connection = self.try_get_connection(&bucket).await?;

        let object = connection
            .read(move |connection| {
                let transaction = connection.transaction()?;
                Ok(Self::try_get_metadata(&transaction, &key)?
                    .ok_or_else(|| s3_error!(NoSuchKey))?)
            })
            .await?;

        // TODO: detect content type
        let content_type = mime::APPLICATION_OCTET_STREAM;

        let output = HeadObjectOutput {
            content_length: Some(try_!(i64::try_from(object.size))),
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
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let ListBucketsInput {} = req.input;

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
                .unwrap_or(false)
                .then(|| {
                    v2.contents
                        .as_ref()
                        .and_then(|contents| contents.last().and_then(|last| last.key.clone()))
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

        let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
        let max_keys_usize = try_!(usize::try_from(max_keys));
        let continuation_token_clone = continuation_token.clone();

        let (key_sizes, next_continuation_token) = match continuation_token {
            // initial request requires taking a snapshot of the state of the database
            None => {
                let prefix_clone = prefix.clone();
                let start_after_clone = start_after.clone();

                let connection = self.try_get_connection(&bucket).await?;
                let mut key_sizes = connection
                    .read(move |connection| {
                        let transaction = connection.transaction()?;
                        Ok(Self::try_list_objects(
                            &transaction,
                            &prefix_clone,
                            &start_after_clone,
                        )?)
                    })
                    .await?;

                if key_sizes.len() <= max_keys_usize {
                    (key_sizes, None)
                } else {
                    let remainder = key_sizes.split_off(max_keys_usize);

                    let mut continuation_tokens = self.continuation_tokens.lock().unwrap();
                    let next_continuation_token = Uuid::new_v4().to_string();
                    continuation_tokens.insert(
                        next_continuation_token.clone(),
                        ContinuationToken {
                            token: next_continuation_token.clone(),
                            last_modified: OffsetDateTime::now_utc(),
                            key_sizes: remainder,
                        },
                    );

                    (key_sizes, Some(next_continuation_token))
                }
            }
            // subsequent request
            Some(continuation_token) => {
                let mut continuation_tokens = self.continuation_tokens.lock().unwrap();
                let mut continuation_token = match continuation_tokens.remove(&continuation_token) {
                    Some(continuation_token) => Ok(continuation_token),
                    None => Err(s3_error!(InvalidToken)),
                }?;

                if continuation_token.key_sizes.len() <= max_keys_usize {
                    (continuation_token.key_sizes, None)
                } else {
                    let remainder = continuation_token.key_sizes.split_off(max_keys_usize);
                    let key_sizes = std::mem::replace(&mut continuation_token.key_sizes, remainder);

                    let continuation_token_clone = continuation_token.token.clone();
                    continuation_token.last_modified = OffsetDateTime::now_utc();
                    continuation_tokens
                        .insert(continuation_token_clone.clone(), continuation_token);

                    (key_sizes, Some(continuation_token_clone))
                }
            }
        };

        let objects = key_sizes
            .into_iter()
            .map(|key_size| {
                Ok(Object {
                    key: Some(key_size.key),
                    last_modified: Some(key_size.last_modified.into()),
                    size: Some(i64::try_from(key_size.size)?),
                    e_tag: Some(key_size.md5),
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let key_count = try_!(i32::try_from(objects.len()));

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(max_keys),
            continuation_token: continuation_token_clone,
            is_truncated: Some(next_continuation_token.is_some()),
            contents: Some(objects),
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix,
            start_after,
            next_continuation_token,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let PutObjectInput {
            body,
            bucket,
            key,
            metadata,
            content_length,
            content_md5,
            storage_class,
            ..
        } = req.input;

        self.validate_mutable_bucket(&bucket)?;

        if self.buckets.read().await.contains_key(&bucket).not() {
            return Err(s3_error!(NoSuchBucket));
        }

        if let Some(ref storage_class) = storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if is_valid.not() {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let connection = self.try_get_connection(&bucket).await?;

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

            connection
                .write(move |connection| {
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
                    Ok(transaction.commit()?)
                })
                .await?;

            let output = PutObjectOutput::default();
            return Ok(S3Response::new(output));
        }

        let mut md5_hash = Md5::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut value = Vec::new();
        let size = copy_bytes(stream, &mut value).await?;
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);

        // if provided verify content_md5
        if let Some(content_md5) = content_md5 {
            if content_md5 != base64(md5_bytes) {
                return Err(s3_error!(BadDigest));
            }
        }

        debug!(path = %key, ?size, %md5, "write file");

        let md5_clone = md5.clone();
        connection
            .write(move |connection| {
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
                Ok(transaction.commit()?)
            })
            .await?;

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

        self.validate_mutable_bucket(&bucket)?;

        let upload_id = Uuid::new_v4();

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;
                Self::try_create_multipart_upload(
                    &transaction,
                    upload_id,
                    &bucket_clone,
                    &key_clone,
                    req.credentials,
                )?;
                Ok(transaction.commit()?)
            })
            .await?;

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
        let md5_bytes = md5_hash.finalize();
        let md5 = hex(md5_bytes);
        let size = try_!(i64::try_from(value.len()));

        // if provided verify content_md5
        if let Some(content_md5) = content_md5 {
            if content_md5 != base64(md5_bytes) {
                return Err(s3_error!(BadDigest));
            }
        }

        let md5_clone = md5.clone();
        let connection = self.try_get_connection(&bucket).await?;
        connection
            .write(move |connection| {
                let transaction = connection.transaction()?;

                if Self::try_verify_upload_id(
                    &transaction,
                    upload_id,
                    &bucket,
                    &key,
                    req.credentials,
                )?
                .not()
                {
                    return Err(s3_error!(AccessDenied).into());
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
                )?;

                Ok(transaction.commit()?)
            })
            .await?;

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
        let connection = self.try_get_connection(&bucket).await?;
        let parts = connection
            .read(move |connection| {
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
                    return Err(s3_error!(AccessDenied).into());
                };

                let parts = Self::try_list_multipart(&transaction, upload_id)
                    .map_err(|err| S3Error::with_message(InternalError, err.to_string()))?;

                drop(transaction);

                Ok(parts)
            })
            .await?;

        let parts = parts
            .into_iter()
            .map(|part| Part {
                last_modified: Some(part.last_modified.into()),
                part_number: Some(part.part_number),
                size: Some(part.size),
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

        self.validate_mutable_bucket(&bucket)?;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = part.part_number;
            cnt += 1;
            if part_number != Some(cnt) {
                return Err(s3_error!(InvalidRequest, "invalid part order"));
            }
        }

        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let connection = self.try_get_connection(&bucket).await?;
        let md5 = connection
            .write(move |connection| {
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
                    return Err(s3_error!(AccessDenied).into());
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
            .await?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(md5),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
