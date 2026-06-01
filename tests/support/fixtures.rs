use anyhow::Result;
use aws_sdk_s3::{Client, primitives::ByteStream};
use uuid::Uuid;

pub fn bucket_name(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

pub async fn create_bucket(client: &Client, bucket: &str) -> Result<()> {
    client.create_bucket().bucket(bucket).send().await?;
    Ok(())
}

pub async fn put_object(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    Ok(())
}

pub async fn get_object_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    Ok(client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes()
        .to_vec())
}
