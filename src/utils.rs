use bytes::Bytes;
use futures::{Stream, StreamExt};
use s3s::StdError;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::S3ite;

pub type Result<T = (), E = crate::error::S3ite> = std::result::Result<T, E>;

pub async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> Result<u64>
where
    S: Stream<Item = Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = result.map_err(|_| S3ite::Copy)?;
        writer.write_all(&bytes).await?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await?;
    Ok(nwritten)
}

pub fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input, hex_simd::AsciiCase::Lower)
}

pub fn base64(input: impl AsRef<[u8]>) -> String {
    let base64 = base64_simd::STANDARD;
    base64.encode_to_string(input)
}

// Helper function to return a comma-separated sequence of `?`.
// - `repeat_vars(0) => panic!(...)`
// - `repeat_vars(1) => "?"`
// - `repeat_vars(2) => "?,?"`
// - `repeat_vars(3) => "?,?,?"`
// - ...
pub fn repeat_vars(count: usize) -> String {
    assert_ne!(count, 0);
    let mut s = "?,".repeat(count);
    // Remove trailing comma
    s.pop();
    s
}
