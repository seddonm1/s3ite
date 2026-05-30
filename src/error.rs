#[cfg(feature = "binary")]
use std::panic::Location;

use s3s::{S3Error, S3ErrorCode};
#[cfg(feature = "binary")]
use tracing::error;

pub type Result<T> = std::result::Result<T, S3ite>;

#[derive(Debug, thiserror::Error)]
pub enum S3ite {
    #[error("S3 {}", .0)]
    S3(S3Error),
    #[error("Rusqlite {}", .0)]
    Rusqlite(rusqlite::Error),
    #[error("Io {}", .0)]
    Io(std::io::Error),
    #[error("database queue is full")]
    DatabaseQueueFull,
    #[error("database queue is closed")]
    DatabaseQueueClosed,
    #[error("bucket does not exist")]
    BucketNotFound,
    #[error("database is in read-only mode")]
    BucketReadOnly,
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("database worker thread panicked")]
    ThreadJoin,
    #[error("Tokio")]
    Tokio,
    #[error("TryFromInt")]
    TryFromInt,
    #[error("Copy")]
    Copy,
    #[error("Hyper")]
    Hyper,
    #[error("Yaml")]
    Yaml,
}

impl From<S3ite> for S3Error {
    fn from(e: S3ite) -> Self {
        match e {
            S3ite::S3(s3error) => s3error,
            S3ite::BucketNotFound => S3Error::new(S3ErrorCode::NoSuchBucket),
            S3ite::BucketReadOnly => S3Error::with_message(
                S3ErrorCode::MethodNotAllowed,
                "database is in read-only mode",
            ),
            S3ite::Configuration(message) => {
                S3Error::with_message(S3ErrorCode::InternalError, message)
            }
            _ => S3Error::internal_error(e),
        }
    }
}

impl From<S3Error> for S3ite {
    fn from(value: S3Error) -> Self {
        Self::S3(value)
    }
}

impl From<std::io::Error> for S3ite {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<rusqlite::Error> for S3ite {
    fn from(value: rusqlite::Error) -> Self {
        Self::Rusqlite(value)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for S3ite {
    fn from(_value: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Tokio
    }
}

impl From<std::num::TryFromIntError> for S3ite {
    fn from(_value: std::num::TryFromIntError) -> Self {
        Self::TryFromInt
    }
}

#[cfg(feature = "hyper")]
impl From<hyper::Error> for S3ite {
    fn from(_value: hyper::Error) -> Self {
        Self::Hyper
    }
}

#[cfg(feature = "yaml_serde")]
impl From<yaml_serde::Error> for S3ite {
    fn from(_value: yaml_serde::Error) -> Self {
        Self::Yaml
    }
}

#[inline]
#[track_caller]
#[cfg(feature = "binary")]
pub(crate) fn log(source: &dyn std::error::Error) {
    let location = Location::caller();
    let span_trace = tracing_error::SpanTrace::capture();

    error!(
        target: "s3s_fs_internal_error",
        %location,
        error=%source,
        "span trace:\n{span_trace}"
    );
}

#[inline]
#[track_caller]
#[cfg(not(feature = "binary"))]
pub(crate) fn log(_source: &dyn std::error::Error) {}

macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                $crate::error::log(&err);
                return Err(::s3s::S3Error::internal_error(err).into());
            }
        }
    };
}
