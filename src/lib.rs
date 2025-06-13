#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::pedantic, //
)]
#![allow(
    clippy::wildcard_imports,
    clippy::missing_errors_doc, // TODO: docs
    clippy::let_underscore_untyped,
)]

#[macro_use]
mod error;

mod config;
mod database;
mod s3;
mod sqlite;
mod utils;

pub use self::{config::*, error::*, sqlite::*};
