#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::expect_used,
    clippy::pedantic,
    clippy::unwrap_used
)]

#[macro_use]
mod error;

mod config;
mod database;
mod s3;
mod sqlite;

pub use self::{config::*, error::*, sqlite::*};
