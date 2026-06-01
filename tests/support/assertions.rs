use std::fmt::Debug;

use reqwest::{Response, StatusCode};

pub fn assert_error_contains(error: &impl Debug, expected: &str) {
    let message = format!("{error:?}");
    assert!(
        message.contains(expected),
        "expected error to contain {expected:?}, got {message}"
    );
}

pub fn assert_status(response: &Response, expected: StatusCode) {
    assert_eq!(
        response.status(),
        expected,
        "unexpected status for {}",
        response.url()
    );
}
