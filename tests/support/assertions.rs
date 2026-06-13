use std::fmt::Debug;

use reqwest::{Response, StatusCode};

pub fn assert_error_contains(error: &impl Debug, expected: &str) {
    let message = format!("{error:?}");
    assert!(
        message.contains(expected),
        "expected error to contain {expected:?}, got {message}"
    );
}

/// Asserts that an SDK-style error carries the expected S3 error code.
pub fn assert_error_code(error: &impl Debug, expected: &str) {
    let message = format!("{error:?}");
    let expected_quoted = format!("code: Some(\"{expected}\")");
    let expected_xml = format!("<Code>{expected}</Code>");
    assert!(
        message.contains(&expected_quoted) || message.contains(&expected_xml),
        "expected error code {expected:?}, got {message}"
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
