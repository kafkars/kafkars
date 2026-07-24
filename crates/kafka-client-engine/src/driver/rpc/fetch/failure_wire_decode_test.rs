//! Exhaustive known generated-response decoding categories for Fetch.

use kafka_client_core::FetchFailure;
use kafka_driver::{ApiVersion, RequestError};
use kafka_wire_core::{DecodeError, VersionRange};

use super::failure::classify_fetch_request_error;

#[test]
fn generated_decode_version_failure_is_compatibility() {
    assert_eq!(
        classify(DecodeError::UnsupportedVersion {
            message: "FetchResponse",
            version: ApiVersion::new(3),
            supported: VersionRange::new(4, 12),
        }),
        FetchFailure::Compatibility
    );
}

#[test]
fn generated_decode_length_bounds_are_response_too_large() {
    let failures = [
        DecodeError::LimitExceeded {
            kind: "records",
            length: 2,
            limit: 1,
            offset: 0,
        },
        DecodeError::LengthOverflow {
            kind: "records",
            offset: 0,
        },
    ];
    for failure in failures {
        assert_eq!(classify(failure), FetchFailure::ResponseTooLarge);
    }
}

#[test]
fn every_known_malformed_generated_decode_is_invalid_response() {
    let failures = [
        DecodeError::UnexpectedEnd {
            offset: 0,
            needed: 1,
            remaining: 0,
        },
        DecodeError::InvalidBoolean {
            offset: 0,
            value: 2,
        },
        DecodeError::NegativeLength {
            kind: "records",
            length: -2,
            offset: 0,
        },
        DecodeError::NullNotAllowed {
            kind: "topic",
            offset: 0,
        },
        DecodeError::CountExceedsFrame {
            kind: "topics",
            count: 2,
            remaining: 1,
            offset: 0,
        },
        DecodeError::InvalidUtf8 {
            offset: 0,
            valid_up_to: 0,
        },
        DecodeError::MalformedVarint { offset: 0 },
        DecodeError::TaggedFieldOrder {
            previous: 2,
            current: 1,
            offset: 0,
        },
        DecodeError::TaggedFieldSize {
            tag: 1,
            size: 2,
            consumed: 1,
            offset: 0,
        },
        DecodeError::TrailingBytes { remaining: 1 },
    ];
    for failure in failures {
        assert_eq!(classify(failure), FetchFailure::InvalidResponse);
    }
}

fn classify(failure: DecodeError) -> FetchFailure {
    classify_fetch_request_error(&RequestError::Decode(failure))
}
