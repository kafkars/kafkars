//! Driver and wire classification scenarios for `ListOffsets` terminals.

use kafka_client_core::PositionResolutionAttemptFailure;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError, ResponseCloseReason};
use kafka_wire_core::{DecodeError, EncodeError, TaggedFieldsError, VersionRange};

use super::list_offsets_failure::classify_request_error;

#[test]
fn driver_facts_cover_each_stable_position_failure_category() {
    let api_key = ApiKey::new(2);
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::NotSent,
            },
            PositionResolutionAttemptFailure::DeadlineElapsed,
        ),
        (
            RequestError::ResponseCapacityReached { limit: 1 },
            PositionResolutionAttemptFailure::DriverRejected,
        ),
        (
            RequestError::RouteUnavailable,
            PositionResolutionAttemptFailure::Transport,
        ),
        (
            RequestError::UnsupportedVersion {
                message: "ListOffsetsRequest",
                version: ApiVersion::new(12),
            },
            PositionResolutionAttemptFailure::Compatibility,
        ),
        (
            RequestError::ConnectionClosed(ResponseCloseReason::ProtocolFault),
            PositionResolutionAttemptFailure::InvalidResponse,
        ),
        (
            RequestError::Decode(DecodeError::LimitExceeded {
                kind: "response",
                length: 2,
                limit: 1,
                offset: 0,
            }),
            PositionResolutionAttemptFailure::ResponseTooLarge,
        ),
        (
            RequestError::ApiUnavailable { api_key },
            PositionResolutionAttemptFailure::Compatibility,
        ),
    ];

    for (failure, expected) in cases {
        assert_eq!(classify_request_error(&failure), expected);
    }
}

#[test]
fn every_known_encode_representation_failure_is_compatibility() {
    let failures = [
        EncodeError::UnsupportedVersion {
            message: "ListOffsetsRequest",
            version: version(0),
            supported: VersionRange::new(1, 11),
        },
        EncodeError::FieldNotRepresentable {
            message: "ListOffsetsRequest",
            field: "isolation_level",
            version: version(1),
        },
        EncodeError::NullNotAllowed {
            message: "ListOffsetsRequest",
            field: "topics",
            version: version(1),
        },
        EncodeError::TaggedFieldsNotRepresentable {
            message: "ListOffsetsRequest",
            version: version(11),
        },
    ];
    for failure in failures {
        assert_eq!(
            classify_encode(failure),
            PositionResolutionAttemptFailure::Compatibility
        );
    }
}

#[test]
fn every_known_encode_size_capacity_or_state_failure_is_driver_rejected() {
    let failures = [
        EncodeError::LengthOverflow {
            kind: "topic",
            length: 2,
            maximum: 1,
        },
        EncodeError::KnownTagConflict {
            message: "ListOffsetsRequest",
            tag: 1,
            version: version(11),
        },
        EncodeError::UnclaimedKnownTag { tag: 1 },
        EncodeError::KnownTagCapacityExceeded { capacity: 1 },
        EncodeError::TaggedFieldsInvalid(TaggedFieldsError::Duplicate { tag: 1 }),
        EncodeError::SizeMismatch {
            predicted: 1,
            actual: 2,
        },
        EncodeError::FrameTooLarge { bytes: usize::MAX },
        EncodeError::FrameLimitExceeded {
            actual: 2,
            limit: 1,
        },
    ];
    for failure in failures {
        assert_eq!(
            classify_encode(failure),
            PositionResolutionAttemptFailure::DriverRejected
        );
    }
}

#[test]
fn every_known_decode_category_is_explicit() {
    assert_eq!(
        classify_decode(DecodeError::UnsupportedVersion {
            message: "ListOffsetsResponse",
            version: version(0),
            supported: VersionRange::new(1, 11),
        }),
        PositionResolutionAttemptFailure::Compatibility
    );
    for failure in [
        DecodeError::LimitExceeded {
            kind: "topics",
            length: 2,
            limit: 1,
            offset: 0,
        },
        DecodeError::LengthOverflow {
            kind: "topics",
            offset: 0,
        },
    ] {
        assert_eq!(
            classify_decode(failure),
            PositionResolutionAttemptFailure::ResponseTooLarge
        );
    }
    for failure in malformed_decode_failures() {
        assert_eq!(
            classify_decode(failure),
            PositionResolutionAttemptFailure::InvalidResponse
        );
    }
}

fn malformed_decode_failures() -> [DecodeError; 10] {
    [
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
            kind: "topics",
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
    ]
}

fn classify_encode(failure: EncodeError) -> PositionResolutionAttemptFailure {
    classify_request_error(&RequestError::Encode(failure))
}

fn classify_decode(failure: DecodeError) -> PositionResolutionAttemptFailure {
    classify_request_error(&RequestError::Decode(failure))
}

const fn version(value: i16) -> ApiVersion {
    ApiVersion::new(value)
}
