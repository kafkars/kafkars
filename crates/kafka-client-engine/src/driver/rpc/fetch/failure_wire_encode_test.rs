//! Exhaustive known wire-encoding categories at the Fetch driver seam.

use kafka_client_core::FetchFailure;
use kafka_driver::{ApiVersion, RequestError};
use kafka_wire_core::{EncodeError, TaggedFieldsError, VersionRange};

use super::failure::classify_fetch_request_error;

#[test]
fn every_known_version_representation_failure_is_compatibility() {
    let failures = [
        EncodeError::UnsupportedVersion {
            message: "FetchRequest",
            version: version(3),
            supported: VersionRange::new(4, 12),
        },
        EncodeError::FieldNotRepresentable {
            message: "FetchRequest",
            field: "isolation_level",
            version: version(3),
        },
        EncodeError::NullNotAllowed {
            message: "FetchRequest",
            field: "topics",
            version: version(4),
        },
        EncodeError::TaggedFieldsNotRepresentable {
            message: "FetchRequest",
            version: version(12),
        },
    ];
    for failure in failures {
        assert_eq!(classify(failure), FetchFailure::Compatibility);
    }
}

#[test]
fn every_known_outbound_size_capacity_or_state_failure_is_driver_rejected() {
    let failures = [
        EncodeError::LengthOverflow {
            kind: "topic",
            length: 2,
            maximum: 1,
        },
        EncodeError::KnownTagConflict {
            message: "FetchRequest",
            tag: 1,
            version: version(12),
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
        assert_eq!(classify(failure), FetchFailure::DriverRejected);
    }
}

fn classify(failure: EncodeError) -> FetchFailure {
    classify_fetch_request_error(&RequestError::Encode(failure))
}

const fn version(value: i16) -> ApiVersion {
    ApiVersion::new(value)
}
