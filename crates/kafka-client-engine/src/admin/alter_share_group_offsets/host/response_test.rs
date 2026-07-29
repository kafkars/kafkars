//! Exhaustive host translation of API-91 protocol and driver failures.

use kafka_client_core::{AlterShareGroupOffsetsInput, DeliveryStatus};

use crate::protocol::admin::alter_share_group_offsets::AlterShareGroupOffsetsProtocolFailure;

use super::response::protocol_failure;

#[test]
fn version_failures_preserve_possibly_sent_compatibility() {
    for failure in [
        AlterShareGroupOffsetsProtocolFailure::MissingSelectedVersion,
        AlterShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual: 1 },
    ] {
        assert!(matches!(
            protocol_failure(failure),
            AlterShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent
            }
        ));
    }
}

#[test]
fn capacity_failures_are_distinct_from_malformed_responses() {
    assert!(matches!(
        protocol_failure(
            AlterShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
                required: usize::MAX,
                max: 1,
            }
        ),
        AlterShareGroupOffsetsInput::ResponseTooLarge
    ));
    assert!(matches!(
        protocol_failure(AlterShareGroupOffsetsProtocolFailure::ZeroTopicId),
        AlterShareGroupOffsetsInput::InvalidResponse
    ));
}
