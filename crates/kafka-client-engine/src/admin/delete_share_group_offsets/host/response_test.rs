//! Exhaustive host translation of API-92 protocol and driver failures.

use kafka_client_core::{DeleteShareGroupOffsetsInput, DeliveryStatus};

use crate::protocol::admin::delete_share_group_offsets::DeleteShareGroupOffsetsProtocolFailure;

use super::response::protocol_failure;

#[test]
fn version_failures_preserve_possibly_sent_compatibility() {
    for failure in [
        DeleteShareGroupOffsetsProtocolFailure::MissingSelectedVersion,
        DeleteShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual: 1 },
    ] {
        assert!(matches!(
            protocol_failure(failure),
            DeleteShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent
            }
        ));
    }
}

#[test]
fn capacity_failures_are_distinct_from_malformed_responses() {
    assert!(matches!(
        protocol_failure(
            DeleteShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
                required: usize::MAX,
                max: 1,
            }
        ),
        DeleteShareGroupOffsetsInput::ResponseTooLarge
    ));
    assert!(matches!(
        protocol_failure(DeleteShareGroupOffsetsProtocolFailure::ZeroTopicId),
        DeleteShareGroupOffsetsInput::InvalidResponse
    ));
}
