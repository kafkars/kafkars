//! Exhaustive host translation of API-77 protocol and driver failures.

use kafka_client_core::{DeliveryStatus, DescribeShareGroupInput};

use crate::protocol::admin::describe_share_group::DescribeShareGroupProtocolFailure;

use super::response::protocol_failure;

#[test]
fn version_failures_preserve_possibly_sent_compatibility() {
    for failure in [
        DescribeShareGroupProtocolFailure::MissingSelectedVersion,
        DescribeShareGroupProtocolFailure::UnsupportedApiVersion { actual: 1 },
    ] {
        assert!(matches!(
            protocol_failure(failure),
            DescribeShareGroupInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent
            }
        ));
    }
}

#[test]
fn capacity_failures_are_distinct_from_malformed_responses() {
    assert!(matches!(
        protocol_failure(DescribeShareGroupProtocolFailure::RetainedBytes {
            required: usize::MAX,
            limit: 1,
        },),
        DescribeShareGroupInput::ResponseTooLarge
    ));
    assert!(matches!(
        protocol_failure(DescribeShareGroupProtocolFailure::ZeroTopicId),
        DescribeShareGroupInput::InvalidResponse
    ));
}
