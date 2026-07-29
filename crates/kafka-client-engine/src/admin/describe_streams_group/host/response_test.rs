//! Exhaustive host translation of API-89 protocol and driver failures.

use kafka_client_core::{DeliveryStatus, DescribeStreamsGroupInput};

use crate::protocol::admin::describe_streams_group::DescribeStreamsGroupProtocolFailure;

use super::response::protocol_failure;

#[test]
fn version_failures_preserve_possibly_sent_compatibility() {
    for failure in [
        DescribeStreamsGroupProtocolFailure::MissingSelectedVersion,
        DescribeStreamsGroupProtocolFailure::UnsupportedApiVersion { actual: 2 },
        DescribeStreamsGroupProtocolFailure::TopologyDescriptionRequiresV1,
    ] {
        assert!(matches!(
            protocol_failure(failure),
            DescribeStreamsGroupInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent
            }
        ));
    }
}

#[test]
fn capacity_failures_are_distinct_from_malformed_responses() {
    assert!(matches!(
        protocol_failure(DescribeStreamsGroupProtocolFailure::RetainedBytes {
            required: usize::MAX,
            limit: 1,
        },),
        DescribeStreamsGroupInput::ResponseTooLarge
    ));
    assert!(matches!(
        protocol_failure(DescribeStreamsGroupProtocolFailure::DuplicateIdentity),
        DescribeStreamsGroupInput::InvalidResponse
    ));
}
