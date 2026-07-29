//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{DeliveryStatus, DescribeMetadataQuorumInput};

use crate::protocol::admin::describe_metadata_quorum::DescribeMetadataQuorumProtocolFailure;

use super::response::protocol_failure;

#[test]
fn compatibility_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(
            DescribeMetadataQuorumProtocolFailure::UnsupportedApiVersion { actual: 3 }
        ),
        DescribeMetadataQuorumInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeMetadataQuorumProtocolFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        DescribeMetadataQuorumInput::ResponseTooLarge
    );
}

#[test]
fn malformed_fixed_partition_shapes_are_invalid_responses() {
    for failure in [
        DescribeMetadataQuorumProtocolFailure::UnexpectedTopicCount { actual: 2 },
        DescribeMetadataQuorumProtocolFailure::UnexpectedTopicName,
        DescribeMetadataQuorumProtocolFailure::UnexpectedPartitionCount { actual: 2 },
        DescribeMetadataQuorumProtocolFailure::UnexpectedPartition { actual: 1 },
        DescribeMetadataQuorumProtocolFailure::EmptyVoterSet,
        DescribeMetadataQuorumProtocolFailure::LeaderNotVoter { actual: 7 },
        DescribeMetadataQuorumProtocolFailure::ReplicaInBothRoles { actual: 7 },
        DescribeMetadataQuorumProtocolFailure::DuplicateNodeId { actual: 7 },
        DescribeMetadataQuorumProtocolFailure::ZeroListenerPort,
    ] {
        assert_eq!(
            protocol_failure(failure),
            DescribeMetadataQuorumInput::InvalidResponse
        );
    }
}
