//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{AdminDescribeProducersInput, DeliveryStatus};

use crate::protocol::admin::describe_producers::DescribeProducersProtocolFailure;

use super::response::protocol_failure;

#[test]
fn unsupported_versions_and_capacity_failures_remain_distinct() {
    assert_eq!(
        protocol_failure(DescribeProducersProtocolFailure::UnsupportedApiVersion { actual: 1 }),
        AdminDescribeProducersInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeProducersProtocolFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        AdminDescribeProducersInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(DescribeProducersProtocolFailure::Allocation {
            field: "active_producers",
            requested: 2,
        }),
        AdminDescribeProducersInput::ResponseTooLarge
    );
}

#[test]
fn malformed_correlated_shapes_are_invalid_responses() {
    for failure in [
        DescribeProducersProtocolFailure::UnexpectedTopicCount { actual: 2 },
        DescribeProducersProtocolFailure::UnexpectedTopic,
        DescribeProducersProtocolFailure::UnexpectedPartitionCount { actual: 2 },
        DescribeProducersProtocolFailure::UnexpectedPartition { actual: 3 },
        DescribeProducersProtocolFailure::DuplicateProducerId { actual: 7 },
    ] {
        assert_eq!(
            protocol_failure(failure),
            AdminDescribeProducersInput::InvalidResponse
        );
    }
}
