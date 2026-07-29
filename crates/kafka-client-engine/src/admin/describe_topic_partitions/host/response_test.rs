//! Exhaustive protocol-failure classification without generated DTO ownership.

use kafka_client_core::{DeliveryStatus, DescribeTopicPartitionsInput};

use crate::protocol::admin::describe_topic_partitions::DescribeTopicPartitionsProtocolFailure;

use super::response::protocol_failure;

#[test]
fn version_and_retained_capacity_have_distinct_terminal_categories() {
    assert_eq!(
        protocol_failure(
            DescribeTopicPartitionsProtocolFailure::UnsupportedApiVersion { actual: 1 }
        ),
        DescribeTopicPartitionsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeTopicPartitionsProtocolFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        DescribeTopicPartitionsInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(DescribeTopicPartitionsProtocolFailure::Allocation {
            field: "topics",
            requested: 1,
        }),
        DescribeTopicPartitionsInput::ResponseTooLarge
    );
}

#[test]
fn malformed_page_identity_and_scalars_are_invalid_responses() {
    for failure in [
        DescribeTopicPartitionsProtocolFailure::EmptyTopicName,
        DescribeTopicPartitionsProtocolFailure::DuplicateTopicName,
        DescribeTopicPartitionsProtocolFailure::NegativePartition { actual: -1 },
        DescribeTopicPartitionsProtocolFailure::InvalidLeaderId { actual: -2 },
        DescribeTopicPartitionsProtocolFailure::DuplicateBrokerId {
            field: "replicas",
            actual: 1,
        },
        DescribeTopicPartitionsProtocolFailure::EmptyCursorTopic,
        DescribeTopicPartitionsProtocolFailure::NegativeCursorPartition { actual: -1 },
    ] {
        assert_eq!(
            protocol_failure(failure),
            DescribeTopicPartitionsInput::InvalidResponse
        );
    }
}
