//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{AdminDescribeTransactionsInput, DeliveryStatus};

use crate::protocol::admin::describe_transactions::DescribeTransactionsProtocolFailure;

use super::response::protocol_failure;

#[test]
fn unsupported_versions_and_capacity_failures_remain_distinct() {
    assert_eq!(
        protocol_failure(DescribeTransactionsProtocolFailure::UnsupportedApiVersion { actual: 1 }),
        AdminDescribeTransactionsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeTransactionsProtocolFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        AdminDescribeTransactionsInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(DescribeTransactionsProtocolFailure::Allocation {
            field: "topics",
            requested: 2,
        }),
        AdminDescribeTransactionsInput::ResponseTooLarge
    );
}

#[test]
fn malformed_correlated_shapes_are_invalid_responses() {
    for failure in [
        DescribeTransactionsProtocolFailure::UnexpectedTransactionStateCount { actual: 2 },
        DescribeTransactionsProtocolFailure::UnexpectedTransactionalId,
        DescribeTransactionsProtocolFailure::EmptyTransactionState,
        DescribeTransactionsProtocolFailure::DuplicateTopic,
        DescribeTransactionsProtocolFailure::DuplicatePartition { actual: 7 },
    ] {
        assert_eq!(
            protocol_failure(failure),
            AdminDescribeTransactionsInput::InvalidResponse
        );
    }
}
