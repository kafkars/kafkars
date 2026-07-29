//! Validate-before-allocation checks for generated API-key 65 facts.

use kafka_client_core::{
    DESCRIBE_TRANSACTIONS_MAX_PARTITIONS, DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES,
    DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES, DESCRIBE_TRANSACTIONS_MAX_TOPICS,
};
use kafka_wire::describe_transactions_response::TransactionState;

use super::DescribeTransactionsProtocolFailure;

const MAX_TOPIC_NAME_BYTES: usize = 249;

pub(super) fn validate_error_payload(
    state: &TransactionState,
) -> Result<(), DescribeTransactionsProtocolFailure> {
    for (present, field) in [
        (!state.transaction_state.is_empty(), "transaction_state"),
        (state.transaction_timeout_ms != 0, "transaction_timeout_ms"),
        (
            state.transaction_start_time_ms != 0,
            "transaction_start_time_ms",
        ),
        (state.producer_id != 0, "producer_id"),
        (state.producer_epoch != 0, "producer_epoch"),
        (!state.topics.is_empty(), "topics"),
    ] {
        if present {
            return Err(
                DescribeTransactionsProtocolFailure::SuccessPayloadWithBrokerError { field },
            );
        }
    }
    Ok(())
}

pub(super) fn validate_success_payload(
    state: &TransactionState,
) -> Result<(), DescribeTransactionsProtocolFailure> {
    if state.transaction_state.is_empty() {
        return Err(DescribeTransactionsProtocolFailure::EmptyTransactionState);
    }
    if state.transaction_state.len() > DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES {
        return Err(
            DescribeTransactionsProtocolFailure::TransactionStateTooLong {
                actual: state.transaction_state.len(),
                max: DESCRIBE_TRANSACTIONS_MAX_STATE_BYTES,
            },
        );
    }
    if state.transaction_start_time_ms < -1 {
        return Err(
            DescribeTransactionsProtocolFailure::InvalidTransactionStartTime {
                actual: state.transaction_start_time_ms,
            },
        );
    }
    if state.topics.len() > DESCRIBE_TRANSACTIONS_MAX_TOPICS {
        return Err(DescribeTransactionsProtocolFailure::TooManyTopics {
            actual: state.topics.len(),
            max: DESCRIBE_TRANSACTIONS_MAX_TOPICS,
        });
    }
    let mut partition_count = 0usize;
    let mut topic_bytes = 0usize;
    for topic in &state.topics {
        if topic.topic.is_empty() {
            return Err(DescribeTransactionsProtocolFailure::EmptyTopic);
        }
        if topic.topic.len() > MAX_TOPIC_NAME_BYTES {
            return Err(DescribeTransactionsProtocolFailure::TopicTooLong {
                actual: topic.topic.len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        topic_bytes = topic_bytes.checked_add(topic.topic.len()).ok_or(
            DescribeTransactionsProtocolFailure::TopicBytesExceeded {
                required: usize::MAX,
                max: DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES,
            },
        )?;
        if topic_bytes > DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES {
            return Err(DescribeTransactionsProtocolFailure::TopicBytesExceeded {
                required: topic_bytes,
                max: DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES,
            });
        }
        if topic.partitions.is_empty() {
            return Err(DescribeTransactionsProtocolFailure::EmptyPartitions);
        }
        partition_count = partition_count
            .checked_add(topic.partitions.len())
            .unwrap_or(usize::MAX);
        if partition_count > DESCRIBE_TRANSACTIONS_MAX_PARTITIONS {
            return Err(DescribeTransactionsProtocolFailure::TooManyPartitions {
                actual: partition_count,
                max: DESCRIBE_TRANSACTIONS_MAX_PARTITIONS,
            });
        }
        if let Some(partition) = topic.partitions.iter().find(|partition| **partition < 0) {
            return Err(DescribeTransactionsProtocolFailure::NegativePartition {
                actual: *partition,
            });
        }
    }
    Ok(())
}
