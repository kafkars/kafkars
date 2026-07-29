//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminDescribeTransactionBrokerError, AdminDescribeTransactionDescription,
    AdminDescribeTransactionOutcome, AdminDescribeTransactionTopic, AdminDescribeTransactionsInput,
    DeliveryStatus,
};

use crate::{
    driver::{
        DescribeTransactionsDriverFailureKind, DescribeTransactionsRawTerminal,
        DescribeTransactionsTerminalFact,
    },
    protocol::admin::describe_transactions::{
        DescribeTransactionsProtocolFailure, NormalizedDescribeTransactionResult,
        normalize_describe_transactions_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeTransactionsRawTerminal,
    transactional_id: &str,
    retained_limit: usize,
) -> (AdminDescribeTransactionsInput, usize) {
    match raw.fact() {
        DescribeTransactionsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_describe_transactions_response(
            transactional_id,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, result, retained_bytes) = normalized.into_parts();
                (
                    normalized_input(transactional_id, throttle_time_ms, result),
                    retained_bytes,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeTransactionsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AdminDescribeTransactionsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeTransactionsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn normalized_input(
    transactional_id: &str,
    throttle_time_ms: u32,
    result: NormalizedDescribeTransactionResult,
) -> AdminDescribeTransactionsInput {
    let outcome = match result {
        NormalizedDescribeTransactionResult::Described(description) => {
            let (state, timeout_ms, start_time_ms, producer_id, producer_epoch, topics) =
                description.into_parts();
            AdminDescribeTransactionOutcome::described(
                transactional_id.to_owned(),
                AdminDescribeTransactionDescription::new(
                    state,
                    timeout_ms,
                    start_time_ms,
                    producer_id,
                    producer_epoch,
                    topics
                        .into_iter()
                        .map(|topic| {
                            let (topic, partitions) = topic.into_parts();
                            AdminDescribeTransactionTopic::new(topic, partitions)
                        })
                        .collect(),
                ),
            )
        }
        NormalizedDescribeTransactionResult::BrokerFailed(error) => {
            let Some(code) = NonZeroI16::new(error.code()) else {
                return AdminDescribeTransactionsInput::InvalidResponse;
            };
            AdminDescribeTransactionOutcome::broker_failed(
                transactional_id.to_owned(),
                AdminDescribeTransactionBrokerError::new(code),
            )
        }
    };
    AdminDescribeTransactionsInput::BrokerResponded {
        throttle_time_ms,
        outcome,
    }
}

pub(super) const fn protocol_failure(
    error: DescribeTransactionsProtocolFailure,
) -> AdminDescribeTransactionsInput {
    match error {
        DescribeTransactionsProtocolFailure::UnsupportedApiVersion { .. } => {
            AdminDescribeTransactionsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeTransactionsProtocolFailure::RetainedBytes { .. }
        | DescribeTransactionsProtocolFailure::Allocation { .. } => {
            AdminDescribeTransactionsInput::ResponseTooLarge
        }
        DescribeTransactionsProtocolFailure::NegativeThrottleTime { .. }
        | DescribeTransactionsProtocolFailure::UnexpectedTransactionStateCount { .. }
        | DescribeTransactionsProtocolFailure::UnexpectedTransactionalId
        | DescribeTransactionsProtocolFailure::SuccessPayloadWithBrokerError { .. }
        | DescribeTransactionsProtocolFailure::EmptyTransactionState
        | DescribeTransactionsProtocolFailure::TransactionStateTooLong { .. }
        | DescribeTransactionsProtocolFailure::InvalidTransactionStartTime { .. }
        | DescribeTransactionsProtocolFailure::TooManyTopics { .. }
        | DescribeTransactionsProtocolFailure::EmptyTopic
        | DescribeTransactionsProtocolFailure::TopicTooLong { .. }
        | DescribeTransactionsProtocolFailure::EmptyPartitions
        | DescribeTransactionsProtocolFailure::TooManyPartitions { .. }
        | DescribeTransactionsProtocolFailure::NegativePartition { .. }
        | DescribeTransactionsProtocolFailure::DuplicateTopic
        | DescribeTransactionsProtocolFailure::DuplicatePartition { .. }
        | DescribeTransactionsProtocolFailure::TopicBytesExceeded { .. } => {
            AdminDescribeTransactionsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeTransactionsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AdminDescribeTransactionsInput {
    match kind {
        DescribeTransactionsDriverFailureKind::DeadlineElapsed => {
            AdminDescribeTransactionsInput::DriverDeadlineElapsed { delivery }
        }
        DescribeTransactionsDriverFailureKind::Compatibility => {
            AdminDescribeTransactionsInput::ProtocolIncompatible { delivery }
        }
        DescribeTransactionsDriverFailureKind::InvalidResponse => {
            AdminDescribeTransactionsInput::InvalidResponse
        }
        DescribeTransactionsDriverFailureKind::Transport => {
            AdminDescribeTransactionsInput::TransportFailed { delivery }
        }
    }
}
