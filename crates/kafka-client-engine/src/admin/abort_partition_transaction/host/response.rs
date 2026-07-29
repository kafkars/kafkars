//! Exhaustive wire and driver-failure translation into deterministic input.

use kafka_client_core::{
    AbortPartitionTransactionBrokerError as CoreBrokerError, AbortPartitionTransactionInput,
    DeliveryStatus,
};

use crate::{
    driver::{
        AbortPartitionTransactionDriverFailureKind, AbortPartitionTransactionRawTerminal,
        AbortPartitionTransactionTerminalFact,
    },
    protocol::admin::abort_partition_transaction::{
        AbortPartitionTransactionResponseFailure, normalize_abort_partition_transaction_response,
    },
};

pub(super) fn terminal_input(
    raw: &AbortPartitionTransactionRawTerminal,
    plan: &kafka_client_core::AbortPartitionTransactionPlan,
) -> AbortPartitionTransactionInput {
    match raw.fact() {
        AbortPartitionTransactionTerminalFact::Response {
            selected_version: None,
            ..
        } => AbortPartitionTransactionInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        },
        AbortPartitionTransactionTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_abort_partition_transaction_response(plan, selected_version, response)
        {
            Ok(None) => AbortPartitionTransactionInput::BrokerResponded,
            Ok(Some(code)) => AbortPartitionTransactionInput::BrokerRejected {
                error: CoreBrokerError::new(code),
            },
            Err(AbortPartitionTransactionResponseFailure::UnsupportedApiVersion { .. }) => {
                AbortPartitionTransactionInput::ProtocolIncompatible {
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            Err(
                AbortPartitionTransactionResponseFailure::MissingProducer
                | AbortPartitionTransactionResponseFailure::DuplicateProducer
                | AbortPartitionTransactionResponseFailure::UnexpectedProducer { .. }
                | AbortPartitionTransactionResponseFailure::MissingTopic
                | AbortPartitionTransactionResponseFailure::DuplicateTopic
                | AbortPartitionTransactionResponseFailure::UnexpectedTopic
                | AbortPartitionTransactionResponseFailure::MissingPartition
                | AbortPartitionTransactionResponseFailure::DuplicatePartition
                | AbortPartitionTransactionResponseFailure::InvalidPartition { .. }
                | AbortPartitionTransactionResponseFailure::UnexpectedPartition { .. },
            ) => AbortPartitionTransactionInput::InvalidResponse,
        },
        AbortPartitionTransactionTerminalFact::Failed { kind, delivery } => {
            driver_failure(kind, delivery)
        }
    }
}

const fn driver_failure(
    kind: AbortPartitionTransactionDriverFailureKind,
    delivery: DeliveryStatus,
) -> AbortPartitionTransactionInput {
    match kind {
        AbortPartitionTransactionDriverFailureKind::DeadlineElapsed => {
            AbortPartitionTransactionInput::DriverDeadlineElapsed { delivery }
        }
        AbortPartitionTransactionDriverFailureKind::Compatibility => {
            AbortPartitionTransactionInput::ProtocolIncompatible { delivery }
        }
        AbortPartitionTransactionDriverFailureKind::InvalidResponse => {
            AbortPartitionTransactionInput::InvalidResponse
        }
        AbortPartitionTransactionDriverFailureKind::Transport => {
            AbortPartitionTransactionInput::TransportFailed { delivery }
        }
    }
}
