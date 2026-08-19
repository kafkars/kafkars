//! Exhaustive `InitProducerId` terminal translation into deterministic fencing input.

use kafka_client_core::{
    AdminFenceProducerBrokerError, AdminFenceProducerOutcome, AdminFenceProducersInput,
    AdminFencedProducerIdentity, DeliveryStatus,
};

use crate::{
    driver::{
        TransactionInitDriverFailureKind, TransactionInitTerminal, TransactionInitTerminalFact,
    },
    protocol::transaction::{
        FenceProducerResponseFailure, NormalizedFenceProducerResult,
        normalize_fence_producer_response,
    },
};

pub(super) fn terminal_input(
    raw: &TransactionInitTerminal,
    transactional_id: &str,
    retained_limit: usize,
) -> (AdminFenceProducersInput, usize) {
    match raw.fact() {
        TransactionInitTerminalFact::Response {
            selected_version: Some(_),
            response,
        } => match normalize_fence_producer_response(response, transactional_id, retained_limit) {
            Ok(normalized) => {
                let (throttle_time_ms, result, retained_bytes) = normalized.into_parts();
                (
                    normalized_input(transactional_id, throttle_time_ms, result),
                    retained_bytes,
                )
            }
            Err(FenceProducerResponseFailure::RetainedBytes { .. }) => {
                (AdminFenceProducersInput::ResponseTooLarge, 0)
            }
            Err(
                FenceProducerResponseFailure::NegativeThrottleTime
                | FenceProducerResponseFailure::InvalidIdentity,
            ) => (AdminFenceProducersInput::InvalidResponse, 0),
        },
        TransactionInitTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AdminFenceProducersInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        TransactionInitTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn normalized_input(
    transactional_id: &str,
    throttle_time_ms: u32,
    result: NormalizedFenceProducerResult,
) -> AdminFenceProducersInput {
    let outcome = match result {
        NormalizedFenceProducerResult::Fenced {
            producer_id,
            producer_epoch,
        } => {
            let Some(identity) = AdminFencedProducerIdentity::try_new(producer_id, producer_epoch)
            else {
                return AdminFenceProducersInput::InvalidResponse;
            };
            AdminFenceProducerOutcome::fenced(transactional_id.to_owned(), identity)
        }
        NormalizedFenceProducerResult::BrokerFailed { code } => {
            AdminFenceProducerOutcome::broker_failed(
                transactional_id.to_owned(),
                AdminFenceProducerBrokerError::new(code),
            )
        }
    };
    AdminFenceProducersInput::BrokerResponded {
        throttle_time_ms,
        outcome,
    }
}

const fn driver_failure(
    kind: TransactionInitDriverFailureKind,
    delivery: DeliveryStatus,
) -> AdminFenceProducersInput {
    match kind {
        TransactionInitDriverFailureKind::DeadlineElapsed => {
            AdminFenceProducersInput::DriverDeadlineElapsed { delivery }
        }
        TransactionInitDriverFailureKind::InvalidResponse => {
            AdminFenceProducersInput::InvalidResponse
        }
        TransactionInitDriverFailureKind::Compatibility => {
            AdminFenceProducersInput::ProtocolIncompatible { delivery }
        }
        TransactionInitDriverFailureKind::Transport => {
            AdminFenceProducersInput::TransportFailed { delivery }
        }
    }
}
