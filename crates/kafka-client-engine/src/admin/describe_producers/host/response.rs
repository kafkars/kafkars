//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminDescribeProducerBrokerError, AdminDescribeProducerOutcome, AdminDescribeProducerTarget,
    AdminDescribeProducersInput, AdminProducerState, DeliveryStatus,
};

use crate::{
    driver::{
        DescribeProducersDriverFailureKind, DescribeProducersRawTerminal,
        DescribeProducersTerminalFact,
    },
    protocol::admin::describe_producers::{
        DescribeProducersProtocolFailure, NormalizedDescribeProducerResult,
        normalize_describe_producers_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeProducersRawTerminal,
    target: &AdminDescribeProducerTarget,
    retained_limit: usize,
) -> (AdminDescribeProducersInput, usize) {
    match raw.fact() {
        DescribeProducersTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_describe_producers_response(
            target,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, result, retained_bytes) = normalized.into_parts();
                (
                    normalized_input(target, throttle_time_ms, result),
                    retained_bytes,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeProducersTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AdminDescribeProducersInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeProducersTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn normalized_input(
    target: &AdminDescribeProducerTarget,
    throttle_time_ms: u32,
    result: NormalizedDescribeProducerResult,
) -> AdminDescribeProducersInput {
    let outcome = match result {
        NormalizedDescribeProducerResult::Described(states) => {
            AdminDescribeProducerOutcome::described(
                target.topic().to_owned(),
                target.partition(),
                states
                    .into_iter()
                    .map(|state| {
                        let (
                            producer_id,
                            producer_epoch,
                            last_sequence,
                            last_timestamp,
                            coordinator_epoch,
                            transaction_offset,
                        ) = state.into_parts();
                        AdminProducerState::new(
                            producer_id,
                            producer_epoch,
                            last_sequence,
                            last_timestamp,
                            coordinator_epoch,
                            transaction_offset,
                        )
                    })
                    .collect(),
            )
        }
        NormalizedDescribeProducerResult::BrokerFailed(error) => {
            let (code, message, message_truncated) = error.into_parts();
            let Some(code) = NonZeroI16::new(code) else {
                return AdminDescribeProducersInput::InvalidResponse;
            };
            AdminDescribeProducerOutcome::broker_failed(
                target.topic().to_owned(),
                target.partition(),
                AdminDescribeProducerBrokerError::new(code, message, message_truncated),
            )
        }
    };
    AdminDescribeProducersInput::BrokerResponded {
        throttle_time_ms,
        outcome,
    }
}

pub(super) const fn protocol_failure(
    error: DescribeProducersProtocolFailure,
) -> AdminDescribeProducersInput {
    match error {
        DescribeProducersProtocolFailure::UnsupportedApiVersion { .. } => {
            AdminDescribeProducersInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeProducersProtocolFailure::RetainedBytes { .. }
        | DescribeProducersProtocolFailure::Allocation { .. } => {
            AdminDescribeProducersInput::ResponseTooLarge
        }
        DescribeProducersProtocolFailure::NegativeThrottleTime { .. }
        | DescribeProducersProtocolFailure::UnexpectedTopicCount { .. }
        | DescribeProducersProtocolFailure::UnexpectedTopic
        | DescribeProducersProtocolFailure::UnexpectedPartitionCount { .. }
        | DescribeProducersProtocolFailure::NegativePartition { .. }
        | DescribeProducersProtocolFailure::UnexpectedPartition { .. }
        | DescribeProducersProtocolFailure::ProducerStatesWithPartitionError { .. }
        | DescribeProducersProtocolFailure::DiagnosticOnSuccess
        | DescribeProducersProtocolFailure::TooManyProducerStates { .. }
        | DescribeProducersProtocolFailure::NegativeProducerId { .. }
        | DescribeProducersProtocolFailure::NegativeProducerEpoch { .. }
        | DescribeProducersProtocolFailure::InvalidLastSequence { .. }
        | DescribeProducersProtocolFailure::InvalidLastTimestamp { .. }
        | DescribeProducersProtocolFailure::NegativeCoordinatorEpoch { .. }
        | DescribeProducersProtocolFailure::InvalidCurrentTransactionStartOffset { .. }
        | DescribeProducersProtocolFailure::DuplicateProducerId { .. } => {
            AdminDescribeProducersInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeProducersDriverFailureKind,
    delivery: DeliveryStatus,
) -> AdminDescribeProducersInput {
    match kind {
        DescribeProducersDriverFailureKind::DeadlineElapsed => {
            AdminDescribeProducersInput::DriverDeadlineElapsed { delivery }
        }
        DescribeProducersDriverFailureKind::Compatibility => {
            AdminDescribeProducersInput::ProtocolIncompatible { delivery }
        }
        DescribeProducersDriverFailureKind::InvalidResponse => {
            AdminDescribeProducersInput::InvalidResponse
        }
        DescribeProducersDriverFailureKind::Transport => {
            AdminDescribeProducersInput::TransportFailed { delivery }
        }
    }
}
