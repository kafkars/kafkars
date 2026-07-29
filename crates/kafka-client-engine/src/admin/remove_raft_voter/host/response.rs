//! Exhaustive generated-free response and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, RemoveRaftVoterBrokerError as CoreBrokerError, RemoveRaftVoterInput,
    RemoveRaftVoterSuccess,
};

use crate::{
    driver::{
        RemoveRaftVoterDriverFailureKind, RemoveRaftVoterRawTerminal, RemoveRaftVoterTerminalFact,
    },
    protocol::admin::remove_raft_voter::{
        RemoveRaftVoterResponseFailure, normalize_remove_raft_voter_response,
    },
};

pub(super) fn terminal_input(
    raw: &RemoveRaftVoterRawTerminal,
    retained_limit: usize,
) -> (RemoveRaftVoterInput, usize) {
    match raw.fact() {
        RemoveRaftVoterTerminalFact::Response {
            selected_version,
            response,
        } => {
            match normalize_remove_raft_voter_response(selected_version, response, retained_limit) {
                Ok(normalized) => {
                    let (throttle, code, message, truncated, retained) = normalized.into_parts();
                    (
                        normalized_input(throttle, code, message, truncated),
                        retained,
                    )
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        RemoveRaftVoterTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    message: Option<String>,
    message_truncated: bool,
) -> RemoveRaftVoterInput {
    match NonZeroI16::new(error_code) {
        Some(code) => RemoveRaftVoterInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code, message, message_truncated),
        },
        None if message.is_none() && !message_truncated => RemoveRaftVoterInput::BrokerResponded {
            success: RemoveRaftVoterSuccess::new(throttle_time_ms),
        },
        None => RemoveRaftVoterInput::InvalidResponse,
    }
}

const fn protocol_failure(error: RemoveRaftVoterResponseFailure) -> RemoveRaftVoterInput {
    match error {
        RemoveRaftVoterResponseFailure::MissingSelectedVersion
        | RemoveRaftVoterResponseFailure::UnsupportedApiVersion { .. } => {
            RemoveRaftVoterInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        RemoveRaftVoterResponseFailure::RetainedBytes { .. }
        | RemoveRaftVoterResponseFailure::Allocation { .. } => {
            RemoveRaftVoterInput::ResponseTooLarge
        }
        RemoveRaftVoterResponseFailure::NegativeThrottleTime { .. } => {
            RemoveRaftVoterInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: RemoveRaftVoterDriverFailureKind,
    delivery: DeliveryStatus,
) -> RemoveRaftVoterInput {
    match kind {
        RemoveRaftVoterDriverFailureKind::DeadlineElapsed => {
            RemoveRaftVoterInput::DriverDeadlineElapsed { delivery }
        }
        RemoveRaftVoterDriverFailureKind::Compatibility => {
            RemoveRaftVoterInput::ProtocolIncompatible { delivery }
        }
        RemoveRaftVoterDriverFailureKind::InvalidResponse => RemoveRaftVoterInput::InvalidResponse,
        RemoveRaftVoterDriverFailureKind::Transport => {
            RemoveRaftVoterInput::TransportFailed { delivery }
        }
    }
}
