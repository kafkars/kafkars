//! Exhaustive generated-free response and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    AddRaftVoterBrokerError as CoreBrokerError, AddRaftVoterInput, AddRaftVoterSuccess,
    DeliveryStatus,
};

use crate::{
    driver::{AddRaftVoterDriverFailureKind, AddRaftVoterRawTerminal, AddRaftVoterTerminalFact},
    protocol::admin::add_raft_voter::{
        AddRaftVoterResponseFailure, normalize_add_raft_voter_response,
    },
};

pub(super) fn terminal_input(
    raw: &AddRaftVoterRawTerminal,
    retained_limit: usize,
) -> (AddRaftVoterInput, usize) {
    match raw.fact() {
        AddRaftVoterTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_add_raft_voter_response(selected_version, response, retained_limit) {
            Ok(normalized) => {
                let (throttle, code, message, truncated, retained) = normalized.into_parts();
                (
                    normalized_input(throttle, code, message, truncated),
                    retained,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        AddRaftVoterTerminalFact::Failed { kind, delivery } => (driver_failure(kind, delivery), 0),
    }
}

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    message: Option<String>,
    message_truncated: bool,
) -> AddRaftVoterInput {
    match NonZeroI16::new(error_code) {
        Some(code) => AddRaftVoterInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code, message, message_truncated),
        },
        None if message.is_none() && !message_truncated => AddRaftVoterInput::BrokerResponded {
            success: AddRaftVoterSuccess::new(throttle_time_ms),
        },
        None => AddRaftVoterInput::InvalidResponse,
    }
}

pub(super) const fn protocol_failure(error: AddRaftVoterResponseFailure) -> AddRaftVoterInput {
    match error {
        AddRaftVoterResponseFailure::MissingSelectedVersion
        | AddRaftVoterResponseFailure::UnsupportedApiVersion { .. } => {
            AddRaftVoterInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        AddRaftVoterResponseFailure::RetainedBytes { .. }
        | AddRaftVoterResponseFailure::Allocation { .. } => AddRaftVoterInput::ResponseTooLarge,
        AddRaftVoterResponseFailure::NegativeThrottleTime { .. } => {
            AddRaftVoterInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: AddRaftVoterDriverFailureKind,
    delivery: DeliveryStatus,
) -> AddRaftVoterInput {
    match kind {
        AddRaftVoterDriverFailureKind::DeadlineElapsed => {
            AddRaftVoterInput::DriverDeadlineElapsed { delivery }
        }
        AddRaftVoterDriverFailureKind::Compatibility => {
            AddRaftVoterInput::ProtocolIncompatible { delivery }
        }
        AddRaftVoterDriverFailureKind::InvalidResponse => AddRaftVoterInput::InvalidResponse,
        AddRaftVoterDriverFailureKind::Transport => AddRaftVoterInput::TransportFailed { delivery },
    }
}
