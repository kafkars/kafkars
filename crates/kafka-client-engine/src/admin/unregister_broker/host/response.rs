//! Exhaustive generated-free response and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, UnregisterBrokerBrokerError as CoreBrokerError, UnregisterBrokerInput,
    UnregisterBrokerSuccess,
};

use crate::{
    driver::{
        UnregisterBrokerDriverFailureKind, UnregisterBrokerRawTerminal,
        UnregisterBrokerTerminalFact,
    },
    protocol::admin::unregister_broker::{
        UnregisterBrokerResponseFailure, normalize_unregister_broker_response,
    },
};

pub(super) fn terminal_input(
    raw: &UnregisterBrokerRawTerminal,
    retained_limit: usize,
) -> (UnregisterBrokerInput, usize) {
    match raw.fact() {
        UnregisterBrokerTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_unregister_broker_response(selected_version, response, retained_limit)
        {
            Ok(normalized) => {
                let (throttle, code, message, truncated, retained) = normalized.into_parts();
                (
                    normalized_input(throttle, code, message, truncated),
                    retained,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        UnregisterBrokerTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    message: Option<String>,
    message_truncated: bool,
) -> UnregisterBrokerInput {
    match NonZeroI16::new(error_code) {
        Some(code) => UnregisterBrokerInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code, message, message_truncated),
        },
        None if message.is_none() && !message_truncated => UnregisterBrokerInput::BrokerResponded {
            success: UnregisterBrokerSuccess::new(throttle_time_ms),
        },
        None => UnregisterBrokerInput::InvalidResponse,
    }
}

const fn protocol_failure(error: UnregisterBrokerResponseFailure) -> UnregisterBrokerInput {
    match error {
        UnregisterBrokerResponseFailure::MissingSelectedVersion
        | UnregisterBrokerResponseFailure::UnsupportedApiVersion { .. } => {
            UnregisterBrokerInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        UnregisterBrokerResponseFailure::RetainedBytes { .. }
        | UnregisterBrokerResponseFailure::Allocation { .. } => {
            UnregisterBrokerInput::ResponseTooLarge
        }
        UnregisterBrokerResponseFailure::NegativeThrottleTime { .. } => {
            UnregisterBrokerInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: UnregisterBrokerDriverFailureKind,
    delivery: DeliveryStatus,
) -> UnregisterBrokerInput {
    match kind {
        UnregisterBrokerDriverFailureKind::DeadlineElapsed => {
            UnregisterBrokerInput::DriverDeadlineElapsed { delivery }
        }
        UnregisterBrokerDriverFailureKind::Compatibility => {
            UnregisterBrokerInput::ProtocolIncompatible { delivery }
        }
        UnregisterBrokerDriverFailureKind::InvalidResponse => {
            UnregisterBrokerInput::InvalidResponse
        }
        UnregisterBrokerDriverFailureKind::Transport => {
            UnregisterBrokerInput::TransportFailed { delivery }
        }
    }
}
