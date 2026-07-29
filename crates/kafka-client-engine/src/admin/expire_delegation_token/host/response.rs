//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, ExpireDelegationTokenBrokerError as CoreBrokerError,
    ExpireDelegationTokenInput, ExpireDelegationTokenResponse as CoreResponse,
};

use crate::{
    driver::{
        ExpireDelegationTokenDriverFailureKind, ExpireDelegationTokenRawTerminal,
        ExpireDelegationTokenTerminalFact,
    },
    protocol::admin::expire_delegation_token::{
        ExpireDelegationTokenResponseFailure, NormalizedExpireDelegationTokenResponse,
        normalize_expire_delegation_token_response,
    },
};

pub(super) fn terminal_input(
    raw: &ExpireDelegationTokenRawTerminal,
    retained_limit: usize,
) -> (ExpireDelegationTokenInput, usize) {
    match raw.fact() {
        ExpireDelegationTokenTerminalFact::Response {
            selected_version,
            response,
        } => {
            match normalize_expire_delegation_token_response(
                selected_version,
                response,
                retained_limit,
            ) {
                Ok(normalized) => normalized_input(normalized),
                Err(error) => (protocol_failure(error), 0),
            }
        }
        ExpireDelegationTokenTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    normalized: NormalizedExpireDelegationTokenResponse,
) -> (ExpireDelegationTokenInput, usize) {
    let (throttle_time_ms, error_code, expiry_timestamp_ms, retained_bytes) =
        normalized.into_parts();
    let input = match (NonZeroI16::new(error_code), expiry_timestamp_ms) {
        (Some(code), None) => ExpireDelegationTokenInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code),
        },
        (None, Some(expiry_timestamp_ms)) => {
            CoreResponse::new(throttle_time_ms, expiry_timestamp_ms)
                .map_or(ExpireDelegationTokenInput::InvalidResponse, |response| {
                    ExpireDelegationTokenInput::BrokerResponded { response }
                })
        }
        _ => ExpireDelegationTokenInput::InvalidResponse,
    };
    (input, retained_bytes)
}

pub(super) const fn protocol_failure(
    error: ExpireDelegationTokenResponseFailure,
) -> ExpireDelegationTokenInput {
    match error {
        ExpireDelegationTokenResponseFailure::MissingSelectedVersion
        | ExpireDelegationTokenResponseFailure::UnsupportedApiVersion { .. } => {
            ExpireDelegationTokenInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ExpireDelegationTokenResponseFailure::RetainedBytes { .. } => {
            ExpireDelegationTokenInput::ResponseTooLarge
        }
        ExpireDelegationTokenResponseFailure::NegativeThrottleTime { .. }
        | ExpireDelegationTokenResponseFailure::InvalidExpiryTimestamp { .. } => {
            ExpireDelegationTokenInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: ExpireDelegationTokenDriverFailureKind,
    delivery: DeliveryStatus,
) -> ExpireDelegationTokenInput {
    match kind {
        ExpireDelegationTokenDriverFailureKind::DeadlineElapsed => {
            ExpireDelegationTokenInput::DriverDeadlineElapsed { delivery }
        }
        ExpireDelegationTokenDriverFailureKind::Compatibility => {
            ExpireDelegationTokenInput::ProtocolIncompatible { delivery }
        }
        ExpireDelegationTokenDriverFailureKind::InvalidResponse => {
            ExpireDelegationTokenInput::InvalidResponse
        }
        ExpireDelegationTokenDriverFailureKind::Transport => {
            ExpireDelegationTokenInput::TransportFailed { delivery }
        }
    }
}
