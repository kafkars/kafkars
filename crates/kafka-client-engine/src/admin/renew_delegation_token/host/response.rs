//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, RenewDelegationTokenBrokerError as CoreBrokerError, RenewDelegationTokenInput,
    RenewDelegationTokenResponse as CoreResponse,
};

use crate::{
    driver::{
        RenewDelegationTokenDriverFailureKind, RenewDelegationTokenRawTerminal,
        RenewDelegationTokenTerminalFact,
    },
    protocol::admin::renew_delegation_token::{
        NormalizedRenewDelegationTokenResponse, RenewDelegationTokenResponseFailure,
        normalize_renew_delegation_token_response,
    },
};

pub(super) fn terminal_input(
    raw: &RenewDelegationTokenRawTerminal,
    retained_limit: usize,
) -> (RenewDelegationTokenInput, usize) {
    match raw.fact() {
        RenewDelegationTokenTerminalFact::Response {
            selected_version,
            response,
        } => {
            match normalize_renew_delegation_token_response(
                selected_version,
                response,
                retained_limit,
            ) {
                Ok(normalized) => normalized_input(normalized),
                Err(error) => (protocol_failure(error), 0),
            }
        }
        RenewDelegationTokenTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    normalized: NormalizedRenewDelegationTokenResponse,
) -> (RenewDelegationTokenInput, usize) {
    let (throttle_time_ms, error_code, expiry_timestamp_ms, retained_bytes) =
        normalized.into_parts();
    let input = match (NonZeroI16::new(error_code), expiry_timestamp_ms) {
        (Some(code), None) => RenewDelegationTokenInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code),
        },
        (None, Some(expiry_timestamp_ms)) => {
            CoreResponse::new(throttle_time_ms, expiry_timestamp_ms)
                .map_or(RenewDelegationTokenInput::InvalidResponse, |response| {
                    RenewDelegationTokenInput::BrokerResponded { response }
                })
        }
        _ => RenewDelegationTokenInput::InvalidResponse,
    };
    (input, retained_bytes)
}

pub(super) const fn protocol_failure(
    error: RenewDelegationTokenResponseFailure,
) -> RenewDelegationTokenInput {
    match error {
        RenewDelegationTokenResponseFailure::MissingSelectedVersion
        | RenewDelegationTokenResponseFailure::UnsupportedApiVersion { .. } => {
            RenewDelegationTokenInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        RenewDelegationTokenResponseFailure::RetainedBytes { .. } => {
            RenewDelegationTokenInput::ResponseTooLarge
        }
        RenewDelegationTokenResponseFailure::NegativeThrottleTime { .. }
        | RenewDelegationTokenResponseFailure::InvalidExpiryTimestamp { .. } => {
            RenewDelegationTokenInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: RenewDelegationTokenDriverFailureKind,
    delivery: DeliveryStatus,
) -> RenewDelegationTokenInput {
    match kind {
        RenewDelegationTokenDriverFailureKind::DeadlineElapsed => {
            RenewDelegationTokenInput::DriverDeadlineElapsed { delivery }
        }
        RenewDelegationTokenDriverFailureKind::Compatibility => {
            RenewDelegationTokenInput::ProtocolIncompatible { delivery }
        }
        RenewDelegationTokenDriverFailureKind::InvalidResponse => {
            RenewDelegationTokenInput::InvalidResponse
        }
        RenewDelegationTokenDriverFailureKind::Transport => {
            RenewDelegationTokenInput::TransportFailed { delivery }
        }
    }
}
