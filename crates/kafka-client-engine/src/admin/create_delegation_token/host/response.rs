//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    CreateDelegationTokenBrokerError as CoreBrokerError, CreateDelegationTokenInput,
    CreateDelegationTokenResponse as CoreResponse, DelegationTokenHmac as CoreHmac,
    DelegationTokenPrincipal as CorePrincipal, DeliveryStatus,
};

use crate::{
    driver::{
        CreateDelegationTokenDriverFailureKind, CreateDelegationTokenRawTerminal,
        CreateDelegationTokenTerminalFact,
    },
    protocol::admin::create_delegation_token::{
        CreateDelegationTokenResponseFailure, NormalizedCreateDelegationTokenResponse,
        NormalizedDelegationTokenPrincipal, normalize_create_delegation_token_response,
    },
};

pub(super) fn terminal_input(
    raw: &CreateDelegationTokenRawTerminal,
    retained_limit: usize,
) -> (CreateDelegationTokenInput, usize) {
    match raw.fact() {
        CreateDelegationTokenTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_create_delegation_token_response(
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => normalized_input(normalized),
            Err(error) => (protocol_failure(error), 0),
        },
        CreateDelegationTokenTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    normalized: NormalizedCreateDelegationTokenResponse,
) -> (CreateDelegationTokenInput, usize) {
    let (throttle_time_ms, error_code, token, retained_bytes) = normalized.into_parts();
    let input = match (NonZeroI16::new(error_code), token) {
        (Some(code), None) => CreateDelegationTokenInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code),
        },
        (None, Some(token)) => {
            let (owner, requester, issue, expiry, max, token_id, hmac) = token.into_parts();
            let response = core_response(
                throttle_time_ms,
                owner,
                requester,
                issue,
                expiry,
                max,
                token_id,
                hmac.into_bytes(),
            );
            response.map_or(CreateDelegationTokenInput::InvalidResponse, |response| {
                CreateDelegationTokenInput::BrokerResponded { response }
            })
        }
        _ => CreateDelegationTokenInput::InvalidResponse,
    };
    (input, retained_bytes)
}

#[allow(clippy::too_many_arguments)]
fn core_response(
    throttle_time_ms: u32,
    owner: NormalizedDelegationTokenPrincipal,
    requester: Option<NormalizedDelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: Vec<u8>,
) -> Option<CoreResponse> {
    let hmac = CoreHmac::new(hmac).ok()?;
    let owner = core_principal(owner)?;
    let requester = match requester {
        Some(requester) => Some(core_principal(requester)?),
        None => None,
    };
    CoreResponse::new(
        throttle_time_ms,
        owner,
        requester,
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        hmac,
    )
    .ok()
}

fn core_principal(value: NormalizedDelegationTokenPrincipal) -> Option<CorePrincipal> {
    let (principal_type, principal_name) = value.into_parts();
    CorePrincipal::new(principal_type, principal_name).ok()
}

pub(super) const fn protocol_failure(
    error: CreateDelegationTokenResponseFailure,
) -> CreateDelegationTokenInput {
    match error {
        CreateDelegationTokenResponseFailure::MissingSelectedVersion
        | CreateDelegationTokenResponseFailure::UnsupportedApiVersion { .. } => {
            CreateDelegationTokenInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        CreateDelegationTokenResponseFailure::RetainedBytes { .. }
        | CreateDelegationTokenResponseFailure::Allocation { .. } => {
            CreateDelegationTokenInput::ResponseTooLarge
        }
        _ => CreateDelegationTokenInput::InvalidResponse,
    }
}

const fn driver_failure(
    kind: CreateDelegationTokenDriverFailureKind,
    delivery: DeliveryStatus,
) -> CreateDelegationTokenInput {
    match kind {
        CreateDelegationTokenDriverFailureKind::DeadlineElapsed => {
            CreateDelegationTokenInput::DriverDeadlineElapsed { delivery }
        }
        CreateDelegationTokenDriverFailureKind::Compatibility => {
            CreateDelegationTokenInput::ProtocolIncompatible { delivery }
        }
        CreateDelegationTokenDriverFailureKind::InvalidResponse => {
            CreateDelegationTokenInput::InvalidResponse
        }
        CreateDelegationTokenDriverFailureKind::Transport => {
            CreateDelegationTokenInput::TransportFailed { delivery }
        }
    }
}
