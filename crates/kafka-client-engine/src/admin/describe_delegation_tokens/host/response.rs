//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DelegationTokenHmac as CoreHmac, DelegationTokenPrincipal as CorePrincipal, DeliveryStatus,
    DescribeDelegationTokenResponse as CoreToken,
    DescribeDelegationTokensBrokerError as CoreBrokerError, DescribeDelegationTokensInput,
    DescribeDelegationTokensPlan, DescribeDelegationTokensResponse as CoreResponse,
    DescribeDelegationTokensSelection,
};

use crate::{
    driver::{
        DescribeDelegationTokensDriverFailureKind, DescribeDelegationTokensRawTerminal,
        DescribeDelegationTokensTerminalFact,
    },
    protocol::admin::describe_delegation_tokens::{
        DescribeDelegationTokenPrincipalRef, DescribeDelegationTokensRequestRef,
        DescribeDelegationTokensResponseFailure, NormalizedDescribeDelegationTokenPrincipal,
        NormalizedDescribeDelegationTokensResponse, NormalizedDescribedDelegationToken,
        normalize_describe_delegation_tokens_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeDelegationTokensRawTerminal,
    plan: &DescribeDelegationTokensPlan,
    retained_limit: usize,
) -> (DescribeDelegationTokensInput, usize) {
    match raw.fact() {
        DescribeDelegationTokensTerminalFact::Response {
            selected_version,
            response,
        } => {
            let normalized = match plan.selection() {
                DescribeDelegationTokensSelection::All => {
                    normalize_describe_delegation_tokens_response(
                        selected_version,
                        DescribeDelegationTokensRequestRef::all(),
                        response,
                        retained_limit,
                    )
                    .map_err(protocol_failure)
                }
                DescribeDelegationTokensSelection::Owners(owners) => {
                    let scratch = owners
                        .len()
                        .checked_mul(
                            core::mem::size_of::<DescribeDelegationTokenPrincipalRef<'_>>(),
                        )
                        .ok_or(DescribeDelegationTokensInput::ResponseTooLarge);
                    scratch.and_then(|scratch| {
                        let normalization_limit = retained_limit
                            .checked_sub(scratch)
                            .ok_or(DescribeDelegationTokensInput::ResponseTooLarge)?;
                        let mut refs = Vec::new();
                        refs.try_reserve_exact(owners.len())
                            .map_err(|_| DescribeDelegationTokensInput::ResponseTooLarge)?;
                        refs.extend(owners.iter().map(|owner| {
                            DescribeDelegationTokenPrincipalRef::new(
                                owner.principal_type(),
                                owner.principal_name(),
                            )
                        }));
                        normalize_describe_delegation_tokens_response(
                            selected_version,
                            DescribeDelegationTokensRequestRef::selected(&refs),
                            response,
                            normalization_limit,
                        )
                        .map_err(protocol_failure)
                    })
                }
            };
            match normalized {
                Ok(normalized) => normalized_input(normalized),
                Err(input) => (input, 0),
            }
        }
        DescribeDelegationTokensTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    normalized: NormalizedDescribeDelegationTokensResponse,
) -> (DescribeDelegationTokensInput, usize) {
    let (throttle_time_ms, error_code, tokens, retained_bytes) = normalized.into_parts();
    let input = match NonZeroI16::new(error_code) {
        Some(code) if tokens.is_empty() => DescribeDelegationTokensInput::BrokerRejected {
            error: CoreBrokerError::new(throttle_time_ms, code),
        },
        Some(_) => DescribeDelegationTokensInput::InvalidResponse,
        None => {
            core_tokens(tokens).map_or(DescribeDelegationTokensInput::InvalidResponse, |tokens| {
                let response = CoreResponse::new(throttle_time_ms, tokens);
                response.map_or(DescribeDelegationTokensInput::InvalidResponse, |response| {
                    DescribeDelegationTokensInput::BrokerResponded { response }
                })
            })
        }
    };
    (input, retained_bytes)
}

fn core_tokens(tokens: Vec<NormalizedDescribedDelegationToken>) -> Option<Vec<CoreToken>> {
    let mut normalized = Vec::new();
    normalized.try_reserve_exact(tokens.len()).ok()?;
    for token in tokens {
        normalized.push(core_token(token)?);
    }
    Some(normalized)
}

fn core_token(token: NormalizedDescribedDelegationToken) -> Option<CoreToken> {
    let (owner, requester, issue, expiry, max, token_id, hmac, renewers) = token.into_parts();
    let hmac = CoreHmac::new(hmac.into_bytes()).ok()?;
    let owner = core_principal(owner)?;
    let requester = match requester {
        Some(requester) => Some(core_principal(requester)?),
        None => None,
    };
    let mut core_renewers = Vec::new();
    core_renewers.try_reserve_exact(renewers.len()).ok()?;
    for renewer in renewers {
        core_renewers.push(core_principal(renewer)?);
    }
    CoreToken::new(
        owner,
        requester,
        core_renewers,
        issue,
        expiry,
        max,
        token_id,
        hmac,
    )
    .ok()
}

fn core_principal(value: NormalizedDescribeDelegationTokenPrincipal) -> Option<CorePrincipal> {
    let (principal_type, principal_name) = value.into_parts();
    CorePrincipal::new(principal_type, principal_name).ok()
}

pub(super) const fn protocol_failure(
    error: DescribeDelegationTokensResponseFailure,
) -> DescribeDelegationTokensInput {
    match error {
        DescribeDelegationTokensResponseFailure::MissingSelectedVersion
        | DescribeDelegationTokensResponseFailure::UnsupportedApiVersion { .. } => {
            DescribeDelegationTokensInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeDelegationTokensResponseFailure::RetainedBytes { .. }
        | DescribeDelegationTokensResponseFailure::Allocation { .. } => {
            DescribeDelegationTokensInput::ResponseTooLarge
        }
        _ => DescribeDelegationTokensInput::InvalidResponse,
    }
}

const fn driver_failure(
    kind: DescribeDelegationTokensDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeDelegationTokensInput {
    match kind {
        DescribeDelegationTokensDriverFailureKind::DeadlineElapsed => {
            DescribeDelegationTokensInput::DriverDeadlineElapsed { delivery }
        }
        DescribeDelegationTokensDriverFailureKind::Compatibility => {
            DescribeDelegationTokensInput::ProtocolIncompatible { delivery }
        }
        DescribeDelegationTokensDriverFailureKind::InvalidResponse => {
            DescribeDelegationTokensInput::InvalidResponse
        }
        DescribeDelegationTokensDriverFailureKind::Transport => {
            DescribeDelegationTokensInput::TransportFailed { delivery }
        }
    }
}
