//! Exhaustive core-to-engine token-creation terminal translation.

use kafka_client_core::{
    CreateDelegationTokenFailureKind as CoreFailureKind,
    CreateDelegationTokenTerminal as CoreTerminal, DelegationTokenPrincipal as CorePrincipal,
    DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    CreateDelegationTokenBrokerError, CreateDelegationTokenDeliveryStatus,
    CreateDelegationTokenFailure, CreateDelegationTokenFailureKind, CreateDelegationTokenOutcome,
};
use crate::admin::create_delegation_token::{
    CreateDelegationTokenHmac, CreateDelegationTokenPrincipal, CreateDelegationTokenResult,
    CreatedDelegationToken,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> CreateDelegationTokenOutcome {
    match terminal {
        CoreTerminal::Created(success) => {
            let (throttle_time_ms, token) = success.into_parts();
            let (
                owner,
                requester,
                renewers,
                issue_timestamp_ms,
                expiry_timestamp_ms,
                max_timestamp_ms,
                token_id,
                hmac,
            ) = token.into_parts();
            CreateDelegationTokenOutcome::Created(CreateDelegationTokenResult {
                throttle_time_ms,
                token: CreatedDelegationToken {
                    owner: principal(owner),
                    requester: requester.map(principal),
                    renewers: renewers.into_iter().map(principal).collect(),
                    issue_timestamp_ms,
                    expiry_timestamp_ms,
                    max_timestamp_ms,
                    token_id,
                    hmac: CreateDelegationTokenHmac::new(hmac.into_bytes()),
                },
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            CreateDelegationTokenOutcome::BrokerRejected(CreateDelegationTokenBrokerError {
                throttle_time_ms,
                code,
            })
        }
        CoreTerminal::Failed(failure) => {
            CreateDelegationTokenOutcome::Failed(CreateDelegationTokenFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn principal(value: CorePrincipal) -> CreateDelegationTokenPrincipal {
    let (principal_type, principal_name) = value.into_parts();
    CreateDelegationTokenPrincipal::new(principal_type, principal_name)
}

const fn failure_kind(kind: CoreFailureKind) -> CreateDelegationTokenFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => CreateDelegationTokenFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => CreateDelegationTokenFailureKind::DriverRejected,
        CoreFailureKind::Transport => CreateDelegationTokenFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => CreateDelegationTokenFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => CreateDelegationTokenFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => CreateDelegationTokenFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> CreateDelegationTokenDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => CreateDelegationTokenDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => CreateDelegationTokenDeliveryStatus::PossiblySent,
    }
}
