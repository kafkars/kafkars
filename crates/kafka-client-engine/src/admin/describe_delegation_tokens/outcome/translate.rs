//! Exhaustive core-to-engine token-description terminal translation.

use kafka_client_core::{
    DelegationToken as CoreToken, DelegationTokenPrincipal as CorePrincipal,
    DeliveryStatus as CoreDeliveryStatus, DescribeDelegationTokensFailureKind as CoreFailureKind,
    DescribeDelegationTokensTerminal as CoreTerminal,
};

use super::{
    DescribeDelegationTokensBrokerError, DescribeDelegationTokensDeliveryStatus,
    DescribeDelegationTokensFailure, DescribeDelegationTokensFailureKind,
    DescribeDelegationTokensOutcome,
};
use crate::admin::describe_delegation_tokens::{
    DescribeDelegationTokenHmac, DescribeDelegationTokenPrincipal, DescribeDelegationTokensResult,
    DescribedDelegationToken,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeDelegationTokensOutcome {
    match terminal {
        CoreTerminal::Described(listing) => {
            let (throttle_time_ms, tokens) = listing.into_parts();
            DescribeDelegationTokensOutcome::Described(DescribeDelegationTokensResult {
                throttle_time_ms,
                tokens: tokens.into_iter().map(token).collect(),
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            DescribeDelegationTokensOutcome::BrokerRejected(DescribeDelegationTokensBrokerError {
                throttle_time_ms,
                code,
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeDelegationTokensOutcome::Failed(DescribeDelegationTokensFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn token(value: CoreToken) -> DescribedDelegationToken {
    let (owner, requester, renewers, issue, expiry, max, token_id, hmac) = value.into_parts();
    DescribedDelegationToken {
        owner: principal(owner),
        requester: requester.map(principal),
        renewers: renewers.into_iter().map(principal).collect(),
        issue_timestamp_ms: issue,
        expiry_timestamp_ms: expiry,
        max_timestamp_ms: max,
        token_id,
        hmac: DescribeDelegationTokenHmac::new(hmac.into_bytes()),
    }
}

fn principal(value: CorePrincipal) -> DescribeDelegationTokenPrincipal {
    let (principal_type, principal_name) = value.into_parts();
    DescribeDelegationTokenPrincipal::new(principal_type, principal_name)
}

const fn failure_kind(kind: CoreFailureKind) -> DescribeDelegationTokensFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeDelegationTokensFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeDelegationTokensFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeDelegationTokensFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeDelegationTokensFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeDelegationTokensFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeDelegationTokensFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DescribeDelegationTokensDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeDelegationTokensDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeDelegationTokensDeliveryStatus::PossiblySent,
    }
}
