//! Exhaustive core-to-engine token-expiration terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ExpireDelegationTokenFailureKind as CoreFailureKind,
    ExpireDelegationTokenTerminal as CoreTerminal,
};

use super::{
    ExpireDelegationTokenBrokerError, ExpireDelegationTokenDeliveryStatus,
    ExpireDelegationTokenFailure, ExpireDelegationTokenFailureKind, ExpireDelegationTokenOutcome,
};
use crate::admin::expire_delegation_token::ExpireDelegationTokenResult;

#[allow(
    clippy::needless_pass_by_value,
    reason = "terminal translation is an explicit ownership boundary even while the core value is copyable"
)]
pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ExpireDelegationTokenOutcome {
    match terminal {
        CoreTerminal::Expired(success) => {
            let (throttle_time_ms, expiry_timestamp_ms) = success.into_parts();
            ExpireDelegationTokenOutcome::Expired(ExpireDelegationTokenResult {
                throttle_time_ms,
                expiry_timestamp_ms,
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            ExpireDelegationTokenOutcome::BrokerRejected(ExpireDelegationTokenBrokerError {
                throttle_time_ms,
                code,
            })
        }
        CoreTerminal::Failed(failure) => {
            ExpireDelegationTokenOutcome::Failed(ExpireDelegationTokenFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ExpireDelegationTokenFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ExpireDelegationTokenFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ExpireDelegationTokenFailureKind::DriverRejected,
        CoreFailureKind::Transport => ExpireDelegationTokenFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => ExpireDelegationTokenFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ExpireDelegationTokenFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ExpireDelegationTokenFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ExpireDelegationTokenDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ExpireDelegationTokenDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ExpireDelegationTokenDeliveryStatus::PossiblySent,
    }
}
