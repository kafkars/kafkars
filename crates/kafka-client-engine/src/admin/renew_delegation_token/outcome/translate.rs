//! Exhaustive core-to-engine token-renewal terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, RenewDelegationTokenFailureKind as CoreFailureKind,
    RenewDelegationTokenTerminal as CoreTerminal,
};

use super::{
    RenewDelegationTokenBrokerError, RenewDelegationTokenDeliveryStatus,
    RenewDelegationTokenFailure, RenewDelegationTokenFailureKind, RenewDelegationTokenOutcome,
};
use crate::admin::renew_delegation_token::RenewDelegationTokenResult;

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> RenewDelegationTokenOutcome {
    match terminal {
        CoreTerminal::Renewed(success) => {
            let (throttle_time_ms, expiry_timestamp_ms) = success.into_parts();
            RenewDelegationTokenOutcome::Renewed(RenewDelegationTokenResult {
                throttle_time_ms,
                expiry_timestamp_ms,
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            RenewDelegationTokenOutcome::BrokerRejected(RenewDelegationTokenBrokerError {
                throttle_time_ms,
                code,
            })
        }
        CoreTerminal::Failed(failure) => {
            RenewDelegationTokenOutcome::Failed(RenewDelegationTokenFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> RenewDelegationTokenFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => RenewDelegationTokenFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => RenewDelegationTokenFailureKind::DriverRejected,
        CoreFailureKind::Transport => RenewDelegationTokenFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => RenewDelegationTokenFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => RenewDelegationTokenFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => RenewDelegationTokenFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> RenewDelegationTokenDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => RenewDelegationTokenDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => RenewDelegationTokenDeliveryStatus::PossiblySent,
    }
}
