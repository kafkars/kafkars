//! Exhaustive core-to-engine broker-unregistration terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, UnregisterBrokerFailureKind as CoreFailureKind,
    UnregisterBrokerTerminal as CoreTerminal,
};

use super::{
    UnregisterBrokerBrokerError, UnregisterBrokerDeliveryStatus, UnregisterBrokerFailure,
    UnregisterBrokerFailureKind, UnregisterBrokerOutcome,
};
use crate::admin::unregister_broker::UnregisterBrokerResult;

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> UnregisterBrokerOutcome {
    match terminal {
        CoreTerminal::Unregistered(success) => {
            UnregisterBrokerOutcome::Unregistered(UnregisterBrokerResult {
                throttle_time_ms: success.throttle_time_ms(),
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
            UnregisterBrokerOutcome::BrokerRejected(UnregisterBrokerBrokerError {
                throttle_time_ms,
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => UnregisterBrokerOutcome::Failed(UnregisterBrokerFailure {
            kind: failure_kind(failure.kind()),
            delivery: delivery(failure.delivery()),
        }),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> UnregisterBrokerFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => UnregisterBrokerFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => UnregisterBrokerFailureKind::DriverRejected,
        CoreFailureKind::Transport => UnregisterBrokerFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => UnregisterBrokerFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => UnregisterBrokerFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => UnregisterBrokerFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> UnregisterBrokerDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => UnregisterBrokerDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => UnregisterBrokerDeliveryStatus::PossiblySent,
    }
}
