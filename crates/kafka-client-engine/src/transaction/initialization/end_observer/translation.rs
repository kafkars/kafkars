//! Exact core-to-engine transaction-end terminal translation.

use kafka_client_core::{
    DeliveryStatus, TransactionEndBrokerFailureKind as CoreBrokerKind,
    TransactionEndFailure as CoreFailure, TransactionEndFailureKind as CoreFailureKind,
    TransactionEndMode, TransactionLifecycleTerminal,
};

use crate::completion::CompletionObserverError;

use super::{
    TransactionEndDeliveryStatus, TransactionEndFailure, TransactionEndFailureKind,
    TransactionEndIntent, TransactionEndObserverError, TransactionEndOutcome,
};

pub(in crate::transaction::initialization) const fn translate_terminal(
    terminal: TransactionLifecycleTerminal,
    intent: TransactionEndIntent,
) -> TransactionEndOutcome {
    match terminal {
        TransactionLifecycleTerminal::Committed => TransactionEndOutcome::Committed,
        TransactionLifecycleTerminal::Aborted => TransactionEndOutcome::Aborted,
        TransactionLifecycleTerminal::Fatal => {
            TransactionEndOutcome::Failed(TransactionEndFailure {
                kind: TransactionEndFailureKind::Lifecycle,
                intent,
                delivery: TransactionEndDeliveryStatus::NotSent,
                broker_code: None,
            })
        }
        TransactionLifecycleTerminal::Failed(failure) => {
            TransactionEndOutcome::Failed(translate_failure(failure))
        }
    }
}

const fn translate_failure(failure: CoreFailure) -> TransactionEndFailure {
    TransactionEndFailure {
        kind: failure_kind(failure.kind()),
        intent: intent(failure.mode()),
        delivery: delivery(failure.delivery()),
        broker_code: failure.broker_code(),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> TransactionEndFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => TransactionEndFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => TransactionEndFailureKind::DriverRejected,
        CoreFailureKind::Transport => TransactionEndFailureKind::Transport,
        CoreFailureKind::Compatibility => TransactionEndFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => TransactionEndFailureKind::InvalidResponse,
        CoreFailureKind::DriverClosed => TransactionEndFailureKind::DriverClosed,
        CoreFailureKind::Correlation => TransactionEndFailureKind::Correlation,
        CoreFailureKind::Broker(CoreBrokerKind::Access) => TransactionEndFailureKind::Access,
        CoreFailureKind::Broker(CoreBrokerKind::Coordinator) => {
            TransactionEndFailureKind::Coordinator
        }
        CoreFailureKind::Broker(CoreBrokerKind::Fenced) => TransactionEndFailureKind::Fenced,
        CoreFailureKind::Broker(CoreBrokerKind::Rejected) => TransactionEndFailureKind::Broker,
    }
}

const fn intent(mode: TransactionEndMode) -> TransactionEndIntent {
    match mode {
        TransactionEndMode::Commit => TransactionEndIntent::Commit,
        TransactionEndMode::Abort => TransactionEndIntent::Abort,
    }
}

const fn delivery(value: DeliveryStatus) -> TransactionEndDeliveryStatus {
    match value {
        DeliveryStatus::NotSent => TransactionEndDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => TransactionEndDeliveryStatus::PossiblySent,
    }
}

pub(super) const fn observer_error(error: CompletionObserverError) -> TransactionEndObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => TransactionEndObserverError::AlreadyObserved,
        CompletionObserverError::Stale => TransactionEndObserverError::Stale,
    }
}
