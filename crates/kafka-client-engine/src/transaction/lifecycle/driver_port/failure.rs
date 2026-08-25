//! Exact driver-to-core transaction-end failure normalization.

use kafka_client_core::{
    DeliveryStatus, TransactionEndBrokerFailureKind, TransactionEndFailure,
    TransactionEndFailureKind, TransactionEndMode,
};

use crate::{
    driver::transaction_control::{
        TransactionControlDriverFailureKind, TransactionEndCallAdmissionFailure,
        TransactionEndCallAdmissionFailureKind, TransactionEndCompletionFailureKind,
    },
    protocol::transaction::{TransactionBrokerCategory, TransactionBrokerError},
};

pub(super) const fn broker_failure(
    mode: TransactionEndMode,
    error: TransactionBrokerError,
) -> TransactionEndFailure {
    let kind = match error.category() {
        TransactionBrokerCategory::Access => TransactionEndBrokerFailureKind::Access,
        TransactionBrokerCategory::Coordinator => TransactionEndBrokerFailureKind::Coordinator,
        TransactionBrokerCategory::Fenced => TransactionEndBrokerFailureKind::Fenced,
        TransactionBrokerCategory::Rejected => TransactionEndBrokerFailureKind::Rejected,
    };
    TransactionEndFailure::broker(mode, kind, DeliveryStatus::PossiblySent, error.code())
}

pub(super) const fn driver_failure_kind(
    kind: TransactionControlDriverFailureKind,
) -> TransactionEndFailureKind {
    match kind {
        TransactionControlDriverFailureKind::DeadlineElapsed => {
            TransactionEndFailureKind::DeadlineElapsed
        }
        TransactionControlDriverFailureKind::Compatibility => {
            TransactionEndFailureKind::Compatibility
        }
        TransactionControlDriverFailureKind::InvalidResponse => {
            TransactionEndFailureKind::InvalidResponse
        }
        TransactionControlDriverFailureKind::Transport => TransactionEndFailureKind::Transport,
    }
}

pub(super) fn submit_failure(
    mode: TransactionEndMode,
    error: &TransactionEndCallAdmissionFailure,
) -> TransactionEndFailure {
    let kind = match error.kind() {
        TransactionEndCallAdmissionFailureKind::InvalidTransactionalId
        | TransactionEndCallAdmissionFailureKind::ForeignDriver => {
            TransactionEndFailureKind::Correlation
        }
        TransactionEndCallAdmissionFailureKind::Capacity
        | TransactionEndCallAdmissionFailureKind::DriverRejected => {
            TransactionEndFailureKind::DriverRejected
        }
        TransactionEndCallAdmissionFailureKind::Closed => TransactionEndFailureKind::DriverClosed,
        TransactionEndCallAdmissionFailureKind::Wake => TransactionEndFailureKind::Transport,
        TransactionEndCallAdmissionFailureKind::Compatibility => {
            TransactionEndFailureKind::Compatibility
        }
    };
    TransactionEndFailure::local(mode, kind, DeliveryStatus::NotSent)
}

pub(super) const fn completion_failure(
    mode: TransactionEndMode,
    error: TransactionEndCompletionFailureKind,
) -> TransactionEndFailure {
    let kind = match error {
        TransactionEndCompletionFailureKind::Closed => TransactionEndFailureKind::DriverClosed,
        TransactionEndCompletionFailureKind::Consumed
        | TransactionEndCompletionFailureKind::Correlation => {
            TransactionEndFailureKind::Correlation
        }
    };
    TransactionEndFailure::local(mode, kind, DeliveryStatus::PossiblySent)
}
