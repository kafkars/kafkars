//! Stable terminal values and unique retained transactional-owner handle.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{SyncSender, TrySendError},
};

use kafka_client_core::{
    DeliveryStatus, TransactionInitializationBrokerCategory,
    TransactionInitializationFailureKind as CoreFailureKind, TransactionInitializationTerminal,
    TransactionalOwnerId,
};

use super::{
    RetainedTransactionInitializationOutcome, TransactionInitializationHostError,
    TransactionInitializationObserver,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationAcceptedFaultKind {
    Wake,
    HostInvariant,
}

#[must_use = "accepted initialization retains its sole terminal observer"]
pub(crate) struct TransactionInitializationAccepted {
    pub(super) observer: TransactionInitializationObserver,
    pub(super) fault: Option<TransactionInitializationAcceptedFaultKind>,
}

impl TransactionInitializationAccepted {
    pub(crate) const fn fault(&self) -> Option<TransactionInitializationAcceptedFaultKind> {
        self.fault
    }

    pub(crate) fn into_observer(self) -> TransactionInitializationObserver {
        self.observer
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationDeliveryStatus {
    NotSent,
    PossiblySent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationFailureKind {
    DeadlineElapsed,
    DriverRejected,
    Transport,
    Broker { code: i16, fenced: bool },
    InvalidResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionInitializationFailure {
    pub(crate) kind: TransactionInitializationFailureKind,
    pub(crate) delivery: TransactionInitializationDeliveryStatus,
}

pub(crate) enum TransactionInitializationOutcome {
    Initialized(TransactionalOwnerHandle),
    Failed(TransactionInitializationFailure),
}

/// Unique idle transactional owner; close and drop both release host ownership.
#[must_use = "the transactional owner remains fenced until closed or dropped"]
pub(crate) struct TransactionalOwnerHandle {
    owner_id: TransactionalOwnerId,
    transactional_id: Option<String>,
    producer_id: i64,
    producer_epoch: i16,
    active: Arc<AtomicBool>,
    release: SyncSender<TransactionalOwnerId>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl TransactionalOwnerHandle {
    pub(super) const fn new(
        owner_id: TransactionalOwnerId,
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        active: Arc<AtomicBool>,
        release: SyncSender<TransactionalOwnerId>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            owner_id,
            transactional_id: Some(transactional_id),
            producer_id,
            producer_epoch,
            active,
            release,
            _lifetime: lifetime,
        }
    }

    pub(crate) fn transactional_id(&self) -> &str {
        self.transactional_id.as_deref().unwrap_or("")
    }

    pub(crate) const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    pub(crate) const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn close(mut self) {
        self.release();
    }

    fn release(&mut self) {
        let Some(transactional_id) = self.transactional_id.take() else {
            return;
        };
        drop(transactional_id);
        release_owner(self.owner_id, &self.active, &self.release);
    }
}

impl Drop for TransactionalOwnerHandle {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) fn release_owner(
    owner_id: TransactionalOwnerId,
    active: &AtomicBool,
    release: &SyncSender<TransactionalOwnerId>,
) {
    active.store(false, Ordering::Release);
    match release.try_send(owner_id) {
        Ok(()) | Err(TrySendError::Disconnected(_)) => {}
        Err(TrySendError::Full(_)) => {
            debug_assert!(false, "owner-release capacity equals owner capacity");
        }
    }
}

pub(super) fn failed_retained_outcome(
    terminal: TransactionInitializationTerminal,
) -> Option<RetainedTransactionInitializationOutcome> {
    let TransactionInitializationTerminal::Failed(failure) = terminal else {
        return None;
    };
    let kind = match failure.kind() {
        CoreFailureKind::DeadlineElapsed => {
            self::TransactionInitializationFailureKind::DeadlineElapsed
        }
        CoreFailureKind::DriverRejected => {
            self::TransactionInitializationFailureKind::DriverRejected
        }
        CoreFailureKind::Transport => self::TransactionInitializationFailureKind::Transport,
        CoreFailureKind::Broker(broker) => self::TransactionInitializationFailureKind::Broker {
            code: broker.code(),
            fenced: broker.category() == TransactionInitializationBrokerCategory::Fenced,
        },
        CoreFailureKind::InvalidResponse => {
            self::TransactionInitializationFailureKind::InvalidResponse
        }
    };
    Some(RetainedTransactionInitializationOutcome::Failed(
        TransactionInitializationFailure {
            kind,
            delivery: delivery(failure.delivery()),
        },
    ))
}

pub(super) const fn accepted_fault(
    error: TransactionInitializationHostError,
) -> TransactionInitializationAcceptedFaultKind {
    match error {
        TransactionInitializationHostError::Wake => {
            TransactionInitializationAcceptedFaultKind::Wake
        }
        _ => TransactionInitializationAcceptedFaultKind::HostInvariant,
    }
}

const fn delivery(status: DeliveryStatus) -> TransactionInitializationDeliveryStatus {
    match status {
        DeliveryStatus::NotSent => TransactionInitializationDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => TransactionInitializationDeliveryStatus::PossiblySent,
    }
}
