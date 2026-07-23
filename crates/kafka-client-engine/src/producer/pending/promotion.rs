//! Private cell claim resolved only through its coordinated promotion attempt.

use std::sync::Arc;

use crate::ProducerDeliveryObserver;

use super::{
    PendingCellError, PendingNotificationJob, PendingSendCell, ProducerSendFailure,
    cell::PromotionRestore,
};

/// Unique right to resolve one cell that remains in `Promoting`.
#[derive(Debug)]
pub(super) struct PendingPromotion {
    cell: Arc<PendingSendCell>,
}

impl PendingPromotion {
    pub(super) const fn new(cell: Arc<PendingSendCell>) -> Self {
        Self { cell }
    }

    pub(in crate::producer::pending) fn accept(
        self,
        observer: ProducerDeliveryObserver,
    ) -> Result<PendingNotificationJob, (Self, ProducerDeliveryObserver)> {
        match self.cell.accept_promotion(observer) {
            Ok(job) => Ok(job),
            Err(observer) => Err((self, observer)),
        }
    }

    pub(in crate::producer::pending) fn settle_local(
        self,
        failure: ProducerSendFailure,
    ) -> Result<PendingNotificationJob, (Self, PendingCellError)> {
        match self.cell.settle_promotion(failure) {
            Ok(job) => Ok(job),
            Err(error) => Err((self, error)),
        }
    }

    pub(in crate::producer::pending) fn restore(
        self,
    ) -> Result<PromotionRestore, (Self, PendingCellError)> {
        match self.cell.restore_promotion() {
            Ok(restore) => Ok(restore),
            Err(error) => Err((self, error)),
        }
    }
}
