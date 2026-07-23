//! Linear claim that makes promotion and observer drop choose one winner.

use std::sync::Arc;

use crate::ProducerDeliveryObserver;

use super::{
    PendingCellError, PendingNotificationJob, PendingSendCell, ProducerSendFailure,
    cell::PromotionRestore,
};

/// Result of returning a failed admission attempt to pending ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingPromotionRestore {
    Pending,
    Abandoned,
}

/// Unique right to finish or restore one pending-send transition.
pub(crate) struct PendingPromotion {
    cell: Arc<PendingSendCell>,
    live: bool,
}

impl PendingPromotion {
    pub(super) const fn new(cell: Arc<PendingSendCell>) -> Self {
        Self { cell, live: true }
    }

    pub(crate) fn accept(
        mut self,
        observer: ProducerDeliveryObserver,
    ) -> Result<PendingNotificationJob, ProducerDeliveryObserver> {
        let result = self.cell.accept_promotion(observer);
        self.live = false;
        result
    }

    pub(crate) fn settle_local(
        mut self,
        failure: ProducerSendFailure,
    ) -> Result<PendingNotificationJob, PendingCellError> {
        let result = self.cell.settle_promotion(failure);
        self.live = false;
        result
    }

    pub(crate) fn restore(mut self) -> Result<PendingPromotionRestore, PendingCellError> {
        let result = self.cell.restore_promotion().map(map_restore);
        self.live = false;
        result
    }
}

impl Drop for PendingPromotion {
    fn drop(&mut self) {
        if self.live {
            let _restored = self.cell.restore_promotion();
        }
    }
}

fn map_restore(restore: PromotionRestore) -> PendingPromotionRestore {
    match restore {
        PromotionRestore::Pending => PendingPromotionRestore::Pending,
        PromotionRestore::Abandoned => PendingPromotionRestore::Abandoned,
    }
}
