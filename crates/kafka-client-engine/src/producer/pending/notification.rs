//! Arc-only pending-cell work dispatched away from host and driver reactors.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use super::PendingNotificationDispatchAuthority;
use super::{PendingNotificationPermit, PendingSendCell};

pub(crate) struct PendingNotificationJob {
    cell: Arc<PendingSendCell>,
    permit: PendingNotificationPermit,
}

impl PendingNotificationJob {
    pub(super) const fn new(cell: Arc<PendingSendCell>, permit: PendingNotificationPermit) -> Self {
        Self { cell, permit }
    }

    /// Runs only on the completion notifier or an off-reactor recovery owner.
    pub(crate) fn dispatch_pending_notification(
        self,
        _authority: &PendingNotificationDispatchAuthority,
    ) {
        let outcome = self.cell.dispatch();
        let _ignored = catch_unwind(AssertUnwindSafe(|| drop(outcome.discarded)));
        if let Some(waker) = outcome.waker {
            let _ignored = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        }
        self.permit.release();
    }

    #[cfg(test)]
    pub(crate) const fn permit_slot_for_test(&self) -> Option<usize> {
        self.permit.slot_for_test()
    }
}
