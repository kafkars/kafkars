//! Arc-only pending-cell work dispatched away from host and driver reactors.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use super::PendingSendCell;

pub(crate) struct PendingNotificationJob {
    cell: Arc<PendingSendCell>,
}

impl PendingNotificationJob {
    pub(super) const fn new(cell: Arc<PendingSendCell>) -> Self {
        Self { cell }
    }

    /// Runs only on the completion notifier or an off-reactor recovery owner.
    pub(crate) fn dispatch(self) {
        let outcome = self.cell.dispatch();
        let _ignored = catch_unwind(AssertUnwindSafe(|| drop(outcome.discarded)));
        if let Some(waker) = outcome.waker {
            let _ignored = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        }
    }
}
