//! Typed terminal publication ownership crossing one notifier queue.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use super::{CompletionId, cell::CompletionCell};

/// One typed terminal value and its exact observer cell generation.
pub(crate) struct PublishTicket<T> {
    pub(super) id: CompletionId,
    pub(super) cell: Arc<CompletionCell<T>>,
    pub(super) value: T,
}

impl<T> PublishTicket<T> {
    pub(super) const fn new(id: CompletionId, cell: Arc<CompletionCell<T>>, value: T) -> Self {
        Self { id, cell, value }
    }

    /// Stores and wakes one terminal away from the engine reactor.
    pub(crate) fn publish(self) {
        let outcome = self.cell.store_terminal(self.id, self.value);
        let _ignored = catch_unwind(AssertUnwindSafe(|| drop(outcome.discarded)));
        if outcome.reclaim_after_drop {
            self.cell.queue_reclaim(self.id);
        }
        if let Some(waker) = outcome.waker {
            let _ignored = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        }
    }
}
