//! Bounded notifier-only settlement of completion slots left reserved by host failure.

use std::sync::Arc;

#[cfg(test)]
use super::NotifierJoin;
use super::{
    CompletionId, CompletionRegistry, CompletionRegistryError,
    notifier::PublishJob,
    notifier_queue::{QueuePushError, QueuePushError::Closed},
};

/// Exact progress from one bounded reserved-slot settlement pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SettlementProgress {
    queued: usize,
    remaining: usize,
}

impl SettlementProgress {
    /// Returns terminal jobs accepted by the notifier during this pass.
    pub(crate) const fn queued(self) -> usize {
        self.queued
    }

    /// Returns reserved slots still lacking a queued terminal job.
    pub(crate) const fn remaining(self) -> usize {
        self.remaining
    }
}

/// Failed notifier admission retaining the terminal value on behalf of its caller.
#[derive(Debug)]
#[must_use = "the terminal value remains owned by this failure"]
pub(crate) struct SettlementFailure<T> {
    progress: SettlementProgress,
    completion_id: CompletionId,
    error: CompletionRegistryError,
    terminal: T,
}

impl<T> SettlementFailure<T> {
    /// Returns progress committed before notifier admission failed.
    pub(crate) const fn progress(&self) -> SettlementProgress {
        self.progress
    }

    /// Returns the reserved completion whose terminal was not queued.
    pub(crate) const fn completion_id(&self) -> CompletionId {
        self.completion_id
    }

    /// Returns the exact bounded-notifier failure.
    pub(crate) const fn error(&self) -> CompletionRegistryError {
        self.error
    }

    /// Returns the terminal value that never crossed notifier ownership.
    pub(crate) fn into_terminal(self) -> T {
        self.terminal
    }
}

impl<T: Send + 'static> CompletionRegistry<T> {
    /// Queues at most `limit` caller-created terminals for unpublished reservations.
    ///
    /// Already published, reclaiming, vacant, and retired slots are skipped.
    /// Successful jobs change phase before a retry can visit them again. The
    /// notifier queue covers every registry slot, so backpressure indicates a
    /// violated construction invariant rather than a need for unbounded storage.
    pub(crate) fn settle_reserved_with<F>(
        &mut self,
        limit: usize,
        mut terminal_for: F,
    ) -> Result<SettlementProgress, SettlementFailure<T>>
    where
        F: FnMut(CompletionId) -> T,
    {
        let mut queued = 0;
        for index in 0..self.slots.len() {
            if queued == limit {
                break;
            }
            let Some(completion_id) = self.slots[index].reserved_id() else {
                continue;
            };
            let terminal = terminal_for(completion_id);
            let job = PublishJob {
                id: completion_id,
                cell: Arc::clone(&self.slots[index].cell),
                value: terminal,
            };
            let result = match &self.notifier {
                Some(notifier) => notifier.try_publish(job),
                None => Err(Closed(job)),
            };
            match result {
                Ok(()) => {
                    self.slots[index].mark_published(completion_id);
                    queued += 1;
                }
                Err(QueuePushError::Full(job)) => {
                    return Err(self.settlement_failure(
                        queued,
                        job,
                        CompletionRegistryError::NotificationBackpressure,
                    ));
                }
                Err(QueuePushError::Closed(job)) => {
                    return Err(self.settlement_failure(
                        queued,
                        job,
                        CompletionRegistryError::NotifierStopped,
                    ));
                }
            }
        }
        Ok(SettlementProgress {
            queued,
            remaining: self.reserved_count(),
        })
    }

    fn settlement_failure(
        &self,
        queued: usize,
        job: PublishJob<T>,
        error: CompletionRegistryError,
    ) -> SettlementFailure<T> {
        SettlementFailure {
            progress: SettlementProgress {
                queued,
                remaining: self.reserved_count(),
            },
            completion_id: job.id,
            error,
            terminal: job.value,
        }
    }

    fn reserved_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.reserved_id().is_some())
            .count()
    }

    #[cfg(test)]
    pub(super) fn disconnect_notifier_for_settlement_test(
        &mut self,
    ) -> Result<NotifierJoin, CompletionRegistryError> {
        let Some(notifier) = self.notifier.take() else {
            return Err(CompletionRegistryError::NotifierStopped);
        };
        Ok(notifier.stop())
    }
}
