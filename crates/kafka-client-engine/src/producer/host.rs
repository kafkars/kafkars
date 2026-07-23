//! Synchronized capacity and ownership for one explicit-partition producer host.

use kafka_client_core::{ByteCount, ProducerBatchPolicy, ProducerEffect, ProducerMachine};

use crate::{clock::BatchTimers, completion::CompletionRegistry};

use super::{
    CompletionBindings, ProducerHostInvariantError, ProducerHostLimitError, ProducerHostStartError,
    ProducerStore, ProducerStoreLimits, ProducerStoreStats, reclaim::CompletionReclaimer,
};

/// Capacity values shared by core policy and every bounded engine owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostLimits {
    pub(crate) retained_bytes: usize,
    pub(crate) completion_capacity: usize,
    pub(crate) record_capacity: usize,
    pub(crate) batch_capacity: usize,
    pub(crate) timer_capacity: usize,
    pub(crate) notification_capacity: usize,
    pub(crate) batch_policy: ProducerBatchPolicy,
}

impl ProducerHostLimits {
    pub(super) fn validate(self) -> Result<ByteCount, ProducerHostLimitError> {
        if self.retained_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroRetainedBytes);
        }
        if self.completion_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroCompletionCapacity);
        }
        if self.record_capacity != self.completion_capacity {
            return Err(ProducerHostLimitError::RecordCompletionMismatch);
        }
        if self.batch_capacity < self.record_capacity {
            return Err(ProducerHostLimitError::InsufficientBatchCapacity);
        }
        if self.timer_capacity < self.batch_capacity {
            return Err(ProducerHostLimitError::InsufficientTimerCapacity);
        }
        if self.notification_capacity < self.completion_capacity {
            return Err(ProducerHostLimitError::InsufficientNotificationCapacity);
        }
        if self.batch_policy.max_records() > self.record_capacity {
            return Err(ProducerHostLimitError::BatchRecordLimitExceedsCapacity);
        }
        let bytes = u64::try_from(self.retained_bytes)
            .map_err(|_| ProducerHostLimitError::RetainedBytesOutOfRange)?;
        Ok(ByteCount::new(bytes))
    }
}

/// Current agreement between deterministic accounting and engine resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostStats {
    pub(crate) store: ProducerStoreStats,
    pub(crate) core_retained_bytes: ByteCount,
    pub(crate) core_completion_slots: usize,
    pub(crate) active_timers: usize,
    pub(crate) pending_effects: usize,
    pub(crate) healthy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProducerHostHealth {
    Healthy,
    Poisoned(ProducerHostInvariantError),
}

/// Single engine owner of atomic producer admission and effect execution.
#[derive(Debug)]
pub(crate) struct ProducerHost {
    pub(super) core: ProducerMachine,
    pub(super) store: ProducerStore,
    pub(super) completions: CompletionRegistry<kafka_client_core::ProducerCompletion>,
    pub(super) bindings: CompletionBindings,
    pub(super) reclaimer: CompletionReclaimer,
    pub(super) timers: BatchTimers,
    pub(super) pending_effects: Vec<ProducerEffect>,
    pub(super) effect_capacity: usize,
    pub(super) health: ProducerHostHealth,
    #[cfg(test)]
    post_acceptance_fault: Option<ProducerHostInvariantError>,
}

impl ProducerHost {
    /// Builds all bounded owners from one validated capacity contract.
    pub(crate) fn new(limits: ProducerHostLimits) -> Result<Self, ProducerHostStartError> {
        let retained_bytes = limits.validate()?;
        let completions =
            CompletionRegistry::new(limits.completion_capacity, limits.notification_capacity)
                .map_err(ProducerHostStartError::Notifier)?;
        Ok(Self {
            core: ProducerMachine::with_batch_policy(
                retained_bytes,
                limits.completion_capacity,
                limits.batch_policy,
            ),
            store: ProducerStore::new(ProducerStoreLimits {
                records: limits.record_capacity,
                bytes: limits.retained_bytes,
                batches: limits.batch_capacity,
            }),
            completions,
            bindings: CompletionBindings::new(limits.completion_capacity),
            reclaimer: CompletionReclaimer::new(),
            timers: BatchTimers::new(limits.timer_capacity),
            pending_effects: Vec::with_capacity(limits.completion_capacity),
            effect_capacity: limits.completion_capacity,
            health: ProducerHostHealth::Healthy,
            #[cfg(test)]
            post_acceptance_fault: None,
        })
    }

    /// Returns bounded state without exposing lifecycle owners.
    pub(crate) fn stats(&self) -> ProducerHostStats {
        ProducerHostStats {
            store: self.store.stats(),
            core_retained_bytes: self.core.retained_bytes(),
            core_completion_slots: self.core.completion_slots(),
            active_timers: self.timers.len(),
            pending_effects: self.pending_effects.len(),
            healthy: self.health == ProducerHostHealth::Healthy,
        }
    }

    /// Returns mechanism work deliberately retained for a later vertical slice.
    pub(crate) fn pending_effects(&self) -> &[ProducerEffect] {
        &self.pending_effects
    }

    pub(super) const fn poison_reason(&self) -> Option<ProducerHostInvariantError> {
        match self.health {
            ProducerHostHealth::Healthy => None,
            ProducerHostHealth::Poisoned(error) => Some(error),
        }
    }

    pub(super) fn poison(
        &mut self,
        error: ProducerHostInvariantError,
    ) -> ProducerHostInvariantError {
        self.health = ProducerHostHealth::Poisoned(error);
        error
    }

    #[cfg(test)]
    pub(super) fn inject_post_acceptance_fault(&mut self, error: ProducerHostInvariantError) {
        self.post_acceptance_fault = Some(error);
    }

    #[cfg(test)]
    pub(super) fn take_post_acceptance_fault(&mut self) -> Option<ProducerHostInvariantError> {
        self.post_acceptance_fault.take()
    }
}
