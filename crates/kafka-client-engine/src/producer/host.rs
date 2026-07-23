//! Synchronized capacity and ownership for one explicit-partition producer host.

use kafka_client_core::{ByteCount, ProducerBatchPolicy, ProducerEffect, ProducerMachine};

use crate::{clock::BatchTimers, completion::CompletionRegistry};

use super::{
    ProducerHostInvariantError, ProducerHostLimitError, ProducerHostStartError, ProducerStore,
    ProducerStoreLimits, ProducerStoreStats,
    binding::CompletionBindings,
    execution::{PreparedExecution, PreparedExecutionLimits},
    reclaim::CompletionReclaimer,
    terminal_backlog::{
        FatalTransitionBuffer, OrderedTerminalBacklog, TerminalPoisonSlot, TerminalQuarantine,
        TerminalRefusalOwner,
    },
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostLimits {
    pub(crate) retained_bytes: usize,
    pub(crate) completion_capacity: usize,
    pub(crate) record_capacity: usize,
    pub(crate) batch_capacity: usize,
    pub(crate) timer_capacity: usize,
    pub(crate) notification_capacity: usize,
    pub(crate) encoded_byte_capacity: usize,
    pub(crate) max_wire_batch_bytes: usize,
    pub(crate) batch_policy: ProducerBatchPolicy,
}

impl ProducerHostLimits {
    pub(crate) fn validate(self) -> Result<ByteCount, ProducerHostLimitError> {
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
        if self.encoded_byte_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroEncodedByteCapacity);
        }
        if self.max_wire_batch_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroWireBatchBytes);
        }
        if self.batch_policy.max_records() > self.record_capacity {
            return Err(ProducerHostLimitError::BatchRecordLimitExceedsCapacity);
        }
        let bytes = u64::try_from(self.retained_bytes)
            .map_err(|_| ProducerHostLimitError::RetainedBytesOutOfRange)?;
        Ok(ByteCount::new(bytes))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostStats {
    pub(crate) store: ProducerStoreStats,
    pub(crate) core_retained_bytes: ByteCount,
    pub(crate) core_completion_slots: usize,
    pub(crate) active_timers: usize,
    pub(crate) prepared_batches: usize,
    pub(crate) prepared_bytes: usize,
    pub(crate) submission_deadlines: usize,
    pub(crate) completion_bindings: usize,
    pub(crate) pending_effects: usize,
    pub(crate) terminal_backlog: usize,
    pub(crate) healthy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProducerHostHealth {
    Healthy,
    Poisoned(ProducerHostInvariantError),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ProducerCoreConfig {
    retained_bytes: ByteCount,
    completion_capacity: usize,
    batch_policy: ProducerBatchPolicy,
}

impl ProducerCoreConfig {
    pub(super) const fn machine(self) -> ProducerMachine {
        ProducerMachine::with_batch_policy(
            self.retained_bytes,
            self.completion_capacity,
            self.batch_policy,
        )
    }
}

#[derive(Debug)]
pub(crate) struct ProducerHost {
    pub(super) core: ProducerMachine,
    pub(super) core_config: ProducerCoreConfig,
    pub(super) store: ProducerStore,
    pub(super) completions: CompletionRegistry<kafka_client_core::ProducerCompletion>,
    pub(super) bindings: CompletionBindings,
    pub(super) reclaimer: CompletionReclaimer,
    pub(super) timers: BatchTimers,
    pub(super) execution: PreparedExecution,
    pub(super) pending_effects: Vec<ProducerEffect>,
    pub(super) terminal_backlog: OrderedTerminalBacklog,
    pub(super) terminal_poison: TerminalPoisonSlot,
    pub(super) terminal_quarantine: TerminalQuarantine,
    pub(super) terminal_refusals: TerminalRefusalOwner,
    pub(super) fatal_transition: FatalTransitionBuffer,
    pub(super) effect_capacity: usize,
    pub(super) health: ProducerHostHealth,
    #[cfg(test)]
    pub(super) terminal_publish_faults:
        std::collections::VecDeque<crate::completion::CompletionRegistryError>,
    #[cfg(test)]
    pub(super) terminal_publish_attempts: usize,
    #[cfg(test)]
    post_acceptance_fault: Option<ProducerHostInvariantError>,
    #[cfg(test)]
    pub(super) terminal_interpretation_fault: bool,
    #[cfg(test)]
    pub(super) terminal_planning_fault: bool,
}

impl ProducerHost {
    pub(crate) fn new(limits: ProducerHostLimits) -> Result<Self, ProducerHostStartError> {
        let retained_bytes = limits.validate()?;
        let terminal_quarantine =
            TerminalQuarantine::for_capacities(limits.record_capacity, limits.completion_capacity)?;
        let transition_capacity = terminal_quarantine.transition_effect_capacity();
        let completions =
            CompletionRegistry::new(limits.completion_capacity, limits.notification_capacity)
                .map_err(ProducerHostStartError::Notifier)?;
        let core_config = ProducerCoreConfig {
            retained_bytes,
            completion_capacity: limits.completion_capacity,
            batch_policy: limits.batch_policy,
        };
        let core = core_config.machine();
        if core.transition_effect_capacity() != Some(transition_capacity) {
            return Err(ProducerHostLimitError::TerminalTailCapacityOverflow.into());
        }
        Ok(Self {
            core,
            core_config,
            store: ProducerStore::new(ProducerStoreLimits {
                records: limits.record_capacity,
                bytes: limits.retained_bytes,
                batches: limits.batch_capacity,
            }),
            completions,
            bindings: CompletionBindings::new(limits.completion_capacity),
            reclaimer: CompletionReclaimer::new(),
            timers: BatchTimers::new(limits.timer_capacity),
            execution: PreparedExecution::new(
                limits.batch_capacity,
                PreparedExecutionLimits {
                    encoded_bytes: limits.encoded_byte_capacity,
                    max_batch_bytes: limits.max_wire_batch_bytes,
                },
            ),
            pending_effects: Vec::with_capacity(limits.completion_capacity),
            terminal_backlog: OrderedTerminalBacklog::new(limits.completion_capacity),
            terminal_poison: TerminalPoisonSlot::empty(),
            terminal_quarantine,
            terminal_refusals: TerminalRefusalOwner::empty(),
            fatal_transition: FatalTransitionBuffer::new(transition_capacity),
            effect_capacity: limits.completion_capacity,
            health: ProducerHostHealth::Healthy,
            #[cfg(test)]
            terminal_publish_faults: std::collections::VecDeque::new(),
            #[cfg(test)]
            terminal_publish_attempts: 0,
            #[cfg(test)]
            post_acceptance_fault: None,
            #[cfg(test)]
            terminal_interpretation_fault: false,
            #[cfg(test)]
            terminal_planning_fault: false,
        })
    }

    pub(crate) fn stats(&self) -> ProducerHostStats {
        let prepared = self.execution.prepared_stats();
        ProducerHostStats {
            store: self.store.stats(),
            core_retained_bytes: self.core.retained_bytes(),
            core_completion_slots: self.core.completion_slots(),
            active_timers: self.timers.len(),
            prepared_batches: prepared.batches,
            prepared_bytes: prepared.encoded_record_bytes,
            submission_deadlines: self.execution.submission_count(),
            completion_bindings: self.bindings.len(),
            pending_effects: self.pending_effects.len(),
            terminal_backlog: self.terminal_backlog.len(),
            healthy: self.health == ProducerHostHealth::Healthy,
        }
    }
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
        match self.health {
            ProducerHostHealth::Healthy => {
                self.health = ProducerHostHealth::Poisoned(error);
                error
            }
            ProducerHostHealth::Poisoned(first) => first,
        }
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
