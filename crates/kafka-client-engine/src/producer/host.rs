//! Synchronized capacity and ownership for one explicit-partition producer host.

use std::sync::Arc;

use kafka_client_core::{ByteCount, ProducerBatchPolicy, ProducerEffect, ProducerMachine};

use crate::{clock::BatchTimers, completion::CompletionRegistry};

use super::{
    ProducerHostInvariantError, ProducerHostLimitError, ProducerHostStartError, ProducerStore,
    ProducerStoreLimits, ProducerStoreStats,
    binding::CompletionBindings,
    execution::{PreparedExecution, PreparedExecutionLimits},
    pending::PendingNotificationPermitPool,
    reclaim::CompletionReclaimer,
    terminal_backlog::{
        FatalTransitionBuffer, OrderedTerminalBacklog, TerminalPoisonSlot, TerminalQuarantine,
        TerminalRefusalOwner,
    },
};

#[path = "host_limits.rs"]
mod limits;
pub(crate) use limits::ProducerHostLimits;

/// Current agreement between deterministic accounting and engine resources.
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
    pub(crate) pending_notification_permits: usize,
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
    pub(super) pending_notification_permits: Arc<PendingNotificationPermitPool>,
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
        let (retained_bytes, notification_budget, transition_capacity) =
            limits.validate()?.into_parts();
        let terminal_quarantine =
            TerminalQuarantine::for_capacities(limits.record_capacity, limits.completion_capacity)?;
        if terminal_quarantine.transition_effect_capacity() != transition_capacity {
            return Err(ProducerHostLimitError::TerminalTailCapacityOverflow.into());
        }
        let core_config = ProducerCoreConfig {
            retained_bytes,
            completion_capacity: limits.completion_capacity,
            batch_policy: limits.batch_policy,
        };
        let core = core_config.machine();
        if core.transition_effect_capacity() != Some(transition_capacity) {
            return Err(ProducerHostLimitError::TerminalTailCapacityOverflow.into());
        }
        let notification_owners = notification_budget
            .start()
            .map_err(ProducerHostStartError::Notifier)?;
        let (completions, pending_notification_permits) = notification_owners.into_parts();
        Ok(Self {
            core,
            core_config,
            store: ProducerStore::new(ProducerStoreLimits {
                records: limits.record_capacity,
                bytes: limits.retained_bytes,
                batches: limits.batch_capacity,
            }),
            completions,
            pending_notification_permits,
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
            pending_notification_permits: self.pending_notification_permits.in_use(),
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
