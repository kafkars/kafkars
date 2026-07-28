//! Synchronized capacity and ownership for one explicit-partition producer host.

mod config;
mod driver_input;
#[cfg(test)]
mod driver_input_test;
mod observation;
#[cfg(test)]
mod test_support;
use kafka_client_core::{ByteCount, ProducerEffect, ProducerMachine, ProducerWaitingQueue};

use crate::{clock::BatchTimers, completion::CompletionRegistry};

use super::{
    ProducerHostInvariantError, ProducerHostStartError, ProducerStore, ProducerStoreLimits,
    ProducerStoreStats,
    binding::OperationBindings,
    compression::{CompressionWorkerLimits, CompressionWorkers, SilentCompressionWake},
    execution::{PreparedExecution, PreparedExecutionLimits},
    flush::FlushBindings,
    reclaim::CompletionReclaimer,
    terminal::ProducerTerminal,
    terminal_backlog::OrderedTerminalBacklog,
    waiting::{ProducerWaitingStats, model::ProducerWaitingStore},
};
use config::ProducerCoreConfig;

#[path = "host_limits.rs"]
mod limits;
pub(crate) use limits::ProducerHostLimits;

/// Current agreement between deterministic accounting and engine resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostStats {
    pub(crate) store: ProducerStoreStats,
    pub(crate) core_retained_bytes: ByteCount,
    pub(crate) core_completion_slots: usize,
    pub(crate) core_flush_slots: usize,
    pub(crate) active_timers: usize,
    pub(crate) prepared_batches: usize,
    pub(crate) prepared_bytes: usize,
    pub(crate) submission_deadlines: usize,
    pub(crate) completion_bindings: usize,
    pub(crate) flush_completion_bindings: usize,
    pub(crate) pending_effects: usize,
    pub(crate) compression_jobs: usize,
    pub(crate) compression_bytes: usize,
    pub(crate) terminal_backlog: usize,
    pub(crate) waiting: ProducerWaitingStats,
    pub(crate) healthy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProducerHostHealth {
    Healthy,
    Poisoned(ProducerHostInvariantError),
}

#[derive(Debug)]
pub(crate) struct ProducerHost {
    pub(super) core: ProducerMachine,
    pub(super) core_config: ProducerCoreConfig,
    pub(super) store: ProducerStore,
    pub(super) completions: CompletionRegistry<ProducerTerminal>,
    pub(super) bindings: OperationBindings,
    pub(super) flush_bindings: FlushBindings,
    pub(super) reclaimer: CompletionReclaimer,
    pub(super) timers: BatchTimers,
    pub(super) execution: PreparedExecution,
    pub(super) compression: CompressionWorkers,
    pub(super) compression_saturated: bool,
    pub(super) pending_effects: Vec<ProducerEffect>,
    pub(super) terminal_backlog: OrderedTerminalBacklog,
    pub(super) waiting_policy: ProducerWaitingQueue,
    pub(super) waiting: ProducerWaitingStore,
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
        Self::new_with_compression_wake(limits, &std::sync::Arc::new(SilentCompressionWake))
    }

    pub(crate) fn new_with_compression_wake<W>(
        limits: ProducerHostLimits,
        wake: &std::sync::Arc<W>,
    ) -> Result<Self, ProducerHostStartError>
    where
        W: super::ingress::ProducerShardWake,
    {
        let validated = limits.validate()?;
        let retained_bytes = validated.retained_bytes();
        let waiting_bytes = validated.waiting_bytes();
        let total_completion_capacity = validated.total_completion_capacity();
        let total_retained_bytes = validated.total_retained_bytes();
        let core_config = ProducerCoreConfig {
            retained_bytes,
            completion_capacity: total_completion_capacity,
            flush_capacity: limits.completion_capacity,
            batch_policy: limits.batch_policy,
            retry_policy: limits.retry_policy,
            compression: limits.compression,
        };
        let core = core_config.machine();
        let compression = CompressionWorkers::start(
            CompressionWorkerLimits {
                workers: limits.compression_worker_count,
                jobs: limits.compression_job_capacity,
                bytes: limits.compression_byte_capacity,
            },
            wake,
        )
        .map_err(ProducerHostStartError::Compression)?;
        let completions = CompletionRegistry::start(total_completion_capacity)
            .map_err(ProducerHostStartError::Notifier)?;
        Ok(Self {
            core,
            core_config,
            store: ProducerStore::new_with_topic_limits(
                ProducerStoreLimits {
                    records: limits.record_capacity,
                    bytes: limits.retained_bytes,
                    batches: limits.batch_capacity,
                },
                total_completion_capacity,
                total_retained_bytes,
            ),
            completions,
            bindings: OperationBindings::new(total_completion_capacity),
            flush_bindings: FlushBindings::new(limits.completion_capacity),
            reclaimer: CompletionReclaimer::new(),
            timers: BatchTimers::new(limits.timer_capacity),
            execution: PreparedExecution::new(
                limits.batch_capacity,
                PreparedExecutionLimits {
                    encoded_bytes: limits.encoded_byte_capacity,
                    max_batch_bytes: limits.max_wire_batch_bytes,
                },
            ),
            compression,
            compression_saturated: false,
            pending_effects: Vec::with_capacity(total_completion_capacity),
            terminal_backlog: OrderedTerminalBacklog::new(total_completion_capacity),
            waiting_policy: ProducerWaitingQueue::new(
                limits.waiting_record_capacity,
                waiting_bytes,
            ),
            waiting: ProducerWaitingStore::new(limits.waiting_record_capacity),
            effect_capacity: total_completion_capacity,
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
            core_flush_slots: self.core.flush_slots(),
            active_timers: self.timers.len(),
            prepared_batches: prepared.batches,
            prepared_bytes: prepared.encoded_record_bytes,
            submission_deadlines: self.execution.submission_count(),
            completion_bindings: self.bindings.len(),
            flush_completion_bindings: self.flush_bindings.len(),
            pending_effects: self.pending_effects.len(),
            compression_jobs: self.compression.retained_jobs(),
            compression_bytes: self.compression.retained_bytes(),
            terminal_backlog: self.terminal_backlog.len(),
            waiting: self.waiting_stats(),
            healthy: self.health == ProducerHostHealth::Healthy,
        }
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
}
