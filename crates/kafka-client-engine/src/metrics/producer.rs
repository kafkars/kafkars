//! Fixed-size projection of producer ownership captured at metrics admission.

use crate::producer::ingress::ProducerShardStats;

/// One bounded view of producer pressure and lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineProducerMetrics {
    active_records: usize,
    active_bytes: usize,
    waiting_records: usize,
    waiting_bytes: usize,
    prepared_batches: usize,
    prepared_batch_bytes: usize,
    terminal_backlog: usize,
    accepting: bool,
    healthy: bool,
}

impl EngineProducerMetrics {
    pub(crate) fn from_shard(stats: ProducerShardStats) -> Self {
        let waiting_bytes = usize::try_from(stats.host.waiting.bytes.get()).unwrap_or(usize::MAX);
        Self {
            active_records: stats.host.store.records,
            active_bytes: stats.host.store.bytes,
            waiting_records: stats.host.waiting.records,
            waiting_bytes,
            prepared_batches: stats.host.prepared_batches,
            prepared_batch_bytes: stats.host.prepared_bytes,
            terminal_backlog: stats.host.terminal_backlog,
            accepting: stats.accepting,
            healthy: stats.host.healthy,
        }
    }

    /// Returns application records retained by active producer ownership.
    pub const fn active_records(self) -> usize {
        self.active_records
    }

    /// Returns application bytes retained by active producer ownership.
    pub const fn active_bytes(self) -> usize {
        self.active_bytes
    }

    /// Returns records retained in bounded FIFO waiting ownership.
    pub const fn waiting_records(self) -> usize {
        self.waiting_records
    }

    /// Returns application bytes retained in bounded FIFO waiting ownership.
    pub const fn waiting_bytes(self) -> usize {
        self.waiting_bytes
    }

    /// Returns protocol-materialized batches not yet released by execution.
    pub const fn prepared_batches(self) -> usize {
        self.prepared_batches
    }

    /// Returns encoded record-batch bytes retained by prepared execution.
    pub const fn prepared_batch_bytes(self) -> usize {
        self.prepared_batch_bytes
    }

    /// Returns terminal decisions awaiting completion publication.
    pub const fn terminal_backlog(self) -> usize {
        self.terminal_backlog
    }

    /// Reports whether the producer was accepting records at this boundary.
    pub const fn accepting(self) -> bool {
        self.accepting
    }

    /// Reports whether the producer host retained healthy ownership.
    pub const fn healthy(self) -> bool {
        self.healthy
    }
}
