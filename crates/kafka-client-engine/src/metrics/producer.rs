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
    produce_requests: u64,
    produce_batches: u64,
    produce_records: u64,
    produce_encoded_bytes: u64,
    peak_produce_in_flight_requests: usize,
    peak_produce_in_flight_requests_per_broker: usize,
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
            produce_requests: stats.host.produce_requests,
            produce_batches: stats.host.produce_batches,
            produce_records: stats.host.produce_records,
            produce_encoded_bytes: stats.host.produce_encoded_bytes,
            peak_produce_in_flight_requests: stats.host.peak_produce_in_flight_requests,
            peak_produce_in_flight_requests_per_broker: stats
                .host
                .peak_produce_in_flight_requests_per_broker,
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

    /// Returns cumulative driver-accepted Produce requests.
    pub const fn produce_requests(self) -> u64 {
        self.produce_requests
    }

    /// Returns cumulative partition batches in accepted Produce requests.
    pub const fn produce_batches(self) -> u64 {
        self.produce_batches
    }

    /// Returns cumulative records in accepted Produce requests.
    pub const fn produce_records(self) -> u64 {
        self.produce_records
    }

    /// Returns cumulative encoded record bytes in accepted Produce requests.
    pub const fn produce_encoded_bytes(self) -> u64 {
        self.produce_encoded_bytes
    }

    /// Returns the peak number of Produce requests owned by transport.
    pub const fn peak_produce_in_flight_requests(self) -> usize {
        self.peak_produce_in_flight_requests
    }

    /// Returns the peak Produce requests owned by one broker connection.
    pub const fn peak_produce_in_flight_requests_per_broker(self) -> usize {
        self.peak_produce_in_flight_requests_per_broker
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
