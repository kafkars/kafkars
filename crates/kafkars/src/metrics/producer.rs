//! Public fixed-size view of producer pressure and lifecycle state.

use crate::bridge::client::metrics::ClientProducerMetrics;

/// Producer ownership captured synchronously at `Client::metrics` admission.
///
/// Broker-call fields in the enclosing snapshot are captured later by the
/// driver reactor and are not an atomic cross-owner view with these fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerMetrics {
    inner: ClientProducerMetrics,
}

impl ProducerMetrics {
    pub(crate) const fn from_bridge(inner: ClientProducerMetrics) -> Self {
        Self { inner }
    }

    /// Returns application records retained by active producer ownership.
    pub const fn active_records(self) -> usize {
        self.inner.active_records()
    }

    /// Returns application bytes retained by active producer ownership.
    pub const fn active_bytes(self) -> usize {
        self.inner.active_bytes()
    }

    /// Returns records retained in bounded FIFO waiting ownership.
    pub const fn waiting_records(self) -> usize {
        self.inner.waiting_records()
    }

    /// Returns application bytes retained in bounded FIFO waiting ownership.
    pub const fn waiting_bytes(self) -> usize {
        self.inner.waiting_bytes()
    }

    /// Returns protocol-materialized batches not yet released by execution.
    pub const fn prepared_batches(self) -> usize {
        self.inner.prepared_batches()
    }

    /// Returns encoded record-batch bytes retained by prepared execution.
    pub const fn prepared_batch_bytes(self) -> usize {
        self.inner.prepared_batch_bytes()
    }

    /// Returns terminal decisions awaiting completion publication.
    pub const fn terminal_backlog(self) -> usize {
        self.inner.terminal_backlog()
    }

    /// Returns cumulative driver-accepted Produce requests.
    pub const fn produce_requests(self) -> u64 {
        self.inner.produce_requests()
    }

    /// Returns cumulative partition batches in accepted Produce requests.
    pub const fn produce_batches(self) -> u64 {
        self.inner.produce_batches()
    }

    /// Returns cumulative records in accepted Produce requests.
    pub const fn produce_records(self) -> u64 {
        self.inner.produce_records()
    }

    /// Returns cumulative encoded record bytes in accepted Produce requests.
    pub const fn produce_encoded_bytes(self) -> u64 {
        self.inner.produce_encoded_bytes()
    }

    /// Returns the peak number of Produce requests owned by transport.
    pub const fn peak_produce_in_flight_requests(self) -> usize {
        self.inner.peak_produce_in_flight_requests()
    }

    /// Returns the peak Produce requests owned by one broker connection.
    pub const fn peak_produce_in_flight_requests_per_broker(self) -> usize {
        self.inner.peak_produce_in_flight_requests_per_broker()
    }

    /// Reports whether the producer was accepting records at this boundary.
    pub const fn accepting(self) -> bool {
        self.inner.accepting()
    }

    /// Reports whether the producer host retained healthy ownership.
    pub const fn healthy(self) -> bool {
        self.inner.healthy()
    }
}
