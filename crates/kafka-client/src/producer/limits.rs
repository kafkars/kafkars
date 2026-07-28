//! Public active, waiting, and batching limits fixed before client startup.

use std::time::Duration;

/// Independent bounds for active records and callers waiting on capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerLimits {
    retained_bytes: usize,
    in_flight_records: usize,
    waiting_records: usize,
    waiting_bytes: usize,
    batch_records: usize,
    batch_bytes: usize,
    linger: Duration,
}

impl ProducerLimits {
    /// Creates an explicit producer ownership contract.
    pub const fn new(
        retained_bytes: usize,
        in_flight_records: usize,
        waiting_records: usize,
        waiting_bytes: usize,
        batch_records: usize,
        batch_bytes: usize,
        linger: Duration,
    ) -> Self {
        Self {
            retained_bytes,
            in_flight_records,
            waiting_records,
            waiting_bytes,
            batch_records,
            batch_bytes,
            linger,
        }
    }

    pub(crate) const fn into_parts(self) -> (usize, usize, usize, usize, usize, usize, Duration) {
        (
            self.retained_bytes,
            self.in_flight_records,
            self.waiting_records,
            self.waiting_bytes,
            self.batch_records,
            self.batch_bytes,
            self.linger,
        )
    }

    /// Returns active application-byte capacity.
    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// Returns active record and completion capacity.
    pub const fn in_flight_records(self) -> usize {
        self.in_flight_records
    }

    /// Returns callers retained in deterministic waiting order.
    pub const fn waiting_records(self) -> usize {
        self.waiting_records
    }

    /// Returns application bytes retained before active admission.
    pub const fn waiting_bytes(self) -> usize {
        self.waiting_bytes
    }

    /// Returns the maximum records retained in one accumulator.
    pub const fn batch_records(self) -> usize {
        self.batch_records
    }

    /// Returns the maximum encoded bytes retained for one Produce batch.
    pub const fn batch_bytes(self) -> usize {
        self.batch_bytes
    }

    /// Returns the engine-owned maximum batching delay.
    pub const fn linger(self) -> Duration {
        self.linger
    }

    /// Replaces active application-byte capacity.
    #[must_use]
    pub const fn with_retained_bytes(mut self, retained_bytes: usize) -> Self {
        self.retained_bytes = retained_bytes;
        self
    }

    /// Replaces active record and completion capacity.
    #[must_use]
    pub const fn with_in_flight_records(mut self, in_flight_records: usize) -> Self {
        self.in_flight_records = in_flight_records;
        self
    }

    /// Replaces deterministic waiting caller capacity.
    #[must_use]
    pub const fn with_waiting_records(mut self, waiting_records: usize) -> Self {
        self.waiting_records = waiting_records;
        self
    }

    /// Replaces application-byte capacity before active admission.
    #[must_use]
    pub const fn with_waiting_bytes(mut self, waiting_bytes: usize) -> Self {
        self.waiting_bytes = waiting_bytes;
        self
    }

    /// Replaces maximum records retained in one accumulator.
    #[must_use]
    pub const fn with_batch_records(mut self, batch_records: usize) -> Self {
        self.batch_records = batch_records;
        self
    }

    /// Replaces maximum encoded bytes retained for one Produce batch.
    #[must_use]
    pub const fn with_batch_bytes(mut self, batch_bytes: usize) -> Self {
        self.batch_bytes = batch_bytes;
        self
    }

    /// Replaces the engine-owned maximum batching delay.
    #[must_use]
    pub const fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }
}

impl Default for ProducerLimits {
    fn default() -> Self {
        Self::new(
            32 * 1024 * 1024,
            1_024,
            1_024,
            32 * 1024 * 1024,
            256,
            1024 * 1024,
            Duration::from_millis(5),
        )
    }
}
