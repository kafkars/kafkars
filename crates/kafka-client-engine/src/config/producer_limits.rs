//! Public, explicit producer resource capacities before host compilation.

use std::time::Duration;

const DEFAULT_RETAINED_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_IN_FLIGHT_RECORDS: usize = 1_024;
const DEFAULT_WAITING_RECORDS: usize = 1_024;
const DEFAULT_WAITING_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_BATCH_RECORDS: usize = 256;
const DEFAULT_BATCH_BYTES: usize = 1024 * 1024;
const DEFAULT_REQUEST_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER: usize = 5;
const DEFAULT_LINGER: Duration = Duration::from_millis(5);

/// Provisional bounded producer resources owned by the engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineProducerLimits {
    retained_bytes: usize,
    in_flight_records: usize,
    waiting_records: usize,
    waiting_bytes: usize,
    batch_records: usize,
    batch_bytes: usize,
    request_bytes: usize,
    max_in_flight_requests_per_broker: usize,
    linger: Duration,
}

impl EngineProducerLimits {
    /// Creates accepted-record and bounded batch capacities.
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
            request_bytes: DEFAULT_REQUEST_BYTES,
            max_in_flight_requests_per_broker: DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER,
            linger,
        }
    }

    /// Returns the application-byte ceiling for accepted records.
    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// Returns the accepted record and terminal-completion capacity.
    pub const fn in_flight_records(self) -> usize {
        self.in_flight_records
    }

    /// Returns the independent FIFO caller count available to `send`.
    pub const fn waiting_records(self) -> usize {
        self.waiting_records
    }

    /// Returns bytes owned for callers waiting before active admission.
    pub const fn waiting_bytes(self) -> usize {
        self.waiting_bytes
    }

    /// Returns the maximum records in one accumulator.
    pub const fn batch_records(self) -> usize {
        self.batch_records
    }

    /// Returns the maximum encoded bytes retained for one Produce batch.
    pub const fn batch_bytes(self) -> usize {
        self.batch_bytes
    }

    /// Returns the maximum encoded record bytes grouped into one Produce request.
    pub const fn request_bytes(self) -> usize {
        self.request_bytes
    }

    /// Returns the idempotent Produce request limit for each broker connection.
    pub const fn max_in_flight_requests_per_broker(self) -> usize {
        self.max_in_flight_requests_per_broker
    }

    /// Returns the engine-owned linger duration.
    pub const fn linger(self) -> Duration {
        self.linger
    }

    /// Replaces the maximum encoded record bytes grouped into one Produce request.
    #[must_use]
    pub const fn with_request_bytes(mut self, request_bytes: usize) -> Self {
        self.request_bytes = request_bytes;
        self
    }

    /// Replaces the per-broker idempotent request limit, which must be `1..=5`.
    #[must_use]
    pub const fn with_max_in_flight_requests_per_broker(
        mut self,
        max_in_flight_requests_per_broker: usize,
    ) -> Self {
        self.max_in_flight_requests_per_broker = max_in_flight_requests_per_broker;
        self
    }
}

impl Default for EngineProducerLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_RETAINED_BYTES,
            DEFAULT_IN_FLIGHT_RECORDS,
            DEFAULT_WAITING_RECORDS,
            DEFAULT_WAITING_BYTES,
            DEFAULT_BATCH_RECORDS,
            DEFAULT_BATCH_BYTES,
            DEFAULT_LINGER,
        )
    }
}
