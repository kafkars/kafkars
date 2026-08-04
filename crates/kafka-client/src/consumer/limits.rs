//! Public bounded Fetch-call and delivery ownership fixed before consumer startup.

/// Independent limits for Fetch execution and retained delivery batches.
///
/// Defaults are eight Fetch calls, eight retained batches, eight MiB retained,
/// and one MiB per decoded Fetch result. Consumer startup rejects zero or
/// internally incoherent capacities and requires the per-result ceiling to
/// cover [`ConsumerFetchConfig::partition_max_bytes`](super::ConsumerFetchConfig::partition_max_bytes).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerLimits {
    in_flight_fetches: usize,
    buffered_batches: usize,
    buffered_bytes: usize,
    max_batch_bytes: usize,
}

impl ConsumerLimits {
    /// Creates one explicit consumer ownership contract.
    pub const fn new(
        in_flight_fetches: usize,
        buffered_batches: usize,
        buffered_bytes: usize,
        max_batch_bytes: usize,
    ) -> Self {
        Self {
            in_flight_fetches,
            buffered_batches,
            buffered_bytes,
            max_batch_bytes,
        }
    }

    /// Returns the maximum concurrently retained Fetch calls.
    pub const fn in_flight_fetches(self) -> usize {
        self.in_flight_fetches
    }

    /// Returns the maximum retained, not-yet-consumed delivery batches.
    pub const fn buffered_batches(self) -> usize {
        self.buffered_batches
    }

    /// Returns the cumulative application-byte capacity for retained batches.
    pub const fn buffered_bytes(self) -> usize {
        self.buffered_bytes
    }

    /// Returns the hard decoded application-byte ceiling for one Fetch result.
    pub const fn max_batch_bytes(self) -> usize {
        self.max_batch_bytes
    }

    /// Replaces concurrently retained Fetch-call capacity.
    #[must_use]
    pub const fn with_in_flight_fetches(mut self, in_flight_fetches: usize) -> Self {
        self.in_flight_fetches = in_flight_fetches;
        self
    }

    /// Replaces retained delivery-batch capacity.
    #[must_use]
    pub const fn with_buffered_batches(mut self, buffered_batches: usize) -> Self {
        self.buffered_batches = buffered_batches;
        self
    }

    /// Replaces cumulative retained application-byte capacity.
    #[must_use]
    pub const fn with_buffered_bytes(mut self, buffered_bytes: usize) -> Self {
        self.buffered_bytes = buffered_bytes;
        self
    }

    /// Replaces the hard decoded byte ceiling for one Fetch result.
    #[must_use]
    pub const fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes;
        self
    }

    pub(crate) const fn into_parts(self) -> (usize, usize, usize, usize) {
        (
            self.in_flight_fetches,
            self.buffered_batches,
            self.buffered_bytes,
            self.max_batch_bytes,
        )
    }
}

impl Default for ConsumerLimits {
    fn default() -> Self {
        Self::new(8, 8, 8 * 1024 * 1024, 1024 * 1024)
    }
}
