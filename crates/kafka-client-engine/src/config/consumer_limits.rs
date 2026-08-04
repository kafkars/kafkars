//! Public raw consumer capacities and their validated engine representation.

/// Bounded Fetch-call and retained-delivery capacities.
///
/// Defaults are eight calls, eight batches, eight MiB retained, and one MiB
/// for one decoded Fetch result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineConsumerLimits {
    in_flight_fetches: usize,
    buffered_batches: usize,
    buffered_bytes: usize,
    max_batch_bytes: usize,
}

impl EngineConsumerLimits {
    /// Creates raw consumer capacities for engine validation.
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

    /// Returns concurrently retained Fetch-call capacity.
    pub const fn in_flight_fetches(self) -> usize {
        self.in_flight_fetches
    }

    /// Returns retained delivery-batch capacity.
    pub const fn buffered_batches(self) -> usize {
        self.buffered_batches
    }

    /// Returns cumulative retained application-byte capacity.
    pub const fn buffered_bytes(self) -> usize {
        self.buffered_bytes
    }

    /// Returns the hard decoded byte ceiling for one Fetch result.
    pub const fn max_batch_bytes(self) -> usize {
        self.max_batch_bytes
    }

    pub(crate) const fn validate(self) -> Result<ValidatedConsumerLimits, ConsumerLimitsError> {
        if self.in_flight_fetches == 0 {
            return Err(ConsumerLimitsError::InFlightFetches);
        }
        if self.buffered_batches == 0 {
            return Err(ConsumerLimitsError::BufferedBatches);
        }
        if self.buffered_bytes == 0 {
            return Err(ConsumerLimitsError::BufferedBytes);
        }
        if self.max_batch_bytes == 0 {
            return Err(ConsumerLimitsError::MaxBatchBytes);
        }
        if self.max_batch_bytes > self.buffered_bytes {
            return Err(ConsumerLimitsError::MaxBatchExceedsBufferedBytes);
        }
        Ok(ValidatedConsumerLimits {
            in_flight_fetches: self.in_flight_fetches,
            buffered_batches: self.buffered_batches,
            buffered_bytes: self.buffered_bytes,
            max_batch_bytes: self.max_batch_bytes,
        })
    }
}

impl Default for EngineConsumerLimits {
    fn default() -> Self {
        Self::new(8, 8, 8 * 1024 * 1024, 1024 * 1024)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedConsumerLimits {
    in_flight_fetches: usize,
    buffered_batches: usize,
    buffered_bytes: usize,
    max_batch_bytes: usize,
}

impl ValidatedConsumerLimits {
    pub(crate) const fn in_flight_fetches(self) -> usize {
        self.in_flight_fetches
    }

    pub(crate) const fn buffered_batches(self) -> usize {
        self.buffered_batches
    }

    pub(crate) const fn buffered_bytes(self) -> usize {
        self.buffered_bytes
    }

    pub(crate) const fn max_batch_bytes(self) -> usize {
        self.max_batch_bytes
    }
}

impl Default for ValidatedConsumerLimits {
    fn default() -> Self {
        Self {
            in_flight_fetches: 8,
            buffered_batches: 8,
            buffered_bytes: 8 * 1024 * 1024,
            max_batch_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerLimitsError {
    InFlightFetches,
    BufferedBatches,
    BufferedBytes,
    MaxBatchBytes,
    MaxBatchExceedsBufferedBytes,
    MaxBatchBelowPartitionFetchBytes,
}

pub(crate) const fn validate_consumer_fetch_envelope(
    limits: ValidatedConsumerLimits,
    partition_max_bytes: u32,
) -> Result<(), ConsumerLimitsError> {
    if limits.max_batch_bytes < partition_max_bytes as usize {
        Err(ConsumerLimitsError::MaxBatchBelowPartitionFetchBytes)
    } else {
        Ok(())
    }
}
