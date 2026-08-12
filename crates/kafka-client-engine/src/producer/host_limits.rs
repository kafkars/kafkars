//! Checked agreement between producer admission and bounded engine owners.

use kafka_client_core::{
    ByteCount, CompressionPolicy, ProducerBatchPolicy, ProducerRetryPolicy,
    producer_transition_effect_capacity,
};

use crate::producer::host_error::ProducerHostLimitError;

const MAX_IDEMPOTENT_IN_FLIGHT_REQUESTS: usize = 5;

/// Capacity values shared by core policy and every bounded engine owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostLimits {
    pub(crate) retained_bytes: usize,
    pub(crate) completion_capacity: usize,
    pub(crate) waiting_record_capacity: usize,
    pub(crate) waiting_byte_capacity: usize,
    pub(crate) record_capacity: usize,
    pub(crate) batch_capacity: usize,
    pub(crate) timer_capacity: usize,
    pub(crate) encoded_byte_capacity: usize,
    pub(crate) max_wire_batch_bytes: usize,
    pub(crate) max_request_bytes: usize,
    pub(crate) max_in_flight_requests_per_broker: usize,
    pub(crate) batch_policy: ProducerBatchPolicy,
    pub(crate) retry_policy: ProducerRetryPolicy,
    pub(crate) compression: CompressionPolicy,
    pub(crate) compression_worker_count: usize,
    pub(crate) compression_job_capacity: usize,
    pub(crate) compression_byte_capacity: usize,
}

/// Fully checked values consumed before the host acquires native resources.
#[must_use = "validated producer capacities must be started or deliberately discarded"]
pub(crate) struct ValidatedProducerHostLimits {
    retained_bytes: ByteCount,
    waiting_bytes: ByteCount,
    total_completion_capacity: usize,
    total_retained_bytes: usize,
}

impl ValidatedProducerHostLimits {
    pub(super) const fn retained_bytes(&self) -> ByteCount {
        self.retained_bytes
    }

    pub(super) const fn waiting_bytes(&self) -> ByteCount {
        self.waiting_bytes
    }

    pub(super) const fn total_completion_capacity(&self) -> usize {
        self.total_completion_capacity
    }

    pub(super) const fn total_retained_bytes(&self) -> usize {
        self.total_retained_bytes
    }
}

impl ProducerHostLimits {
    pub(crate) fn validate(self) -> Result<ValidatedProducerHostLimits, ProducerHostLimitError> {
        if self.retained_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroRetainedBytes);
        }
        if self.completion_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroCompletionCapacity);
        }
        if self.waiting_record_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroWaitingRecordCapacity);
        }
        if self.waiting_byte_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroWaitingByteCapacity);
        }
        let total_completion_capacity = self
            .completion_capacity
            .checked_add(self.waiting_record_capacity)
            .ok_or(ProducerHostLimitError::TotalRecordCapacityOverflow)?;
        let total_retained_bytes = self
            .retained_bytes
            .checked_add(self.waiting_byte_capacity)
            .ok_or(ProducerHostLimitError::TotalRetainedBytesOverflow)?;
        if self.record_capacity != self.completion_capacity {
            return Err(ProducerHostLimitError::RecordCompletionMismatch);
        }
        if self.batch_capacity < self.record_capacity {
            return Err(ProducerHostLimitError::InsufficientBatchCapacity);
        }
        if self.timer_capacity < self.batch_capacity {
            return Err(ProducerHostLimitError::InsufficientTimerCapacity);
        }
        producer_transition_effect_capacity(total_completion_capacity, self.completion_capacity)
            .ok_or(ProducerHostLimitError::TransitionCapacityOverflow)?;
        if self.encoded_byte_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroEncodedByteCapacity);
        }
        if self.max_wire_batch_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroWireBatchBytes);
        }
        if self.max_request_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroRequestBytes);
        }
        if self.max_request_bytes < self.max_wire_batch_bytes {
            return Err(ProducerHostLimitError::RequestSmallerThanBatch);
        }
        if self.max_in_flight_requests_per_broker == 0 {
            return Err(ProducerHostLimitError::ZeroInFlightRequests);
        }
        if self.max_in_flight_requests_per_broker > MAX_IDEMPOTENT_IN_FLIGHT_REQUESTS {
            return Err(ProducerHostLimitError::TooManyInFlightRequests);
        }
        if self.batch_policy.max_records() > self.record_capacity {
            return Err(ProducerHostLimitError::BatchRecordLimitExceedsCapacity);
        }
        match self.compression {
            CompressionPolicy::None => {
                if self.compression_worker_count != 0
                    || self.compression_job_capacity != 0
                    || self.compression_byte_capacity != 0
                {
                    return Err(ProducerHostLimitError::UnexpectedCompressionWorkers);
                }
            }
            CompressionPolicy::Gzip
            | CompressionPolicy::Snappy
            | CompressionPolicy::Lz4
            | CompressionPolicy::Zstd => {
                if self.compression_worker_count == 0
                    || self.compression_job_capacity == 0
                    || self.compression_byte_capacity == 0
                {
                    return Err(ProducerHostLimitError::MissingCompressionWorkers);
                }
                if self.compression_job_capacity > self.batch_capacity {
                    return Err(ProducerHostLimitError::CompressionJobsExceedBatches);
                }
            }
        }
        let bytes = u64::try_from(self.retained_bytes)
            .map_err(|_| ProducerHostLimitError::RetainedBytesOutOfRange)?;
        let waiting_bytes = u64::try_from(self.waiting_byte_capacity)
            .map_err(|_| ProducerHostLimitError::WaitingBytesOutOfRange)?;
        Ok(ValidatedProducerHostLimits {
            retained_bytes: ByteCount::new(bytes),
            waiting_bytes: ByteCount::new(waiting_bytes),
            total_completion_capacity,
            total_retained_bytes,
        })
    }
}
