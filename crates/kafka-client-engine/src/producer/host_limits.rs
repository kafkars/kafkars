//! Checked agreement between producer admission and bounded engine owners.

use kafka_client_core::{
    ByteCount, CompressionPolicy, ProducerBatchPolicy, ProducerRetryPolicy,
    producer_transition_effect_capacity,
};

use crate::producer::host_error::ProducerHostLimitError;

/// Capacity values shared by core policy and every bounded engine owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerHostLimits {
    pub(crate) retained_bytes: usize,
    pub(crate) completion_capacity: usize,
    pub(crate) record_capacity: usize,
    pub(crate) batch_capacity: usize,
    pub(crate) timer_capacity: usize,
    pub(crate) encoded_byte_capacity: usize,
    pub(crate) max_wire_batch_bytes: usize,
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
}

impl ValidatedProducerHostLimits {
    pub(super) const fn retained_bytes(self) -> ByteCount {
        self.retained_bytes
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
        if self.record_capacity != self.completion_capacity {
            return Err(ProducerHostLimitError::RecordCompletionMismatch);
        }
        if self.batch_capacity < self.record_capacity {
            return Err(ProducerHostLimitError::InsufficientBatchCapacity);
        }
        if self.timer_capacity < self.batch_capacity {
            return Err(ProducerHostLimitError::InsufficientTimerCapacity);
        }
        producer_transition_effect_capacity(self.record_capacity, self.completion_capacity)
            .ok_or(ProducerHostLimitError::TransitionCapacityOverflow)?;
        if self.encoded_byte_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroEncodedByteCapacity);
        }
        if self.max_wire_batch_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroWireBatchBytes);
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
        Ok(ValidatedProducerHostLimits {
            retained_bytes: ByteCount::new(bytes),
        })
    }
}
