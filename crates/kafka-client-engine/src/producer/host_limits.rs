//! Checked agreement between producer admission and bounded engine owners.

use kafka_client_core::{ByteCount, ProducerBatchPolicy, producer_transition_effect_capacity};

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
}

/// Fully checked values consumed before the host acquires native resources.
#[must_use = "validated producer capacities must be started or deliberately discarded"]
pub(crate) struct ValidatedProducerHostLimits {
    retained_bytes: ByteCount,
    transition_effect_capacity: usize,
}

impl ValidatedProducerHostLimits {
    pub(super) const fn into_parts(self) -> (ByteCount, usize) {
        (self.retained_bytes, self.transition_effect_capacity)
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
        let transition_effect_capacity =
            producer_transition_effect_capacity(self.record_capacity, self.completion_capacity)
                .ok_or(ProducerHostLimitError::TerminalTailCapacityOverflow)?;
        if self.encoded_byte_capacity == 0 {
            return Err(ProducerHostLimitError::ZeroEncodedByteCapacity);
        }
        if self.max_wire_batch_bytes == 0 {
            return Err(ProducerHostLimitError::ZeroWireBatchBytes);
        }
        if self.batch_policy.max_records() > self.record_capacity {
            return Err(ProducerHostLimitError::BatchRecordLimitExceedsCapacity);
        }
        let bytes = u64::try_from(self.retained_bytes)
            .map_err(|_| ProducerHostLimitError::RetainedBytesOutOfRange)?;
        Ok(ValidatedProducerHostLimits {
            retained_bytes: ByteCount::new(bytes),
            transition_effect_capacity,
        })
    }
}
