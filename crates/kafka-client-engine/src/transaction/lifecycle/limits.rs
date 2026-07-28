//! Shared bounded resources for one installed transactional execution owner.

use kafka_client_core::{CompressionPolicy, ProducerRetryPolicy};

use super::host::TransactionLifecycleHostError;
use crate::transaction::partition_enrollment::TransactionPartitionEnrollmentLimits;

/// One explicit envelope shared by enrollment and producer-lifetime sequencing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionExecutionLimits {
    partition_capacity: usize,
    retained_topic_bytes: usize,
    retained_record_bytes: usize,
    max_wire_batch_bytes: usize,
    transaction_offset_count: usize,
    transaction_offset_bytes: usize,
    compression: CompressionPolicy,
    send_retry_policy: ProducerRetryPolicy,
}

impl TransactionExecutionLimits {
    #[cfg(test)]
    pub(crate) const fn try_new_with_retry_policy(
        partition_capacity: usize,
        retained_bytes: usize,
        compression: CompressionPolicy,
        send_retry_policy: ProducerRetryPolicy,
    ) -> Option<Self> {
        Self::try_new_with_bounds(
            partition_capacity,
            retained_bytes,
            retained_bytes,
            retained_bytes,
            compression,
            send_retry_policy,
        )
    }

    /// Validates distinct topic, source-record, and encoded-batch byte bounds.
    #[cfg(test)]
    pub(crate) const fn try_new_with_producer_bounds(
        partition_capacity: usize,
        retained_topic_bytes: usize,
        retained_record_bytes: usize,
        max_wire_batch_bytes: usize,
        compression: CompressionPolicy,
    ) -> Option<Self> {
        Self::try_new_with_bounds(
            partition_capacity,
            retained_topic_bytes,
            retained_record_bytes,
            max_wire_batch_bytes,
            compression,
            ProducerRetryPolicy::none(),
        )
    }

    /// Validates every producer limit and retains the retry policy captured at host start.
    pub(crate) const fn try_new_with_bounds(
        partition_capacity: usize,
        retained_topic_bytes: usize,
        retained_record_bytes: usize,
        max_wire_batch_bytes: usize,
        compression: CompressionPolicy,
        send_retry_policy: ProducerRetryPolicy,
    ) -> Option<Self> {
        Self::try_new_with_offset_bounds(
            partition_capacity,
            retained_topic_bytes,
            retained_record_bytes,
            max_wire_batch_bytes,
            partition_capacity,
            retained_topic_bytes,
            compression,
            send_retry_policy,
        )
    }

    /// Validates producer and transactional offset-transfer envelopes.
    #[expect(
        clippy::too_many_arguments,
        reason = "the closed limits constructor keeps each independent capacity explicit"
    )]
    pub(crate) const fn try_new_with_offset_bounds(
        partition_capacity: usize,
        retained_topic_bytes: usize,
        retained_record_bytes: usize,
        max_wire_batch_bytes: usize,
        transaction_offset_count: usize,
        transaction_offset_bytes: usize,
        compression: CompressionPolicy,
        send_retry_policy: ProducerRetryPolicy,
    ) -> Option<Self> {
        if partition_capacity == 0
            || retained_topic_bytes == 0
            || retained_record_bytes == 0
            || max_wire_batch_bytes == 0
            || transaction_offset_count == 0
            || transaction_offset_bytes == 0
        {
            None
        } else {
            Some(Self {
                partition_capacity,
                retained_topic_bytes,
                retained_record_bytes,
                max_wire_batch_bytes,
                transaction_offset_count,
                transaction_offset_bytes,
                compression,
                send_retry_policy,
            })
        }
    }

    pub(in crate::transaction) const fn compression(self) -> CompressionPolicy {
        self.compression
    }

    pub(in crate::transaction) const fn partition_capacity(self) -> usize {
        self.partition_capacity
    }

    pub(in crate::transaction) const fn send_retry_policy(self) -> ProducerRetryPolicy {
        self.send_retry_policy
    }

    pub(in crate::transaction) const fn retained_topic_bytes(self) -> usize {
        self.retained_topic_bytes
    }

    pub(in crate::transaction) const fn retained_record_bytes(self) -> usize {
        self.retained_record_bytes
    }

    pub(in crate::transaction) const fn max_wire_batch_bytes(self) -> usize {
        self.max_wire_batch_bytes
    }

    pub(in crate::transaction) const fn transaction_offset_count(self) -> usize {
        self.transaction_offset_count
    }

    pub(in crate::transaction) const fn transaction_offset_bytes(self) -> usize {
        self.transaction_offset_bytes
    }

    pub(super) const fn enrollment(
        self,
    ) -> Result<TransactionPartitionEnrollmentLimits, TransactionLifecycleHostError> {
        match TransactionPartitionEnrollmentLimits::try_new(
            self.partition_capacity,
            self.retained_topic_bytes,
        ) {
            Some(limits) => Ok(limits),
            None => Err(TransactionLifecycleHostError::InvalidExecutionLimits),
        }
    }
}
