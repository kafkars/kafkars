//! Deterministic producer batch limits and linger policy.

use core::fmt;

use crate::ByteCount;

/// Invalid producer batch policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerBatchPolicyError {
    /// A batch must permit at least one record.
    ZeroRecordLimit,
    /// A batch must permit at least one encoded byte.
    ZeroByteLimit,
}

impl fmt::Display for ProducerBatchPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRecordLimit => formatter.write_str("batch record limit must be nonzero"),
            Self::ZeroByteLimit => formatter.write_str("batch byte limit must be nonzero"),
        }
    }
}

impl std::error::Error for ProducerBatchPolicyError {}

/// Count, conservative accumulator-size, and virtual-time thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchPolicy {
    max_records: usize,
    max_accumulator_bytes: ByteCount,
    linger_ticks: u64,
}

impl ProducerBatchPolicy {
    /// Validates deterministic batching thresholds.
    pub const fn try_new(
        max_records: usize,
        max_accumulator_bytes: ByteCount,
        linger_ticks: u64,
    ) -> Result<Self, ProducerBatchPolicyError> {
        if max_records == 0 {
            return Err(ProducerBatchPolicyError::ZeroRecordLimit);
        }
        if max_accumulator_bytes.get() == 0 {
            return Err(ProducerBatchPolicyError::ZeroByteLimit);
        }
        Ok(Self {
            max_records,
            max_accumulator_bytes,
            linger_ticks,
        })
    }

    /// First-slice policy that submits each accumulated record immediately.
    pub const fn single_record() -> Self {
        Self {
            max_records: 1,
            max_accumulator_bytes: ByteCount::new(u64::MAX),
            linger_ticks: 0,
        }
    }

    /// Returns the maximum record count.
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    /// Returns the conservative accumulator-byte readiness threshold.
    ///
    /// This is not exact Kafka wire length. `kafka-wire-records` remains
    /// authoritative for final encoded batch limits.
    pub const fn max_accumulator_bytes(self) -> ByteCount {
        self.max_accumulator_bytes
    }

    /// Returns the virtual linger duration.
    pub const fn linger_ticks(self) -> u64 {
        self.linger_ticks
    }
}
