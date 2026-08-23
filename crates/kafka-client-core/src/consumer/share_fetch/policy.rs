//! Bounded `ShareFetch` acquisition policy and lossless admission errors.

use core::fmt;

use crate::ByteCount;

use super::ShareAcquiredRange;

/// Fixed admission bounds for one acquisition ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareAcquisitionPolicy {
    ranges: usize,
    record_ceiling: u64,
    byte_budget: ByteCount,
}

impl ShareAcquisitionPolicy {
    /// Creates positive range, record, and byte limits.
    pub const fn try_new(
        max_ranges: usize,
        max_records: u64,
        max_retained_bytes: ByteCount,
    ) -> Result<Self, ShareAcquisitionPolicyError> {
        if max_ranges == 0 {
            return Err(ShareAcquisitionPolicyError::ZeroRanges);
        }
        if max_records == 0 {
            return Err(ShareAcquisitionPolicyError::ZeroRecords);
        }
        if max_retained_bytes.get() == 0 {
            return Err(ShareAcquisitionPolicyError::ZeroBytes);
        }
        Ok(Self {
            ranges: max_ranges,
            record_ceiling: max_records,
            byte_budget: max_retained_bytes,
        })
    }

    /// Returns the maximum simultaneously retained ranges.
    pub const fn max_ranges(self) -> usize {
        self.ranges
    }

    /// Returns the maximum simultaneously retained offsets.
    pub const fn max_records(self) -> u64 {
        self.record_ceiling
    }

    /// Returns the maximum simultaneously retained payload bytes.
    pub const fn max_retained_bytes(self) -> ByteCount {
        self.byte_budget
    }
}

/// Invalid bounded acquisition policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcquisitionPolicyError {
    /// No acquired range could ever enter the ledger.
    ZeroRanges,
    /// No acquired record could ever enter the ledger.
    ZeroRecords,
    /// No decoded bytes could ever enter the ledger.
    ZeroBytes,
}

/// Stable reason an entire response fact set could not enter the ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcquisitionAdmissionErrorKind {
    /// Core could not reserve its bounded correlation storage.
    AllocationFailed,
    /// The supplied response belonged to an expired lock boundary.
    ExpiredLock,
    /// One range duplicated or overlapped retained or sibling acquisition.
    OverlappingRange,
    /// Kafka UUID and local catalog identity did not remain one-to-one.
    TopicIdentityMismatch,
    /// The configured range bound would be exceeded.
    RangeCapacity,
    /// The configured record bound would be exceeded.
    RecordCapacity,
    /// The configured byte bound would be exceeded.
    ByteCapacity,
    /// The nonreused acquisition generation space was exhausted.
    GenerationExhausted,
    /// The requested local lifecycle transition was not currently owned.
    InvalidOwnership,
    /// Retained accounting could not be reconciled exactly.
    AccountingInvariant,
}

/// Lossless admission rejection retaining the exact decoded facts.
#[must_use = "rejected share acquisitions retain engine reclamation authority"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcquisitionAdmissionError {
    kind: ShareAcquisitionAdmissionErrorKind,
    ranges: Vec<ShareAcquiredRange>,
}

impl ShareAcquisitionAdmissionError {
    pub(super) const fn new(
        kind: ShareAcquisitionAdmissionErrorKind,
        ranges: Vec<ShareAcquiredRange>,
    ) -> Self {
        Self { kind, ranges }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> ShareAcquisitionAdmissionErrorKind {
        self.kind
    }

    /// Recovers every rejected range in response order.
    pub fn into_ranges(self) -> Vec<ShareAcquiredRange> {
        self.ranges
    }
}

impl fmt::Display for ShareAcquisitionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "share acquisition rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareAcquisitionAdmissionError {}
