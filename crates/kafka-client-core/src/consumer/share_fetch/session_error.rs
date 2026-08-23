//! Lossless local `ShareFetch` session and response-settlement errors.

use core::fmt;

use super::{ShareAcquiredRange, ShareAcquisitionAdmissionErrorKind};

/// Failure while opening one broker-local session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareFetchSessionOpenError {
    /// A live session cannot be reconstructed at a nonzero epoch.
    NoninitialEpoch,
    /// The assignment snapshot was malformed or exceeded bounds.
    Assignment(ShareFetchSessionErrorKind),
    /// The acquisition ledger could not reserve its configured capacity.
    Acquisition(ShareAcquisitionAdmissionErrorKind),
}

/// Stable local session or assignment rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareFetchSessionErrorKind {
    /// The transition does not belong to the current lifecycle phase.
    InvalidState,
    /// An admitted absolute deadline was already elapsed.
    DeadlineElapsed,
    /// One assignment retained the same partition more than once.
    DuplicatePartition,
    /// The broker-local assignment exceeded its fixed bound.
    AssignmentCapacity,
    /// A nonreused generation or epoch was exhausted.
    Exhausted,
}

/// Rejected local session operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareFetchSessionApplyError {
    kind: ShareFetchSessionErrorKind,
}

impl ShareFetchSessionApplyError {
    pub(super) const fn new(kind: ShareFetchSessionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(self) -> ShareFetchSessionErrorKind {
        self.kind
    }
}

/// Stable response-settlement rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareFetchSettlementErrorKind {
    /// The response did not belong to the current exact request.
    StaleAttempt,
    /// The original attempt deadline elapsed before observation.
    DeadlineElapsed,
    /// Assignment control changed while this request was in flight.
    AssignmentChanged,
    /// The response acquired a partition outside the request snapshot.
    UnassignedPartition,
    /// The live `ShareFetch` epoch could not advance.
    SessionEpochExhausted,
    /// The bounded acquisition ledger rejected the complete response.
    Acquisition(ShareAcquisitionAdmissionErrorKind),
}

/// Lossless response rejection retaining every decoded range.
#[must_use = "rejected ShareFetch facts retain engine reclamation authority"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareFetchSettlementError {
    kind: ShareFetchSettlementErrorKind,
    ranges: Vec<ShareAcquiredRange>,
}

impl ShareFetchSettlementError {
    pub(super) const fn new(
        kind: ShareFetchSettlementErrorKind,
        ranges: Vec<ShareAcquiredRange>,
    ) -> Self {
        Self { kind, ranges }
    }

    /// Returns the stable settlement category.
    pub const fn kind(&self) -> ShareFetchSettlementErrorKind {
        self.kind
    }

    /// Recovers every rejected range in response order.
    pub fn into_ranges(self) -> Vec<ShareAcquiredRange> {
        self.ranges
    }
}

impl fmt::Display for ShareFetchSettlementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ShareFetch settlement rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareFetchSettlementError {}
