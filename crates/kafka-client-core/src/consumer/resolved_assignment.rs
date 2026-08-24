//! Linear deadline-free input for installing broker-resolved Fetch positions.

use core::fmt;

use super::{AssignedTopicPartition, AssignmentEpoch, NextFetchOffset};
use crate::Moment;

mod reconciliation;
mod reconciliation_transition;

pub use reconciliation::{
    ReconcileResolvedAssignment, ReconcileResolvedAssignmentError,
    ReconcileResolvedAssignmentErrorKind, ResolvedAssignmentTarget,
};
#[cfg(test)]
mod reconciliation_test;
#[cfg(test)]
mod reconciliation_transition_test;

/// One assigned partition paired with its committed next Fetch offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedAssignedPartition {
    partition: AssignedTopicPartition,
    next_offset: NextFetchOffset,
}

impl ResolvedAssignedPartition {
    /// Creates one scalar resolved assignment fact.
    pub const fn new(partition: AssignedTopicPartition, next_offset: NextFetchOffset) -> Self {
        Self {
            partition,
            next_offset,
        }
    }

    /// Returns the assigned topic-partition.
    pub const fn partition(self) -> AssignedTopicPartition {
        self.partition
    }

    /// Returns the committed offset used by the first Fetch.
    pub const fn next_offset(self) -> NextFetchOffset {
        self.next_offset
    }
}

/// One complete ordered assignment whose positions need no resolution.
#[must_use = "a resolved assignment install must be applied or explicitly abandoned"]
#[derive(Debug, Eq, PartialEq)]
pub struct InstallResolvedAssignment {
    expected_assignment_epoch: Option<AssignmentEpoch>,
    partitions: Vec<ResolvedAssignedPartition>,
    now: Moment,
    throttle_ticks: u64,
}

impl InstallResolvedAssignment {
    /// Retains one ordered assignment and its broker throttle observation.
    pub const fn new(
        expected_assignment_epoch: Option<AssignmentEpoch>,
        partitions: Vec<ResolvedAssignedPartition>,
        now: Moment,
        throttle_ticks: u64,
    ) -> Self {
        Self {
            expected_assignment_epoch,
            partitions,
            now,
            throttle_ticks,
        }
    }

    /// Returns the exact retained control revision this install may replace.
    pub const fn expected_assignment_epoch(&self) -> Option<AssignmentEpoch> {
        self.expected_assignment_epoch
    }

    /// Borrows the exact ordered resolved assignment.
    pub fn partitions(&self) -> &[ResolvedAssignedPartition] {
        &self.partitions
    }

    /// Returns actual retained partition-vector capacity for engine accounting.
    pub fn partitions_capacity(&self) -> usize {
        self.partitions.capacity()
    }

    /// Returns the monotonic observation used only for broker throttle policy.
    pub const fn now(&self) -> Moment {
        self.now
    }

    /// Returns the nonnegative broker throttle duration in deterministic ticks.
    pub const fn throttle_ticks(&self) -> u64 {
        self.throttle_ticks
    }

    /// Recovers every owned input fact.
    pub fn into_parts(
        self,
    ) -> (
        Option<AssignmentEpoch>,
        Vec<ResolvedAssignedPartition>,
        Moment,
        u64,
    ) {
        (
            self.expected_assignment_epoch,
            self.partitions,
            self.now,
            self.throttle_ticks,
        )
    }
}

/// Lossless rejection of one complete resolved assignment install.
#[must_use = "a rejected resolved assignment must be recovered or explicitly abandoned"]
#[derive(Debug, Eq, PartialEq)]
pub struct InstallResolvedAssignmentError {
    kind: InstallResolvedAssignmentErrorKind,
    input: InstallResolvedAssignment,
}

impl InstallResolvedAssignmentError {
    pub(super) const fn new(
        kind: InstallResolvedAssignmentErrorKind,
        input: InstallResolvedAssignment,
    ) -> Self {
        Self { kind, input }
    }

    /// Returns the deterministic rejection reason.
    pub const fn kind(&self) -> InstallResolvedAssignmentErrorKind {
        self.kind
    }

    /// Borrows the exact rejected install input.
    pub const fn input(&self) -> &InstallResolvedAssignment {
        &self.input
    }

    /// Recovers the exact rejected install input.
    pub fn into_input(self) -> InstallResolvedAssignment {
        self.input
    }
}

impl fmt::Display for InstallResolvedAssignmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "resolved assignment install rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for InstallResolvedAssignmentError {}

/// Deterministic reason one resolved assignment could not be installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallResolvedAssignmentErrorKind {
    /// Assigned-consumer admission is permanently closed.
    ConsumerClosed,
    /// One topic-partition appeared more than once.
    DuplicatePartition {
        /// Duplicated partition.
        partition: AssignedTopicPartition,
    },
    /// One resolved partition sorted before its predecessor.
    ResolvedAssignmentOutOfOrder {
        /// Preceding resolved partition.
        previous: AssignedTopicPartition,
        /// Out-of-order resolved partition.
        current: AssignedTopicPartition,
    },
    /// Exact state and effect storage could not be reserved.
    AssignmentAllocationFailed,
    /// The input targets a different retained assignment.
    ResolvedAssignmentEpochMismatch {
        /// Retained assignment the input was permitted to replace.
        expected: Option<AssignmentEpoch>,
        /// Assignment retained when the input reached core.
        actual: Option<AssignmentEpoch>,
    },
    /// No further assignment epoch is representable.
    AssignmentEpochExhausted,
    /// A positive initial Fetch throttle could not become an absolute deadline.
    InitialFetchThrottleDeadlineOverflow,
}
