//! Linear target ownership and lossless rejection for resolved reconciliation.

use core::fmt;

use super::{AssignedTopicPartition, AssignmentEpoch, ResolvedAssignedPartition};
use crate::Moment;

/// One ordered partition in the next assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedAssignmentTarget {
    /// Moves exact active partition state into the next control revision.
    Retain(AssignedTopicPartition),
    /// Acquires a partition that is absent from the active assignment.
    Acquire(ResolvedAssignedPartition),
}

impl ResolvedAssignmentTarget {
    /// Returns the target topic-partition used for canonical ordering.
    pub const fn partition(self) -> AssignedTopicPartition {
        match self {
            Self::Retain(partition) => partition,
            Self::Acquire(assigned) => assigned.partition(),
        }
    }
}

/// One complete ordered reconciliation against an exact active assignment.
#[must_use = "a resolved reconciliation must be applied or explicitly abandoned"]
#[derive(Debug, Eq, PartialEq)]
pub struct ReconcileResolvedAssignment {
    expected_assignment_epoch: AssignmentEpoch,
    targets: Vec<ResolvedAssignmentTarget>,
    now: Moment,
    acquired_throttle_ticks: u64,
}

impl ReconcileResolvedAssignment {
    /// Retains one exact control revision and complete ordered target.
    pub const fn new(
        expected_assignment_epoch: AssignmentEpoch,
        targets: Vec<ResolvedAssignmentTarget>,
        now: Moment,
        acquired_throttle_ticks: u64,
    ) -> Self {
        Self {
            expected_assignment_epoch,
            targets,
            now,
            acquired_throttle_ticks,
        }
    }

    /// Returns the exact active control revision this input may reconcile.
    pub const fn expected_assignment_epoch(&self) -> AssignmentEpoch {
        self.expected_assignment_epoch
    }

    /// Borrows the exact canonical target entries.
    pub fn targets(&self) -> &[ResolvedAssignmentTarget] {
        &self.targets
    }

    /// Returns actual target-vector capacity for engine accounting.
    pub fn targets_capacity(&self) -> usize {
        self.targets.capacity()
    }

    /// Returns the monotonic observation for retained and acquired throttle policy.
    pub const fn now(&self) -> Moment {
        self.now
    }

    /// Returns the nonnegative throttle duration applied only to acquisitions.
    pub const fn acquired_throttle_ticks(&self) -> u64 {
        self.acquired_throttle_ticks
    }

    /// Recovers every owned input fact.
    pub fn into_parts(self) -> (AssignmentEpoch, Vec<ResolvedAssignmentTarget>, Moment, u64) {
        (
            self.expected_assignment_epoch,
            self.targets,
            self.now,
            self.acquired_throttle_ticks,
        )
    }
}

/// Lossless rejection of one resolved assignment reconciliation.
#[must_use = "a rejected reconciliation must be recovered or explicitly abandoned"]
#[derive(Debug, Eq, PartialEq)]
pub struct ReconcileResolvedAssignmentError {
    kind: ReconcileResolvedAssignmentErrorKind,
    input: ReconcileResolvedAssignment,
}

impl ReconcileResolvedAssignmentError {
    pub(in crate::consumer) const fn new(
        kind: ReconcileResolvedAssignmentErrorKind,
        input: ReconcileResolvedAssignment,
    ) -> Self {
        Self { kind, input }
    }

    /// Returns the deterministic rejection reason.
    pub const fn kind(&self) -> ReconcileResolvedAssignmentErrorKind {
        self.kind
    }

    /// Borrows the exact rejected reconciliation input.
    pub const fn input(&self) -> &ReconcileResolvedAssignment {
        &self.input
    }

    /// Recovers the exact rejected reconciliation input.
    pub fn into_input(self) -> ReconcileResolvedAssignment {
        self.input
    }
}

impl fmt::Display for ReconcileResolvedAssignmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "resolved assignment reconciliation rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ReconcileResolvedAssignmentError {}

/// Deterministic reason one resolved reconciliation could not be installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileResolvedAssignmentErrorKind {
    /// Assigned-consumer admission is permanently closed.
    ConsumerClosed,
    /// The input did not name the exact active assignment.
    AssignmentEpochMismatch {
        /// Active assignment required by the input.
        expected: AssignmentEpoch,
        /// Assignment retained when the input reached core.
        actual: Option<AssignmentEpoch>,
    },
    /// One topic-partition appeared more than once.
    DuplicatePartition {
        /// Duplicated target partition.
        partition: AssignedTopicPartition,
    },
    /// One target sorted before its predecessor.
    TargetOutOfOrder {
        /// Preceding target partition.
        previous: AssignedTopicPartition,
        /// Out-of-order target partition.
        current: AssignedTopicPartition,
    },
    /// A retained target was absent from the active assignment.
    RetainedPartitionMissing {
        /// Missing retained partition.
        partition: AssignedTopicPartition,
    },
    /// An acquired target was already present in the active assignment.
    AcquiredPartitionAlreadyExists {
        /// Existing partition that cannot be acquired again.
        partition: AssignedTopicPartition,
    },
    /// One retained partition could not advance its position fence.
    PositionEpochExhausted {
        /// Retained partition whose position identity was exhausted.
        partition: AssignedTopicPartition,
    },
    /// Exact target, plan, state, and effect storage could not be reserved.
    ReconciliationAllocationFailed,
    /// No further assignment epoch is representable.
    AssignmentEpochExhausted,
    /// A positive acquired-position throttle could not become an absolute deadline.
    AcquiredFetchThrottleDeadlineOverflow,
}
