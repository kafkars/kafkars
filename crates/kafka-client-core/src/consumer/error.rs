//! Rejections that preserve direct-assignment and position state unchanged.

use core::fmt;

use super::{
    AssignedConsumerCloseId, AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset,
    PositionFence, RetireAssignmentErrorKind,
};
use crate::{Deadline, Moment};

/// Deterministic rejection of one assigned-consumer input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerMachineError {
    /// Direct-consumer admission is permanently closed.
    ConsumerClosed,
    /// A drain fact arrived before any close was accepted.
    CloseNotPending {
        /// Unowned close identity supplied by the engine.
        supplied: AssignedConsumerCloseId,
    },
    /// A drain fact belongs to a different close identity.
    StaleClose {
        /// Exact close retained by core.
        active: AssignedConsumerCloseId,
        /// Mismatched close supplied by the engine.
        supplied: AssignedConsumerCloseId,
    },
    /// The exact close already selected its terminal outcome.
    CloseAlreadyCompleted {
        /// Close identity whose terminal outcome is immutable.
        close_id: AssignedConsumerCloseId,
    },
    /// A direct assignment must contain at least one partition.
    EmptyAssignment,
    /// One topic-partition appeared more than once.
    DuplicatePartition {
        /// Duplicated partition.
        partition: AssignedTopicPartition,
    },
    /// One atomic control batch could not reserve its complete plan and effects.
    ControlAllocationFailed,
    /// No further assignment epoch is representable.
    AssignmentEpochExhausted,
    /// Reusable assignment retirement was rejected by its canonical owner.
    AssignmentRetirementRejected {
        /// Exact deadline-free retirement rejection.
        kind: RetireAssignmentErrorKind,
    },
    /// Control or execution input arrived before assignment.
    NoAssignment,
    /// Input belongs to a superseded assignment.
    StaleAssignment {
        /// Active assignment epoch.
        active: AssignmentEpoch,
        /// Epoch supplied by the input.
        supplied: AssignmentEpoch,
    },
    /// Input names a partition outside the active assignment.
    UnknownPartition {
        /// Unknown topic-partition.
        partition: AssignedTopicPartition,
    },
    /// No further position generation is representable.
    PositionEpochExhausted {
        /// Partition whose fence could not advance.
        partition: AssignedTopicPartition,
    },
    /// No further fetch revision is representable.
    FetchRevisionExhausted {
        /// Partition whose fetch identity could not advance.
        partition: AssignedTopicPartition,
    },
    /// Group-owned resume found no retained position it may reactivate.
    PositionNotRetained {
        /// Partition whose current phase cannot be resumed without resolution.
        partition: AssignedTopicPartition,
    },
    /// A resolution input belongs to older partition state.
    StalePosition {
        /// Active fence.
        active: PositionFence,
        /// Supplied stale fence.
        supplied: PositionFence,
    },
    /// No start-position resolution is outstanding for the supplied fence.
    PositionResolutionNotPending {
        /// Current position fence.
        fence: PositionFence,
    },
    /// A position-resolution deadline wake arrived before its exact deadline.
    PositionResolutionDeadlineNotElapsed {
        /// Current position fence.
        fence: PositionFence,
        /// Exact deadline retained by the resolving phase.
        deadline: Deadline,
        /// Early monotonic observation.
        now: Moment,
    },
    /// No positive position throttle is outstanding for the supplied fence.
    PositionThrottleNotPending {
        /// Current position fence.
        fence: PositionFence,
    },
    /// A position-throttle wake arrived before its exact deadline.
    PositionThrottleDeadlineNotElapsed {
        /// Current position fence.
        fence: PositionFence,
        /// Exact absolute throttle deadline.
        deadline: Deadline,
        /// Early monotonic observation.
        now: Moment,
    },
    /// No positive successful-Fetch throttle is outstanding for the supplied fence.
    FetchThrottleNotPending {
        /// Supplied future fetch identity.
        fence: FetchFence,
    },
    /// A successful-Fetch throttle wake arrived before its exact deadline.
    FetchThrottleDeadlineNotElapsed {
        /// Exact future fetch fenced by the timer.
        fence: FetchFence,
        /// Exact absolute throttle deadline.
        deadline: Deadline,
        /// Early monotonic observation.
        now: Moment,
    },
    /// A fetch terminal does not own the active execution.
    StaleFetch {
        /// Supplied stale fetch identity.
        supplied: FetchFence,
    },
    /// A fetch attempted to move its next position backwards.
    OffsetRegression {
        /// Offset used by the fetch.
        requested: NextFetchOffset,
        /// Invalid next offset reported by the interpreter.
        observed: NextFetchOffset,
    },
}

impl fmt::Display for AssignedConsumerMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConsumerClosed => formatter.write_str("direct consumer is closed"),
            Self::CloseNotPending { .. } => {
                formatter.write_str("direct-consumer close is not pending")
            }
            Self::StaleClose { .. } => {
                formatter.write_str("close drain belongs to a different close")
            }
            Self::CloseAlreadyCompleted { .. } => {
                formatter.write_str("direct-consumer close already completed")
            }
            Self::EmptyAssignment => formatter.write_str("direct assignment must not be empty"),
            Self::DuplicatePartition { .. } => {
                formatter.write_str("direct assignment contains a duplicate partition")
            }
            Self::ControlAllocationFailed => {
                formatter.write_str("assigned-consumer control allocation failed")
            }
            Self::AssignmentEpochExhausted => {
                formatter.write_str("direct assignment epoch exhausted")
            }
            Self::AssignmentRetirementRejected { .. } => {
                formatter.write_str("assignment retirement was rejected")
            }
            Self::NoAssignment => formatter.write_str("direct consumer has no assignment"),
            Self::StaleAssignment { .. } => {
                formatter.write_str("input belongs to a superseded assignment")
            }
            Self::UnknownPartition { .. } => {
                formatter.write_str("partition is not in the active assignment")
            }
            Self::PositionEpochExhausted { .. } => {
                formatter.write_str("partition position epoch exhausted")
            }
            Self::FetchRevisionExhausted { .. } => {
                formatter.write_str("partition fetch revision exhausted")
            }
            Self::PositionNotRetained { .. } => {
                formatter.write_str("partition has no retained resumable position")
            }
            Self::StalePosition { .. } => {
                formatter.write_str("position result belongs to superseded partition state")
            }
            Self::PositionResolutionNotPending { .. } => {
                formatter.write_str("position resolution is not pending")
            }
            Self::PositionResolutionDeadlineNotElapsed { .. } => {
                formatter.write_str("position resolution deadline has not elapsed")
            }
            Self::PositionThrottleNotPending { .. } => {
                formatter.write_str("position throttle is not pending")
            }
            Self::PositionThrottleDeadlineNotElapsed { .. } => {
                formatter.write_str("position throttle deadline has not elapsed")
            }
            Self::FetchThrottleNotPending { .. } => {
                formatter.write_str("successful-Fetch throttle is not pending")
            }
            Self::FetchThrottleDeadlineNotElapsed { .. } => {
                formatter.write_str("successful-Fetch throttle deadline has not elapsed")
            }
            Self::StaleFetch { .. } => {
                formatter.write_str("fetch result does not own the active execution")
            }
            Self::OffsetRegression { .. } => {
                formatter.write_str("fetch result moved the next offset backwards")
            }
        }
    }
}

impl std::error::Error for AssignedConsumerMachineError {}
