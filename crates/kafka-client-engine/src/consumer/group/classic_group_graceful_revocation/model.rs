//! Retained assignment owner and stable staging, completion, and host failures.

use kafka_client_core::{ClassicGeneration, ClassicGracefulRevocationError, LiveGroupAssignment};

use super::super::classic_group_assignment::ClassicGroupRevocationFailureKind;
use super::super::consumer_group_execution::ConsumerGroupExecutionError;

pub(super) struct PendingClassicGroupRevocation {
    pub(super) assignment: LiveGroupAssignment,
    pub(super) generation: ClassicGeneration,
}

impl PendingClassicGroupRevocation {
    pub(super) const fn new(
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
    ) -> Self {
        Self {
            assignment,
            generation,
        }
    }
}

pub(super) enum PendingGroupRevocation {
    Classic(PendingClassicGroupRevocation),
    ClassicReconciliation(PendingClassicGroupRevocation),
    Consumer(LiveGroupAssignment),
}

impl PendingGroupRevocation {
    pub(super) const fn classic(
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
    ) -> Self {
        Self::Classic(PendingClassicGroupRevocation::new(assignment, generation))
    }

    pub(super) const fn consumer(assignment: LiveGroupAssignment) -> Self {
        Self::Consumer(assignment)
    }

    pub(super) const fn classic_reconciliation(
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
    ) -> Self {
        Self::ClassicReconciliation(PendingClassicGroupRevocation::new(assignment, generation))
    }

    pub(super) fn into_assignment(self) -> LiveGroupAssignment {
        match self {
            Self::Classic(pending) | Self::ClassicReconciliation(pending) => pending.assignment,
            Self::Consumer(assignment) => assignment,
        }
    }
}

/// Pre-core admission rejection; the caller retains its exact assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupRevocationBeginError {
    Occupied,
    Core(ClassicGracefulRevocationError),
    UnexpectedEffect,
}

/// Entry staging rejection that returns the exact core-emitted assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupRevocationStageError {
    AssignmentMismatch,
    FetchBindingMissing,
    FetchBindingMismatch,
    Owner(ClassicGroupRevocationBeginError),
}

/// Private completion rejection that never retires an assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ClassicGroupRevocationAcknowledgeError {
    NoActiveLease,
    AssignmentEpochMismatch,
    DeadlineElapsed,
    Core(ClassicGracefulRevocationError),
    UnexpectedEffect,
}

/// One bounded registry-host transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupRevocationTurn {
    Idle,
    Progress,
}

/// Stable failure while the exact pending assignment remains retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupRevocationHostError {
    Core(ClassicGracefulRevocationError),
    UnexpectedEffect,
    MissingPending,
    Revocation(ClassicGroupRevocationFailureKind),
    ConsumerGroup(ConsumerGroupExecutionError),
}
