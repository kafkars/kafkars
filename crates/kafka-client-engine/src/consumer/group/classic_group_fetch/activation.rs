//! Linear binding, transition, and rejection owners for one Fetch activation.

use kafka_client_core::{
    AssignedConsumerTransition, AssignmentEpoch, GroupPositionFence, InstallResolvedAssignment,
    InstallResolvedAssignmentError, InstallResolvedAssignmentErrorKind,
};

use super::super::classic_group_position::{
    ClassicGroupPositionActivationError, ClassicGroupPositionCompleted,
};

/// Exact relationship between one confirmed group position and one core assignment.
#[must_use = "an active group Fetch binding fences all later Fetch ownership"]
pub(in crate::consumer::group) struct ClassicGroupFetchBinding {
    position_fence: GroupPositionFence,
    assignment_epoch: AssignmentEpoch,
}

impl ClassicGroupFetchBinding {
    pub(super) const fn new(
        position_fence: GroupPositionFence,
        assignment_epoch: AssignmentEpoch,
    ) -> Self {
        Self {
            position_fence,
            assignment_epoch,
        }
    }

    pub(super) const fn position_fence(&self) -> GroupPositionFence {
        self.position_fence
    }

    pub(super) const fn assignment_epoch(&self) -> AssignmentEpoch {
        self.assignment_epoch
    }
}

/// Installed assignment and its uninterpreted ordered core effects.
#[must_use = "Fetch activation effects remain owned until a later interpreter consumes them"]
pub(in crate::consumer::group) struct ClassicGroupFetchActivation {
    binding: ClassicGroupFetchBinding,
    transition: AssignedConsumerTransition,
}

/// Post-core invariant retained after the deterministic machine already mutated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchPostCoreFaultKind {
    MissingAssignmentEpoch,
}

/// Mutated core transition and completed position retained after an invariant fault.
#[must_use = "a post-core activation fault retains every mutated linear owner"]
pub(in crate::consumer::group) struct ClassicGroupFetchActivationFault {
    completed: ClassicGroupPositionCompleted,
    transition: AssignedConsumerTransition,
    kind: ClassicGroupFetchPostCoreFaultKind,
}

impl ClassicGroupFetchActivationFault {
    pub(super) const fn new(
        completed: ClassicGroupPositionCompleted,
        transition: AssignedConsumerTransition,
        kind: ClassicGroupFetchPostCoreFaultKind,
    ) -> Self {
        Self {
            completed,
            transition,
            kind,
        }
    }

    pub(super) const fn kind(&self) -> ClassicGroupFetchPostCoreFaultKind {
        self.kind
    }

    pub(super) const fn completed(&self) -> &ClassicGroupPositionCompleted {
        &self.completed
    }

    pub(super) const fn transition(&self) -> &AssignedConsumerTransition {
        &self.transition
    }
}

impl ClassicGroupFetchActivation {
    pub(super) const fn new(
        binding: ClassicGroupFetchBinding,
        transition: AssignedConsumerTransition,
    ) -> Self {
        Self {
            binding,
            transition,
        }
    }

    pub(super) const fn binding(&self) -> &ClassicGroupFetchBinding {
        &self.binding
    }

    pub(super) const fn transition(&self) -> &AssignedConsumerTransition {
        &self.transition
    }
}

/// Stable reason an exact completed position could not activate Fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchActivationFailureKind {
    AlreadyActive,
    Position(ClassicGroupPositionActivationError),
    Core(InstallResolvedAssignmentErrorKind),
}

enum ClassicGroupFetchActivationFailureSource {
    AlreadyActive,
    Position(ClassicGroupPositionActivationError),
    Core(InstallResolvedAssignmentError),
}

/// Lossless activation rejection retaining the completed position and copied input.
#[must_use = "a rejected activation retains the exact completed position owner"]
pub(in crate::consumer::group) struct ClassicGroupFetchActivationFailure {
    completed: ClassicGroupPositionCompleted,
    source: ClassicGroupFetchActivationFailureSource,
}

/// Stable result category spanning returned pre-core owners and retained post-core fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchActivationErrorKind {
    Returned(ClassicGroupFetchActivationFailureKind),
    Retained(ClassicGroupFetchPostCoreFaultKind),
}

/// Activation error distinguishes lossless rejection from mutated retained fault.
#[must_use = "activation errors identify where every linear owner remains"]
#[expect(
    clippy::large_enum_variant,
    reason = "the returned variant preserves the exact completed position without hidden boxing"
)]
pub(in crate::consumer::group) enum ClassicGroupFetchActivationError {
    Returned(ClassicGroupFetchActivationFailure),
    Retained(ClassicGroupFetchPostCoreFaultKind),
}

impl ClassicGroupFetchActivationError {
    pub(super) const fn kind(&self) -> ClassicGroupFetchActivationErrorKind {
        match self {
            Self::Returned(failure) => {
                ClassicGroupFetchActivationErrorKind::Returned(failure.kind())
            }
            Self::Retained(kind) => ClassicGroupFetchActivationErrorKind::Retained(*kind),
        }
    }

    pub(super) fn into_returned(self) -> Option<ClassicGroupFetchActivationFailure> {
        match self {
            Self::Returned(failure) => Some(failure),
            Self::Retained(_) => None,
        }
    }
}

impl ClassicGroupFetchActivationFailure {
    pub(super) const fn already_active(completed: ClassicGroupPositionCompleted) -> Self {
        Self {
            completed,
            source: ClassicGroupFetchActivationFailureSource::AlreadyActive,
        }
    }

    pub(super) const fn position(
        completed: ClassicGroupPositionCompleted,
        error: ClassicGroupPositionActivationError,
    ) -> Self {
        Self {
            completed,
            source: ClassicGroupFetchActivationFailureSource::Position(error),
        }
    }

    pub(super) const fn core(
        completed: ClassicGroupPositionCompleted,
        error: InstallResolvedAssignmentError,
    ) -> Self {
        Self {
            completed,
            source: ClassicGroupFetchActivationFailureSource::Core(error),
        }
    }

    pub(super) const fn kind(&self) -> ClassicGroupFetchActivationFailureKind {
        match &self.source {
            ClassicGroupFetchActivationFailureSource::AlreadyActive => {
                ClassicGroupFetchActivationFailureKind::AlreadyActive
            }
            ClassicGroupFetchActivationFailureSource::Position(error) => {
                ClassicGroupFetchActivationFailureKind::Position(*error)
            }
            ClassicGroupFetchActivationFailureSource::Core(error) => {
                ClassicGroupFetchActivationFailureKind::Core(error.kind())
            }
        }
    }

    pub(super) const fn completed(&self) -> &ClassicGroupPositionCompleted {
        &self.completed
    }

    pub(super) const fn rejected_input(&self) -> Option<&InstallResolvedAssignment> {
        match &self.source {
            ClassicGroupFetchActivationFailureSource::Core(error) => Some(error.input()),
            _ => None,
        }
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ClassicGroupPositionCompleted,
        Option<InstallResolvedAssignment>,
    ) {
        let input = match self.source {
            ClassicGroupFetchActivationFailureSource::Core(error) => Some(error.into_input()),
            ClassicGroupFetchActivationFailureSource::AlreadyActive
            | ClassicGroupFetchActivationFailureSource::Position(_) => None,
        };
        (self.completed, input)
    }
}
