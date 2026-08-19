//! Sole mutation owner for the position-bootstrap lifecycle.

use crate::consumer::GroupConsumerPositionFailureKind;
use kafka_client_core::{
    GroupPositionBootstrapFailureKind, GroupPositionBootstrapTerminal,
    GroupPositionPartitionResult, GroupPositionResetTerminal, PositionResolutionAttemptFailure,
};

use super::super::classic_group_position_reset::{
    ClassicGroupPositionResetCompleted, ClassicGroupPositionResetCompletionFault,
    ClassicGroupPositionResetDriverOwned, ClassicGroupPositionResetPrepared,
    ClassicGroupPositionResetTerminalFault,
};
use super::{
    ClassicGroupPositionCompleted, ClassicGroupPositionConfirmationPending,
    ClassicGroupPositionDriverOwned, ClassicGroupPositionHandoff, ClassicGroupPositionPrepared,
};

/// Semantic position terminal transferred into the entry's first-fault owner.
#[must_use = "a failed position terminal must remain owned until host shutdown"]
pub(in crate::consumer::group) enum ClassicGroupPositionFailure {
    Bootstrap(ClassicGroupPositionCompleted),
    Reset(ClassicGroupPositionResetCompleted),
}

impl ClassicGroupPositionFailure {
    pub(in crate::consumer::group) fn observation_kind(&self) -> GroupConsumerPositionFailureKind {
        match self {
            Self::Bootstrap(completed) => match completed.terminal() {
                GroupPositionBootstrapTerminal::MissingOffsets(_) => {
                    GroupConsumerPositionFailureKind::MissingOffset
                }
                GroupPositionBootstrapTerminal::PartitionRejected(rejected) => {
                    match rejected.first_rejected().result() {
                        GroupPositionPartitionResult::Rejected(error) => {
                            GroupConsumerPositionFailureKind::Broker(error.code())
                        }
                        GroupPositionPartitionResult::Committed(_)
                        | GroupPositionPartitionResult::Missing => {
                            GroupConsumerPositionFailureKind::InvalidResponse
                        }
                    }
                }
                GroupPositionBootstrapTerminal::Failed(failure) => {
                    bootstrap_failure_kind(failure.kind())
                }
                GroupPositionBootstrapTerminal::Ready(_)
                | GroupPositionBootstrapTerminal::ResetRequired(_) => {
                    GroupConsumerPositionFailureKind::InvalidResponse
                }
            },
            Self::Reset(completed) => match completed.terminal() {
                GroupPositionResetTerminal::Failed(failure) => {
                    resolution_failure_kind(failure.failure())
                }
                GroupPositionResetTerminal::Ready(_) => {
                    GroupConsumerPositionFailureKind::InvalidResponse
                }
            },
        }
    }

    pub(in crate::consumer::group) const fn retained_owner_count(&self) -> usize {
        match self {
            Self::Bootstrap(completed) => {
                let _terminal = completed.terminal();
                1
            }
            Self::Reset(completed) => {
                let _terminal = completed.terminal();
                1
            }
        }
    }
}

const fn bootstrap_failure_kind(
    kind: GroupPositionBootstrapFailureKind,
) -> GroupConsumerPositionFailureKind {
    match kind {
        GroupPositionBootstrapFailureKind::DeadlineElapsed => {
            GroupConsumerPositionFailureKind::DeadlineElapsed
        }
        GroupPositionBootstrapFailureKind::DriverRejected => {
            GroupConsumerPositionFailureKind::DriverRejected
        }
        GroupPositionBootstrapFailureKind::Transport => GroupConsumerPositionFailureKind::Transport,
        GroupPositionBootstrapFailureKind::Compatibility => {
            GroupConsumerPositionFailureKind::Compatibility
        }
        GroupPositionBootstrapFailureKind::InvalidResponse => {
            GroupConsumerPositionFailureKind::InvalidResponse
        }
        GroupPositionBootstrapFailureKind::ResponseTooLarge => {
            GroupConsumerPositionFailureKind::ResponseTooLarge
        }
        GroupPositionBootstrapFailureKind::Broker(error) => {
            GroupConsumerPositionFailureKind::Broker(error.code())
        }
    }
}

const fn resolution_failure_kind(
    kind: PositionResolutionAttemptFailure,
) -> GroupConsumerPositionFailureKind {
    match kind {
        PositionResolutionAttemptFailure::DeadlineElapsed => {
            GroupConsumerPositionFailureKind::DeadlineElapsed
        }
        PositionResolutionAttemptFailure::DriverRejected => {
            GroupConsumerPositionFailureKind::DriverRejected
        }
        PositionResolutionAttemptFailure::Transport => GroupConsumerPositionFailureKind::Transport,
        PositionResolutionAttemptFailure::Broker(error) => {
            GroupConsumerPositionFailureKind::Broker(error.get())
        }
        PositionResolutionAttemptFailure::Compatibility => {
            GroupConsumerPositionFailureKind::Compatibility
        }
        PositionResolutionAttemptFailure::InvalidResponse => {
            GroupConsumerPositionFailureKind::InvalidResponse
        }
        PositionResolutionAttemptFailure::ResponseTooLarge => {
            GroupConsumerPositionFailureKind::ResponseTooLarge
        }
    }
}

/// Complete explicit mechanism lifecycle for one assigned position bootstrap.
pub(in crate::consumer::group) enum ClassicGroupPositionExecutionState {
    Dormant,
    Prepared(ClassicGroupPositionPrepared),
    Handoff(ClassicGroupPositionHandoff),
    DriverOwned(ClassicGroupPositionDriverOwned),
    ConfirmationPending(ClassicGroupPositionConfirmationPending),
    Complete(ClassicGroupPositionCompleted),
    ResetPrepared(ClassicGroupPositionResetPrepared),
    ResetDriverOwned(ClassicGroupPositionResetDriverOwned),
    ResetComplete(ClassicGroupPositionResetCompleted),
    ResetCompletionFault(ClassicGroupPositionResetCompletionFault),
    ResetTerminalFault(ClassicGroupPositionResetTerminalFault),
}

/// Sole state-mutation owner for one entry's position-bootstrap mechanism.
pub(in crate::consumer::group) struct ClassicGroupPositionExecution {
    state: ClassicGroupPositionExecutionState,
}

impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) const fn new() -> Self {
        Self {
            state: ClassicGroupPositionExecutionState::Dormant,
        }
    }

    pub(in crate::consumer::group) const fn state(&self) -> &ClassicGroupPositionExecutionState {
        &self.state
    }

    pub(in crate::consumer::group) const fn is_dormant(&self) -> bool {
        matches!(self.state, ClassicGroupPositionExecutionState::Dormant)
    }

    pub(in crate::consumer::group) fn has_ready_bootstrap_terminal(&self) -> bool {
        matches!(
            &self.state,
            ClassicGroupPositionExecutionState::Complete(completed)
                if matches!(completed.terminal(), GroupPositionBootstrapTerminal::Ready(_))
        )
    }

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        match &self.state {
            ClassicGroupPositionExecutionState::Prepared(prepared) => {
                Some(prepared.key().operation_deadline().core())
            }
            ClassicGroupPositionExecutionState::ResetPrepared(prepared) => {
                Some(prepared.operation_deadline.core())
            }
            _ => None,
        }
    }

    pub(in crate::consumer::group) const fn unsettled(&self) -> usize {
        if self.is_dormant() { 0 } else { 1 }
    }

    /// Transfers an observable semantic failure without consuming Ready work.
    pub(in crate::consumer::group) fn take_failure(
        &mut self,
    ) -> Option<ClassicGroupPositionFailure> {
        let is_failure = match &self.state {
            ClassicGroupPositionExecutionState::Complete(completed) => matches!(
                completed.terminal(),
                GroupPositionBootstrapTerminal::MissingOffsets(_)
                    | GroupPositionBootstrapTerminal::PartitionRejected(_)
                    | GroupPositionBootstrapTerminal::Failed(_)
            ),
            ClassicGroupPositionExecutionState::ResetComplete(_) => true,
            _ => false,
        };
        if !is_failure {
            return None;
        }
        match self.replace(ClassicGroupPositionExecutionState::Dormant) {
            ClassicGroupPositionExecutionState::Complete(completed) => {
                Some(ClassicGroupPositionFailure::Bootstrap(completed))
            }
            ClassicGroupPositionExecutionState::ResetComplete(completed) => {
                Some(ClassicGroupPositionFailure::Reset(completed))
            }
            state => {
                self.set(state);
                None
            }
        }
    }

    pub(in crate::consumer::group) fn replace(
        &mut self,
        replacement: ClassicGroupPositionExecutionState,
    ) -> ClassicGroupPositionExecutionState {
        core::mem::replace(&mut self.state, replacement)
    }

    pub(in crate::consumer::group) fn set(&mut self, state: ClassicGroupPositionExecutionState) {
        self.state = state;
    }
}
