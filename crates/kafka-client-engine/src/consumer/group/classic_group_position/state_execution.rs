//! Sole mutation owner for the position-bootstrap lifecycle.

use super::{
    ClassicGroupPositionCompleted, ClassicGroupPositionConfirmationPending,
    ClassicGroupPositionDriverOwned, ClassicGroupPositionHandoff, ClassicGroupPositionPrepared,
};

/// Complete explicit mechanism lifecycle for one assigned position bootstrap.
#[expect(
    clippy::large_enum_variant,
    reason = "each stage retains one exact preallocated owner; boxing would add hidden allocation"
)]
pub(in crate::consumer::group) enum ClassicGroupPositionExecutionState {
    Dormant,
    Prepared(ClassicGroupPositionPrepared),
    Handoff(ClassicGroupPositionHandoff),
    DriverOwned(ClassicGroupPositionDriverOwned),
    ConfirmationPending(ClassicGroupPositionConfirmationPending),
    Complete(ClassicGroupPositionCompleted),
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

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        match &self.state {
            ClassicGroupPositionExecutionState::Prepared(prepared) => {
                Some(prepared.key().operation_deadline().core())
            }
            _ => None,
        }
    }

    pub(in crate::consumer::group) const fn unsettled(&self) -> usize {
        if self.is_dormant() { 0 } else { 1 }
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
