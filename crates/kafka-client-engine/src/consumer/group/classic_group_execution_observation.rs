//! Read-only observations of one classic-group execution owner.

use kafka_client_core::Deadline;

#[cfg(test)]
use super::classic_group_assignment::ClassicGroupRevocationFailureKind;
use super::{
    classic_group_execution::ClassicGroupExecution,
    classic_group_join::{ClassicGroupExecutionState, PreparedClassicGroupJoin},
};

impl ClassicGroupExecution {
    pub(super) const fn next_deadline(&self) -> Option<Deadline> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::PreparedJoin(prepared) => Some(prepared.deadline().core()),
            ClassicGroupExecutionState::PreparedPartitionCounts(prepared) => {
                Some(prepared.deadline().core())
            }
            ClassicGroupExecutionState::PreparedSync(prepared) => Some(prepared.deadline().core()),
            ClassicGroupExecutionState::Idle
            | ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::JoinDriverOwned(_)
            | ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::PartitionCountHandoff { .. }
            | ClassicGroupExecutionState::PartitionCountDriverOwned { .. }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { .. }
            | ClassicGroupExecutionState::PartitionCountsPostCore { .. }
            | ClassicGroupExecutionState::SyncHandoff(_)
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_)
            | ClassicGroupExecutionState::CloseFault { .. } => None,
        }
    }

    pub(super) fn unsettled(&self) -> usize {
        let state = self.borrow_execution_state();
        usize::from(!matches!(state, ClassicGroupExecutionState::Idle))
    }

    pub(super) const fn is_idle(&self) -> bool {
        let state = self.borrow_execution_state();
        matches!(state, ClassicGroupExecutionState::Idle)
    }

    pub(super) const fn prepared_join(&self) -> Option<&PreparedClassicGroupJoin> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::PreparedJoin(prepared) => Some(prepared),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn close_fault(
        &self,
    ) -> Option<(
        &kafka_client_core::LiveGroupAssignment,
        kafka_client_core::ClassicGeneration,
        ClassicGroupRevocationFailureKind,
    )> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::CloseFault { revoke_failure } => Some((
                &revoke_failure.assignment,
                revoke_failure.classic_generation,
                revoke_failure.kind,
            )),
            _ => None,
        }
    }
}
