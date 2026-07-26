//! Guarded Join terminal, leader deferral, and route-confirmation transitions.

use crate::driver::classic_group::TrackedJoinGroupCalls;

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_call::ClassicGroupJoinCallOwner,
};

impl ClassicGroupExecution {
    pub(super) const fn join_call(&self) -> Option<&ClassicGroupJoinCallOwner> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::JoinDriverOwned(call)
            | ClassicGroupExecutionState::JoinConfirmationPending { call, .. } => Some(call),
            _ => None,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed staging returns the exact linear successor without another allocation"
    )]
    pub(super) fn stage_join_confirmation(
        &mut self,
        successor: ClassicGroupJoinSuccessor,
    ) -> Result<(), (ClassicGroupExecutionError, ClassicGroupJoinSuccessor)> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::JoinDriverOwned(call) = state else {
            self.set_execution_state(state);
            return Err((ClassicGroupExecutionError::HandoffMismatch, successor));
        };
        self.set_execution_state(ClassicGroupExecutionState::JoinConfirmationPending {
            call,
            successor,
        });
        Ok(())
    }

    pub(super) fn confirm_join(
        &mut self,
        calls: &mut TrackedJoinGroupCalls,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::JoinConfirmationPending { call, successor } = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        };
        let (integration, tracking, accepted) = call.into_parts();
        match calls.confirm_join_group_settlement(accepted) {
            Ok(()) => {
                self.set_execution_state(match successor {
                    ClassicGroupJoinSuccessor::Idle => ClassicGroupExecutionState::Idle,
                    ClassicGroupJoinSuccessor::PartitionCounts(prepared) => {
                        ClassicGroupExecutionState::PreparedPartitionCounts(prepared)
                    }
                    ClassicGroupJoinSuccessor::Sync(prepared) => {
                        ClassicGroupExecutionState::PreparedSync(prepared)
                    }
                });
                Ok(())
            }
            Err(failure) => {
                let (accepted, _error) = failure.into_parts();
                self.set_execution_state(ClassicGroupExecutionState::JoinConfirmationPending {
                    call: ClassicGroupJoinCallOwner::new(integration, tracking, accepted),
                    successor,
                });
                Err(ClassicGroupExecutionError::CallIdentityMismatch)
            }
        }
    }
}
