//! Exact position route confirmation and receipt-retaining failure recovery.

use crate::driver::TrackedGroupPositionOffsetFetchCalls;

use super::{
    ClassicGroupPositionConfirmationPending, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
};

impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) const fn settlement_fence(
        &self,
    ) -> Option<kafka_client_core::GroupPositionFence> {
        match self.state() {
            ClassicGroupPositionExecutionState::DriverOwned(owner) => Some(owner.fence()),
            ClassicGroupPositionExecutionState::ConfirmationPending(pending) => {
                Some(pending.fence())
            }
            _ => None,
        }
    }

    pub(in crate::consumer::group) fn confirm_terminal_settlement(
        &mut self,
        calls: &mut TrackedGroupPositionOffsetFetchCalls,
    ) -> Result<(), ClassicGroupPositionExecutionError> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::ConfirmationPending(pending) = state else {
            self.set(state);
            return Err(ClassicGroupPositionExecutionError::NotConfirmationPending);
        };
        let (completed, accepted) = pending.into_parts();
        match calls.confirm_group_position_offset_fetch_settlement(accepted) {
            Ok(()) => {
                self.set(ClassicGroupPositionExecutionState::Complete(completed));
                Ok(())
            }
            Err(failure) => {
                let (accepted, _error) = failure.into_parts();
                self.set(ClassicGroupPositionExecutionState::ConfirmationPending(
                    ClassicGroupPositionConfirmationPending::new(completed, accepted),
                ));
                Err(ClassicGroupPositionExecutionError::Confirmation)
            }
        }
    }
}
