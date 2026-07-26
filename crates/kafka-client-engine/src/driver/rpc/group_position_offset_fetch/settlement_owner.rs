//! Registry mutation for raw handoff, restoration, confirmation, and recovery.

use super::{
    admission::GroupPositionOffsetFetchAccepted,
    calls::TrackedGroupPositionOffsetFetchCalls,
    recovery::{
        GroupPositionOffsetFetchCompletionFailure, GroupPositionOffsetFetchShutdownRecovery,
    },
    settlement::{
        GroupPositionOffsetFetchBeginError, GroupPositionOffsetFetchConfirmationError,
        GroupPositionOffsetFetchConfirmationFailure, GroupPositionOffsetFetchRestoreError,
        GroupPositionOffsetFetchRestoreFailure, PendingGroupPositionOffsetFetchConfirmation,
        SettledGroupPositionOffsetFetchCall,
    },
    terminal::GroupPositionOffsetFetchTerminal,
};

impl TrackedGroupPositionOffsetFetchCalls {
    pub(crate) fn begin_group_position_offset_fetch_settlement(
        &mut self,
        accepted: &GroupPositionOffsetFetchAccepted,
    ) -> Result<GroupPositionOffsetFetchTerminal, GroupPositionOffsetFetchBeginError> {
        let supplied = accepted.fence();
        if let Some(pending) = &self.pending_confirmation {
            return Err(GroupPositionOffsetFetchBeginError::ConfirmationPending {
                pending: pending.fence(),
            });
        }
        let Some(settled) = self.settled.as_ref() else {
            return Err(GroupPositionOffsetFetchBeginError::NoSettlement { supplied });
        };
        if settled.fence() != supplied {
            return Err(GroupPositionOffsetFetchBeginError::FenceMismatch {
                settled: settled.fence(),
                supplied,
            });
        }
        let Some(settled) = self.settled.take() else {
            return Err(GroupPositionOffsetFetchBeginError::NoSettlement { supplied });
        };
        let (terminal, pending) = settled.into_parts();
        self.pending_confirmation = Some(pending);
        Ok(terminal)
    }

    pub(crate) fn confirm_group_position_offset_fetch_settlement(
        &mut self,
        accepted: GroupPositionOffsetFetchAccepted,
    ) -> Result<(), GroupPositionOffsetFetchConfirmationFailure> {
        let supplied = accepted.fence();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(GroupPositionOffsetFetchConfirmationFailure::new(
                accepted,
                GroupPositionOffsetFetchConfirmationError::NoPending { supplied },
            ));
        };
        if pending.fence() != supplied {
            return Err(GroupPositionOffsetFetchConfirmationFailure::new(
                accepted,
                GroupPositionOffsetFetchConfirmationError::FenceMismatch {
                    pending: pending.fence(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(GroupPositionOffsetFetchConfirmationFailure::new(
                accepted,
                GroupPositionOffsetFetchConfirmationError::NoPending { supplied },
            ));
        };
        pending.confirm_route_token();
        accepted.confirm_receipt();
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed restoration returns the exact generated OffsetFetch response"
    )]
    pub(crate) fn restore_group_position_offset_fetch_settlement(
        &mut self,
        terminal: GroupPositionOffsetFetchTerminal,
    ) -> Result<(), GroupPositionOffsetFetchRestoreFailure> {
        let supplied = terminal.key().fence();
        if self.settled.is_some() {
            return Err(GroupPositionOffsetFetchRestoreFailure::new(
                terminal,
                GroupPositionOffsetFetchRestoreError::SettlementPresent { supplied },
            ));
        }
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(GroupPositionOffsetFetchRestoreFailure::new(
                terminal,
                GroupPositionOffsetFetchRestoreError::NoPending { supplied },
            ));
        };
        if pending.fence() != supplied {
            return Err(GroupPositionOffsetFetchRestoreFailure::new(
                terminal,
                GroupPositionOffsetFetchRestoreError::FenceMismatch {
                    pending: pending.fence(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(GroupPositionOffsetFetchRestoreFailure::new(
                terminal,
                GroupPositionOffsetFetchRestoreError::NoPending { supplied },
            ));
        };
        self.settled = Some(pending.into_settled(terminal));
        Ok(())
    }

    pub(crate) fn recover_group_position_offset_fetches_after_driver_shutdown(
        &mut self,
    ) -> GroupPositionOffsetFetchShutdownRecovery {
        let active = core::mem::take(&mut self.calls);
        let settled = self
            .settled
            .take()
            .map(SettledGroupPositionOffsetFetchCall::recover_after_driver_shutdown);
        let pending_fence = self
            .pending_confirmation
            .take()
            .map(PendingGroupPositionOffsetFetchConfirmation::recover_after_driver_shutdown);
        let completion = self
            .completion_failure
            .take()
            .map(GroupPositionOffsetFetchCompletionFailure::into_recovery);
        GroupPositionOffsetFetchShutdownRecovery::new(active, settled, pending_fence, completion)
    }
}
