//! Registry mutation for group commit begin, confirmation, and restoration.

use kafka_client_core::{GroupOffsetCommitInput, OperationId};

use super::{
    group_offset_commit_calls::TrackedGroupOffsetCommitCalls,
    group_offset_commit_settlement::{
        GroupOffsetCommitBeginError, GroupOffsetCommitConfirmationError,
        GroupOffsetCommitRestoreError, GroupOffsetCommitRestoreFailure,
    },
};

impl TrackedGroupOffsetCommitCalls {
    pub(crate) fn begin_group_commit_settlement(
        &mut self,
        supplied: OperationId,
    ) -> Result<GroupOffsetCommitInput, GroupOffsetCommitBeginError> {
        if let Some(pending) = &self.pending_confirmation {
            return Err(GroupOffsetCommitBeginError::ConfirmationPending {
                pending: pending.operation_id(),
            });
        }
        let Some(settled) = self.settled.as_ref() else {
            return Err(GroupOffsetCommitBeginError::NoSettlement { supplied });
        };
        if settled.operation_id() != supplied {
            return Err(GroupOffsetCommitBeginError::OperationMismatch {
                settled: settled.operation_id(),
                supplied,
            });
        }
        let Some(settled) = self.settled.take() else {
            return Err(GroupOffsetCommitBeginError::NoSettlement { supplied });
        };
        let (input, pending) = settled.into_parts();
        self.pending_confirmation = Some(pending);
        Ok(input)
    }

    pub(crate) fn confirm_group_commit_settlement(
        &mut self,
        supplied: OperationId,
    ) -> Result<(), GroupOffsetCommitConfirmationError> {
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(GroupOffsetCommitConfirmationError::NoPendingConfirmation { supplied });
        };
        if pending.operation_id() != supplied {
            return Err(GroupOffsetCommitConfirmationError::OperationMismatch {
                pending: pending.operation_id(),
                supplied,
            });
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(GroupOffsetCommitConfirmationError::NoPendingConfirmation { supplied });
        };
        pending.confirm_group_commit_route_token();
        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed restoration must return the exact Vec-bearing core input"
    )]
    pub(crate) fn restore_group_commit_settlement(
        &mut self,
        supplied: OperationId,
        input: GroupOffsetCommitInput,
    ) -> Result<(), GroupOffsetCommitRestoreFailure> {
        if self.settled.is_some() {
            return Err(GroupOffsetCommitRestoreFailure::new(
                input,
                GroupOffsetCommitRestoreError::SettlementPresent { supplied },
            ));
        }
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(GroupOffsetCommitRestoreFailure::new(
                input,
                GroupOffsetCommitRestoreError::NoPendingConfirmation { supplied },
            ));
        };
        if pending.operation_id() != supplied {
            return Err(GroupOffsetCommitRestoreFailure::new(
                input,
                GroupOffsetCommitRestoreError::OperationMismatch {
                    pending: pending.operation_id(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(GroupOffsetCommitRestoreFailure::new(
                input,
                GroupOffsetCommitRestoreError::NoPendingConfirmation { supplied },
            ));
        };
        self.settled = Some(pending.into_settled(input));
        Ok(())
    }
}
