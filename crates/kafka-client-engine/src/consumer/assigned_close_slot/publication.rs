//! Retry-safe normal and abnormal terminal publication state.

use crate::completion::CompletionId;

use super::{AssignedCloseSlot, AssignedCloseState};
use crate::consumer::{
    assigned_close_error::AssignedCloseSlotError, assigned_host::AssignedConsumerCloseTerminal,
};

impl AssignedCloseSlot {
    /// Returns the normal terminal while retaining it for retryable publication.
    pub(in crate::consumer) fn ready_terminal(
        &self,
    ) -> Result<(CompletionId, AssignedConsumerCloseTerminal), AssignedCloseSlotError> {
        let AssignedCloseState::Ready {
            completion_id,
            close_id,
        } = self.state
        else {
            return Err(AssignedCloseSlotError::TerminalUnavailable {
                phase: self.phase(),
            });
        };
        Ok((
            completion_id,
            AssignedConsumerCloseTerminal::Closed(close_id),
        ))
    }

    /// Selects the exact abnormal terminal after unique driver teardown.
    pub(in crate::consumer) const fn recovery_terminal(
        &self,
    ) -> Option<(CompletionId, AssignedConsumerCloseTerminal)> {
        match self.state {
            AssignedCloseState::Reserved(completion_id)
            | AssignedCloseState::Accepted { completion_id, .. } => Some((
                completion_id,
                AssignedConsumerCloseTerminal::ExecutionUnavailable,
            )),
            AssignedCloseState::Ready {
                completion_id,
                close_id,
            } => Some((
                completion_id,
                AssignedConsumerCloseTerminal::Closed(close_id),
            )),
            AssignedCloseState::Vacant | AssignedCloseState::Published => None,
        }
    }

    /// Commits the exact publication identity without discarding core evidence.
    pub(in crate::consumer) fn mark_published(
        &mut self,
        supplied: CompletionId,
    ) -> Result<(), AssignedCloseSlotError> {
        let Some(active) = self.completion_id() else {
            return Err(AssignedCloseSlotError::TerminalUnavailable {
                phase: self.phase(),
            });
        };
        if active != supplied {
            return Err(AssignedCloseSlotError::MismatchedCompletionId { active, supplied });
        }
        self.state = AssignedCloseState::Published;
        Ok(())
    }

    const fn completion_id(&self) -> Option<CompletionId> {
        match self.state {
            AssignedCloseState::Reserved(completion_id)
            | AssignedCloseState::Accepted { completion_id, .. }
            | AssignedCloseState::Ready { completion_id, .. } => Some(completion_id),
            AssignedCloseState::Vacant | AssignedCloseState::Published => None,
        }
    }
}
