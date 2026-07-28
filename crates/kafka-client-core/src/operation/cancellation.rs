//! Cancellation preflight and sealed-execution restart ownership.

use crate::{BatchId, TerminalRelease, TransitionError};

use super::{ProducerOperation, ProducerOperationState};

impl ProducerOperation {
    pub(crate) fn plan_cancel(&self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::RetryWaiting { .. } => self.plan_finish(),
            ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
        }
    }

    pub(crate) fn require_execution_restart(
        &self,
        batch_id: BatchId,
    ) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::Materializing {
                batch_id: expected, ..
            }
            | ProducerOperationState::AwaitingDriver {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Submitted { .. }
            | ProducerOperationState::RetryWaiting { .. } => Err(TransitionError::InvalidState),
        }
    }
}
