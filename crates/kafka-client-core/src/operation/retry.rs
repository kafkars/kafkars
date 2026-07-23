//! Sole operation-state mutation owner for definitely-unsent retry stages.

use crate::{BatchId, TransitionError};

use super::{ProducerOperation, ProducerOperationState};

impl ProducerOperation {
    pub(crate) fn require_submitted(&self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::Submitted {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::Submitted { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::RetryWaiting { .. } => Err(TransitionError::InvalidState),
        }
    }

    pub(crate) fn commit_retry_waiting(&mut self, batch_id: BatchId) {
        let (deadline, bytes, expected) = match self.state {
            ProducerOperationState::AwaitingDriver {
                deadline,
                bytes,
                batch_id,
            }
            | ProducerOperationState::Submitted {
                deadline,
                bytes,
                batch_id,
            } => (deadline, bytes, batch_id),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::RetryWaiting { .. }
            | ProducerOperationState::Completed => {
                debug_assert!(false, "retry-wait commit must follow preflight");
                return;
            }
        };
        debug_assert_eq!(expected, batch_id);
        self.state = ProducerOperationState::RetryWaiting {
            deadline,
            bytes,
            batch_id,
        };
    }

    pub(crate) fn require_retry_waiting(&self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::RetryWaiting {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::RetryWaiting { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    pub(crate) fn commit_retry_ready(&mut self, batch_id: BatchId) {
        let ProducerOperationState::RetryWaiting {
            deadline,
            bytes,
            batch_id: expected,
        } = self.state
        else {
            debug_assert!(false, "retry-ready commit must follow preflight");
            return;
        };
        debug_assert_eq!(expected, batch_id);
        self.state = ProducerOperationState::Materializing {
            deadline,
            bytes,
            batch_id,
        };
    }
}
