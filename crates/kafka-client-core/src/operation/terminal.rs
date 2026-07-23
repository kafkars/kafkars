//! Sole owner of producer-operation terminal preflight and commitment.

use crate::{DeliveryStatus, TerminalRelease, TransitionError};

use super::{ProducerOperation, ProducerOperationState};

impl ProducerOperation {
    pub(crate) fn plan_delivered(&self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::Submitted { .. } => self.plan_finish(),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. } => Err(TransitionError::InvalidState),
        }
    }

    pub(crate) fn plan_failed(
        &self,
        delivery: DeliveryStatus,
    ) -> Result<TerminalRelease, TransitionError> {
        match (self.state, delivery) {
            (
                ProducerOperationState::Accumulating { .. }
                | ProducerOperationState::Materializing { .. }
                | ProducerOperationState::AwaitingDriver { .. },
                DeliveryStatus::NotSent,
            )
            | (ProducerOperationState::Submitted { .. }, _) => self.plan_finish(),
            (
                ProducerOperationState::Accumulating { .. }
                | ProducerOperationState::Materializing { .. }
                | ProducerOperationState::AwaitingDriver { .. },
                DeliveryStatus::PossiblySent,
            )
            | (ProducerOperationState::WaitingForCapacity { .. }, _) => {
                Err(TransitionError::InvalidState)
            }
            (ProducerOperationState::Completed, _) => Err(TransitionError::AlreadyCompleted),
        }
    }

    pub(crate) fn plan_expire(&self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. } => self.plan_finish(),
            ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
        }
    }

    pub(crate) fn commit_terminal(&mut self) {
        self.state = ProducerOperationState::Completed;
    }

    pub(crate) fn plan_finish(&self) -> Result<TerminalRelease, TransitionError> {
        let released_bytes = match self.state {
            ProducerOperationState::WaitingForCapacity { .. } => None,
            ProducerOperationState::Accumulating { bytes, .. }
            | ProducerOperationState::Materializing { bytes, .. }
            | ProducerOperationState::AwaitingDriver { bytes, .. }
            | ProducerOperationState::Submitted { bytes, .. } => Some(bytes),
            ProducerOperationState::Completed => return Err(TransitionError::AlreadyCompleted),
        };
        Ok(TerminalRelease { released_bytes })
    }
}
