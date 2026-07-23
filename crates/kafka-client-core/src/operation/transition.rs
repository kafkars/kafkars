//! Sole mutation owner for producer-operation lifecycle transitions.

use crate::{BatchId, DeliveryStatus, TerminalRelease, TransitionError};

use super::{ProducerOperation, ProducerOperationState};

impl ProducerOperation {
    /// Transfers the record into the bounded producer buffer budget.
    pub fn admit(&mut self) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { deadline, bytes } => {
                self.state = ProducerOperationState::Accumulating { deadline, bytes };
                Ok(())
            }
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Marks the containing batch as ready for driver submission.
    pub fn mark_ready(&mut self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::Accumulating { deadline, bytes } => {
                self.state = ProducerOperationState::AwaitingDriver {
                    deadline,
                    bytes,
                    batch_id,
                };
                Ok(())
            }
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Marks the containing batch as accepted by the driver.
    ///
    /// Submission alone does not imply `PossiblySent`. The driver's terminal
    /// result remains authoritative for delivery certainty.
    pub fn mark_submitted(&mut self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::AwaitingDriver {
                deadline,
                bytes,
                batch_id: expected,
            } if expected == batch_id => {
                self.state = ProducerOperationState::Submitted {
                    deadline,
                    bytes,
                    batch_id,
                };
                Ok(())
            }
            ProducerOperationState::AwaitingDriver { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Rejects an operation before local capacity has been granted.
    pub fn reject(&mut self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { .. } => self.finish(),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Completes a submitted operation after broker acknowledgment.
    pub fn complete_delivered(&mut self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::Submitted { .. } => self.finish(),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::AwaitingDriver { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Completes failed work with driver-owned delivery certainty.
    pub fn complete_failed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<TerminalRelease, TransitionError> {
        match (self.state, delivery) {
            (
                ProducerOperationState::Accumulating { .. }
                | ProducerOperationState::AwaitingDriver { .. },
                DeliveryStatus::NotSent,
            )
            | (ProducerOperationState::Submitted { .. }, _) => self.finish(),
            (
                ProducerOperationState::Accumulating { .. }
                | ProducerOperationState::AwaitingDriver { .. },
                DeliveryStatus::PossiblySent,
            )
            | (ProducerOperationState::WaitingForCapacity { .. }, _) => {
                Err(TransitionError::InvalidState)
            }
            (ProducerOperationState::Completed, _) => Err(TransitionError::AlreadyCompleted),
        }
    }

    /// Expires work that has not yet entered driver ownership.
    pub fn expire(&mut self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::AwaitingDriver { .. } => self.finish(),
            ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
        }
    }

    pub(crate) fn plan_delivered(&self) -> Result<TerminalRelease, TransitionError> {
        match self.state {
            ProducerOperationState::Submitted { .. } => self.plan_finish(),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
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
                | ProducerOperationState::AwaitingDriver { .. },
                DeliveryStatus::NotSent,
            )
            | (ProducerOperationState::Submitted { .. }, _) => self.plan_finish(),
            (
                ProducerOperationState::Accumulating { .. }
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
            | ProducerOperationState::AwaitingDriver { .. } => self.plan_finish(),
            ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
        }
    }

    pub(crate) fn commit_terminal(&mut self) {
        self.state = ProducerOperationState::Completed;
    }

    fn finish(&mut self) -> Result<TerminalRelease, TransitionError> {
        let release = self.plan_finish()?;
        self.commit_terminal();
        Ok(release)
    }

    fn plan_finish(&self) -> Result<TerminalRelease, TransitionError> {
        let released_bytes = match self.state {
            ProducerOperationState::WaitingForCapacity { .. } => None,
            ProducerOperationState::Accumulating { bytes, .. }
            | ProducerOperationState::AwaitingDriver { bytes, .. }
            | ProducerOperationState::Submitted { bytes, .. } => Some(bytes),
            ProducerOperationState::Completed => return Err(TransitionError::AlreadyCompleted),
        };
        Ok(TerminalRelease { released_bytes })
    }
}
