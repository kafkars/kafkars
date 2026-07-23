//! Sole mutation owner for producer-operation lifecycle transitions.

use crate::{BatchId, DeliveryStatus, TerminalRelease, TransitionError};

use super::{ProducerOperation, ProducerOperationState};

impl ProducerOperation {
    /// Transfers a capacity reservation into one core-owned batch.
    pub fn admit(&mut self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { deadline, bytes } => {
                self.state = ProducerOperationState::Accumulating {
                    deadline,
                    bytes,
                    batch_id,
                };
                Ok(())
            }
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Marks the containing batch sealed for materialization.
    pub fn mark_ready(&mut self, batch_id: BatchId) -> Result<(), TransitionError> {
        self.require_accumulating(batch_id)?;
        self.commit_ready(batch_id);
        Ok(())
    }

    pub(crate) fn require_accumulating(&self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::Accumulating {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::Accumulating { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    pub(crate) fn commit_ready(&mut self, batch_id: BatchId) {
        let ProducerOperationState::Accumulating {
            deadline,
            bytes,
            batch_id: expected,
        } = self.state
        else {
            debug_assert!(false, "ready commit must follow preflight");
            return;
        };
        debug_assert_eq!(expected, batch_id);
        self.state = ProducerOperationState::Materializing {
            deadline,
            bytes,
            batch_id,
        };
    }

    /// Marks wire-records materialization complete before driver admission.
    pub fn mark_materialized(&mut self, batch_id: BatchId) -> Result<(), TransitionError> {
        self.require_materializing(batch_id)?;
        self.commit_materialized(batch_id);
        Ok(())
    }

    pub(crate) fn require_materializing(&self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::Materializing {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::Materializing { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    pub(crate) fn commit_materialized(&mut self, batch_id: BatchId) {
        let ProducerOperationState::Materializing {
            deadline,
            bytes,
            batch_id: expected,
        } = self.state
        else {
            debug_assert!(false, "materialized commit must follow preflight");
            return;
        };
        debug_assert_eq!(expected, batch_id);
        self.state = ProducerOperationState::AwaitingDriver {
            deadline,
            bytes,
            batch_id,
        };
    }

    /// Marks the containing request accepted by the driver.
    pub fn mark_submitted(&mut self, batch_id: BatchId) -> Result<(), TransitionError> {
        self.require_awaiting_driver(batch_id)?;
        self.commit_submitted(batch_id);
        Ok(())
    }

    pub(crate) fn require_awaiting_driver(&self, batch_id: BatchId) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::AwaitingDriver {
                batch_id: expected, ..
            } if expected == batch_id => Ok(()),
            ProducerOperationState::AwaitingDriver { .. } => Err(TransitionError::BatchMismatch),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    pub(crate) fn commit_submitted(&mut self, batch_id: BatchId) {
        let ProducerOperationState::AwaitingDriver {
            deadline,
            bytes,
            batch_id: expected,
        } = self.state
        else {
            debug_assert!(false, "submitted commit must follow preflight");
            return;
        };
        debug_assert_eq!(expected, batch_id);
        self.state = ProducerOperationState::Submitted {
            deadline,
            bytes,
            batch_id,
        };
    }

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

    pub(crate) fn commit_execution_restart(&mut self, batch_id: BatchId) {
        let (deadline, bytes, expected) = match self.state {
            ProducerOperationState::Materializing {
                deadline,
                bytes,
                batch_id,
            }
            | ProducerOperationState::AwaitingDriver {
                deadline,
                bytes,
                batch_id,
            } => (deadline, bytes, batch_id),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Submitted { .. }
            | ProducerOperationState::Completed => {
                debug_assert!(false, "restart commit must follow preflight");
                return;
            }
        };
        debug_assert_eq!(expected, batch_id);
        self.state = ProducerOperationState::Materializing {
            deadline,
            bytes,
            batch_id,
        };
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
