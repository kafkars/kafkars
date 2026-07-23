//! Producer-operation ownership from public creation through terminal completion.

use core::fmt;

use crate::{ByteCount, Deadline, OperationId};

/// Certainty attached to a failed producer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// The operation did not cross the transport ownership boundary.
    NotSent,
    /// The operation may have reached Kafka and a blind retry may duplicate it.
    PossiblySent,
}

/// Terminal producer result retained for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerCompletion {
    /// Kafka acknowledged the record batch containing the operation.
    Delivered,
    /// The operation failed with explicit delivery certainty.
    Failed(DeliveryStatus),
}

/// Current ownership stage for one producer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerOperationState {
    /// The submission envelope retains the record but no producer budget is charged.
    WaitingForCapacity {
        /// Absolute deadline created at the public boundary.
        deadline: Deadline,
        /// Bytes retained by the operation envelope.
        bytes: ByteCount,
    },
    /// The engine admitted the record into a local partition accumulator.
    Accumulating {
        /// Absolute deadline created at the public boundary.
        deadline: Deadline,
        /// Bytes charged to the producer buffer budget.
        bytes: ByteCount,
    },
    /// The containing batch was accepted by the driver.
    Submitted {
        /// Absolute deadline created at the public boundary.
        deadline: Deadline,
        /// Bytes charged to the producer buffer budget.
        bytes: ByteCount,
    },
    /// One terminal completion has been retained.
    Completed,
}

/// Effects emitted exactly once when an operation becomes terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalEffects {
    completion: ProducerCompletion,
    released_bytes: Option<ByteCount>,
}

impl TerminalEffects {
    /// Returns the completion retained for the observer.
    pub const fn completion(self) -> ProducerCompletion {
        self.completion
    }

    /// Returns bytes released from the admitted producer budget, when any.
    pub const fn released_bytes(self) -> Option<ByteCount> {
        self.released_bytes
    }
}

/// Rejected producer-operation state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionError {
    /// The requested action does not belong to the current lifecycle stage.
    InvalidState,
    /// The operation already owns a terminal completion.
    AlreadyCompleted,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState => formatter.write_str("invalid producer operation state"),
            Self::AlreadyCompleted => formatter.write_str("producer operation already completed"),
        }
    }
}

impl std::error::Error for TransitionError {}

/// Deterministic lifecycle owner for one producer record operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerOperation {
    id: OperationId,
    state: ProducerOperationState,
}

impl ProducerOperation {
    /// Creates an operation whose deadline begins at the public API boundary.
    pub const fn new(id: OperationId, deadline: Deadline, bytes: ByteCount) -> Self {
        Self {
            id,
            state: ProducerOperationState::WaitingForCapacity { deadline, bytes },
        }
    }

    pub(crate) const fn admitted(id: OperationId, deadline: Deadline, bytes: ByteCount) -> Self {
        Self {
            id,
            state: ProducerOperationState::Accumulating { deadline, bytes },
        }
    }

    /// Returns the operation identity.
    pub const fn id(self) -> OperationId {
        self.id
    }

    /// Returns the current lifecycle state.
    pub const fn state(self) -> ProducerOperationState {
        self.state
    }

    /// Transfers the record into the bounded producer buffer budget.
    pub fn admit(&mut self) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { deadline, bytes } => {
                self.state = ProducerOperationState::Accumulating { deadline, bytes };
                Ok(())
            }
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Marks the containing batch as accepted by the driver.
    ///
    /// Submission alone does not imply `PossiblySent`. The driver's terminal
    /// result remains authoritative for delivery certainty.
    pub fn mark_submitted(&mut self) -> Result<(), TransitionError> {
        match self.state {
            ProducerOperationState::Accumulating { deadline, bytes } => {
                self.state = ProducerOperationState::Submitted { deadline, bytes };
                Ok(())
            }
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Rejects an operation before local capacity has been granted.
    pub fn reject(&mut self) -> Result<TerminalEffects, TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { .. } => {
                self.finish(ProducerCompletion::Failed(DeliveryStatus::NotSent))
            }
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Completes a submitted operation after broker acknowledgment.
    pub fn complete_delivered(&mut self) -> Result<TerminalEffects, TransitionError> {
        match self.state {
            ProducerOperationState::Submitted { .. } => self.finish(ProducerCompletion::Delivered),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. } => Err(TransitionError::InvalidState),
        }
    }

    /// Completes failed work with driver-owned delivery certainty.
    pub fn complete_failed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<TerminalEffects, TransitionError> {
        match (self.state, delivery) {
            (ProducerOperationState::Accumulating { .. }, DeliveryStatus::NotSent)
            | (ProducerOperationState::Submitted { .. }, _) => {
                self.finish(ProducerCompletion::Failed(delivery))
            }
            (ProducerOperationState::Accumulating { .. }, DeliveryStatus::PossiblySent)
            | (ProducerOperationState::WaitingForCapacity { .. }, _) => {
                Err(TransitionError::InvalidState)
            }
            (ProducerOperationState::Completed, _) => Err(TransitionError::AlreadyCompleted),
        }
    }

    /// Expires work that has not yet entered driver ownership.
    ///
    /// A submitted operation is settled by the driver so the client cannot
    /// invent delivery certainty from timing alone.
    pub fn expire(&mut self) -> Result<TerminalEffects, TransitionError> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. } => {
                self.finish(ProducerCompletion::Failed(DeliveryStatus::NotSent))
            }
            ProducerOperationState::Submitted { .. } => Err(TransitionError::InvalidState),
            ProducerOperationState::Completed => Err(TransitionError::AlreadyCompleted),
        }
    }

    fn finish(
        &mut self,
        completion: ProducerCompletion,
    ) -> Result<TerminalEffects, TransitionError> {
        let released_bytes = match self.state {
            ProducerOperationState::WaitingForCapacity { .. } => None,
            ProducerOperationState::Accumulating { bytes, .. }
            | ProducerOperationState::Submitted { bytes, .. } => Some(bytes),
            ProducerOperationState::Completed => return Err(TransitionError::AlreadyCompleted),
        };

        self.state = ProducerOperationState::Completed;
        Ok(TerminalEffects {
            completion,
            released_bytes,
        })
    }
}
