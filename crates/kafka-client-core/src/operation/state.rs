//! Producer-operation identity and externally observable ownership stages.

use crate::{BatchId, ByteCount, Deadline, OperationId};

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
        /// Core-owned batch membership assigned before the accumulation effect.
        batch_id: BatchId,
    },
    /// Core sealed the batch and requested wire-records materialization.
    Materializing {
        /// Absolute deadline created at the public boundary.
        deadline: Deadline,
        /// Bytes charged to the producer buffer budget.
        bytes: ByteCount,
        /// Core-owned batch being materialized.
        batch_id: BatchId,
    },
    /// The engine materialized a batch and was instructed to submit it.
    AwaitingDriver {
        /// Absolute deadline created at the public boundary.
        deadline: Deadline,
        /// Bytes charged to the producer buffer budget.
        bytes: ByteCount,
        /// Engine-owned batch awaiting driver admission.
        batch_id: BatchId,
    },
    /// The containing batch was accepted by the driver.
    Submitted {
        /// Absolute deadline created at the public boundary.
        deadline: Deadline,
        /// Bytes charged to the producer buffer budget.
        bytes: ByteCount,
        /// Engine-owned batch accepted by the driver.
        batch_id: BatchId,
    },
    /// One terminal completion has been retained.
    Completed,
}

/// Deterministic lifecycle owner for one producer record operation.
#[derive(Debug, PartialEq, Eq)]
pub struct ProducerOperation {
    pub(super) id: OperationId,
    pub(super) state: ProducerOperationState,
}

impl ProducerOperation {
    /// Creates an operation whose deadline begins at the public API boundary.
    pub const fn new(id: OperationId, deadline: Deadline, bytes: ByteCount) -> Self {
        Self {
            id,
            state: ProducerOperationState::WaitingForCapacity { deadline, bytes },
        }
    }

    pub(crate) const fn admitted(
        id: OperationId,
        deadline: Deadline,
        bytes: ByteCount,
        batch_id: BatchId,
    ) -> Self {
        Self {
            id,
            state: ProducerOperationState::Accumulating {
                deadline,
                bytes,
                batch_id,
            },
        }
    }

    /// Returns the operation identity.
    pub const fn id(&self) -> OperationId {
        self.id
    }

    /// Returns the current lifecycle state.
    pub const fn state(&self) -> ProducerOperationState {
        self.state
    }

    /// Returns the absolute deadline while this operation remains active.
    pub const fn deadline(&self) -> Option<Deadline> {
        match self.state {
            ProducerOperationState::WaitingForCapacity { deadline, .. }
            | ProducerOperationState::Accumulating { deadline, .. }
            | ProducerOperationState::Materializing { deadline, .. }
            | ProducerOperationState::AwaitingDriver { deadline, .. }
            | ProducerOperationState::Submitted { deadline, .. } => Some(deadline),
            ProducerOperationState::Completed => None,
        }
    }

    /// Returns the batch identity after materialization.
    pub const fn batch_id(&self) -> Option<BatchId> {
        match self.state {
            ProducerOperationState::Accumulating { batch_id, .. }
            | ProducerOperationState::Materializing { batch_id, .. }
            | ProducerOperationState::AwaitingDriver { batch_id, .. }
            | ProducerOperationState::Submitted { batch_id, .. } => Some(batch_id),
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Completed => None,
        }
    }
}
