//! Sole deterministic owner of one transactional initialization lifecycle.

use core::fmt;

use crate::{Deadline, DeliveryStatus, OperationId};

use super::{TransactionInitializationPlan, TransactionInitializationState, TransactionalOwnerId};

/// Deterministic owner created only after engine terminal reservation.
#[derive(Debug)]
pub struct TransactionInitializationMachine {
    pub(super) owner_id: TransactionalOwnerId,
    pub(super) operation_id: OperationId,
    pub(super) deadline: Deadline,
    pub(super) plan: TransactionInitializationPlan,
    pub(super) state: TransactionInitializationState,
    pub(super) delivery_floor: DeliveryStatus,
}

impl TransactionInitializationMachine {
    /// Creates one accepted transaction-initialization operation.
    pub const fn new(
        owner_id: TransactionalOwnerId,
        operation_id: OperationId,
        deadline: Deadline,
        plan: TransactionInitializationPlan,
    ) -> Self {
        Self {
            owner_id,
            operation_id,
            deadline,
            plan,
            state: TransactionInitializationState::Ready,
            delivery_floor: DeliveryStatus::NotSent,
        }
    }

    /// Returns the owner fence supplied at construction.
    pub const fn owner_id(&self) -> TransactionalOwnerId {
        self.owner_id
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> TransactionInitializationState {
        self.state
    }
}

/// Rejected owner or lifecycle fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationMachineError {
    /// A stale or foreign owner supplied the fact.
    OwnerMismatch {
        /// Owner retained by this machine.
        expected: TransactionalOwnerId,
        /// Owner supplied with the fact.
        supplied: TransactionalOwnerId,
    },
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// Core already assigned the sole terminal outcome.
    AlreadyCompleted,
}

impl fmt::Display for TransactionInitializationMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transaction initialization rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for TransactionInitializationMachineError {}
