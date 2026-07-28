//! Sole deterministic owner of one transactional producer lifecycle.

use core::fmt;

use crate::{OperationId, TransactionalOwnerId};

use super::{TransactionEndMode, TransactionEpoch, TransactionLifecycleState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingTransactionEnd {
    pub(super) mode: TransactionEndMode,
    pub(super) operation_id: Option<OperationId>,
}

/// Linear owner of one active transaction fence and pending terminal intent.
#[derive(Debug)]
pub struct TransactionLifecycleMachine {
    pub(super) owner_id: TransactionalOwnerId,
    pub(super) state: TransactionLifecycleState,
    pub(super) next_epoch: Option<TransactionEpoch>,
    pub(super) active_epoch: Option<TransactionEpoch>,
    pub(super) pending_end: Option<PendingTransactionEnd>,
    pub(super) owner_lost: bool,
}

impl TransactionLifecycleMachine {
    /// Creates one initialized, idle transactional producer owner.
    pub const fn new(owner_id: TransactionalOwnerId) -> Self {
        Self {
            owner_id,
            state: TransactionLifecycleState::Idle,
            next_epoch: Some(TransactionEpoch::initial()),
            active_epoch: None,
            pending_end: None,
            owner_lost: false,
        }
    }

    /// Returns the engine-supplied producer owner fence.
    pub const fn owner_id(&self) -> TransactionalOwnerId {
        self.owner_id
    }

    /// Returns the current closed lifecycle stage.
    pub const fn state(&self) -> TransactionLifecycleState {
        self.state
    }

    /// Returns the current transaction fence, when one remains owned.
    pub const fn active_epoch(&self) -> Option<TransactionEpoch> {
        self.active_epoch
    }
}

/// Rejected owner, epoch, or lifecycle fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleMachineError {
    /// A stale or foreign producer owner supplied the fact.
    OwnerMismatch {
        /// Owner retained by this machine.
        expected: TransactionalOwnerId,
        /// Owner supplied with the fact.
        supplied: TransactionalOwnerId,
    },
    /// The fact does not belong to the current lifecycle stage.
    InvalidState {
        /// Stage retained without mutation.
        state: TransactionLifecycleState,
    },
    /// A stale transaction handle supplied the fact.
    EpochMismatch {
        /// Current transaction fence.
        expected: TransactionEpoch,
        /// Fence supplied with the fact.
        supplied: TransactionEpoch,
    },
    /// The core-owned transaction epoch domain is exhausted.
    EpochExhausted,
}

impl fmt::Display for TransactionLifecycleMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transaction lifecycle rejected fact: {self:?}")
    }
}

impl std::error::Error for TransactionLifecycleMachineError {}
