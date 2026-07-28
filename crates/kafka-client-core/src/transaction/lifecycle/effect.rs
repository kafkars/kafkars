//! Ordered mechanism requests emitted by transaction lifecycle policy.

use crate::{OperationId, TransactionalOwnerId};

use super::{
    TransactionEndMode, TransactionEndObservation, TransactionEpoch, TransactionLifecycleTerminal,
};

/// One concrete mechanism request emitted by deterministic transaction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleEffect {
    /// Transfer one newly active transaction fence to its unique handle.
    Began {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Nonreused active transaction fence.
        epoch: TransactionEpoch,
    },
    /// Submit the sole commit or abort request for this transaction.
    EndTransaction {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Ending transaction fence.
        epoch: TransactionEpoch,
        /// Commit or abort intent.
        mode: TransactionEndMode,
        /// Explicit public observation or owner-loss cleanup.
        observation: TransactionEndObservation,
        /// Reserved public operation, absent only for best-effort cleanup.
        operation_id: Option<OperationId>,
    },
    /// Publish one explicit commit or abort terminal.
    Complete {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Ended transaction fence.
        epoch: TransactionEpoch,
        /// Reserved public operation identity.
        operation_id: OperationId,
        /// Deterministic terminal outcome.
        terminal: TransactionLifecycleTerminal,
    },
    /// Fence the owner and any pending explicit end operation.
    EnterFatal {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Transaction whose certainty was lost.
        epoch: TransactionEpoch,
        /// Public end operation requiring fatal completion, when present.
        operation_id: Option<OperationId>,
        /// Whether the public owner was already lost.
        owner_lost: bool,
    },
    /// Reclaim an idle owner or one whose best-effort abort settled.
    ReleaseOwner {
        /// Owner that can no longer expose a transaction success.
        owner_id: TransactionalOwnerId,
    },
}

/// Ordered result of one atomic lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionLifecycleTransition {
    effect: Option<TransactionLifecycleEffect>,
}

impl TransactionLifecycleTransition {
    pub(super) const fn one(effect: TransactionLifecycleEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional mechanism request.
    pub fn into_effect(self) -> Option<TransactionLifecycleEffect> {
        self.effect
    }
}
