//! Ordered mechanism requests emitted by transaction lifecycle policy.

use crate::{OperationId, TransactionalOwnerId};

use super::{
    TransactionEndMode, TransactionEndObservation, TransactionEpoch, TransactionLifecycleTerminal,
    TransactionSendAttempt, TransactionSendId, TransactionSendIdentity,
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
    /// Publish the first transition into abort-required state.
    AbortRequired {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Active transaction fence.
        epoch: TransactionEpoch,
    },
    /// Replace one correlated broker-rejected execution after bounded backoff.
    ReplaceSendAttempt {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Stable accepted-send identity whose bytes remain mechanism-owned.
        send_id: TransactionSendId,
        /// Correlated execution generation that failed.
        previous: TransactionSendAttempt,
        /// Fresh generation authorized to reuse the exact immutable send shape.
        replacement: TransactionSendAttempt,
        /// Original producer, partition, sequence, and public deadline authority.
        identity: TransactionSendIdentity,
        /// Earliest absolute moment at which the replacement may execute.
        not_before: crate::Deadline,
    },
    /// Terminally cancel accepted sends before submitting abort.
    CancelOutstanding {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Exact number of accepted sends still awaiting settlement.
        outstanding_sends: usize,
        /// Whether abort has a public terminal observer.
        observation: TransactionEndObservation,
    },
    /// Terminally cancel a fatal owner's accepted sends before releasing it.
    CancelFatalOutstanding {
        /// Retained transactional-producer owner.
        owner_id: TransactionalOwnerId,
        /// Fatal transaction fence.
        epoch: TransactionEpoch,
        /// Exact number of accepted sends still awaiting settlement.
        outstanding_sends: usize,
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
        /// Exact public terminal, present with an explicit end operation.
        terminal: Option<TransactionLifecycleTerminal>,
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
    pub(super) const fn none() -> Self {
        Self { effect: None }
    }

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
