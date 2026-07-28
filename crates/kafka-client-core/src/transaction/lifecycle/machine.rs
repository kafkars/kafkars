//! Sole deterministic owner of one transactional producer lifecycle.

use core::fmt;
use std::collections::BTreeMap;

use crate::{OperationId, ProducerRetryPolicy, TransactionalOwnerId};

use super::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleState, TransactionSendAttempt,
    TransactionSendId, TransactionSendIdentity,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingTransactionEnd {
    pub(super) mode: TransactionEndMode,
    pub(super) operation_id: Option<OperationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OutstandingTransactionSend {
    pub(super) identity: Option<TransactionSendIdentity>,
    pub(super) attempt: TransactionSendAttempt,
    pub(super) replacements_started: u32,
}

impl OutstandingTransactionSend {
    pub(super) const fn accepted() -> Self {
        Self {
            identity: None,
            attempt: TransactionSendAttempt::initial(),
            replacements_started: 0,
        }
    }
}

/// Linear owner of one active transaction and its accepted-send fences.
#[derive(Debug)]
pub struct TransactionLifecycleMachine {
    pub(super) owner_id: TransactionalOwnerId,
    pub(super) state: TransactionLifecycleState,
    pub(super) next_epoch: Option<TransactionEpoch>,
    pub(super) active_epoch: Option<TransactionEpoch>,
    pub(super) outstanding_sends: BTreeMap<TransactionSendId, OutstandingTransactionSend>,
    pub(super) send_retry_policy: ProducerRetryPolicy,
    pub(super) pending_end: Option<PendingTransactionEnd>,
    pub(super) owner_lost: bool,
}

impl TransactionLifecycleMachine {
    /// Creates one initialized, idle transactional producer owner.
    pub fn new(owner_id: TransactionalOwnerId) -> Self {
        Self::with_send_retry_policy(owner_id, ProducerRetryPolicy::none())
    }

    /// Creates one initialized owner with the sole transactional send retry policy.
    pub fn with_send_retry_policy(
        owner_id: TransactionalOwnerId,
        send_retry_policy: ProducerRetryPolicy,
    ) -> Self {
        Self {
            owner_id,
            state: TransactionLifecycleState::Idle,
            next_epoch: Some(TransactionEpoch::initial()),
            active_epoch: None,
            outstanding_sends: BTreeMap::new(),
            send_retry_policy,
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

    /// Returns the exact number of accepted sends not yet settled.
    pub fn outstanding_send_count(&self) -> usize {
        self.outstanding_sends.len()
    }

    /// Validates one exact retained send terminal without mutating ownership.
    pub fn preflight_send_settlement(
        &self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<(), TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if !matches!(
            self.state,
            TransactionLifecycleState::Active
                | TransactionLifecycleState::AbortRequired
                | TransactionLifecycleState::DrainingAbort
                | TransactionLifecycleState::Fatal
                | TransactionLifecycleState::DrainingFatal
        ) {
            return Err(self.invalid_state());
        }
        if !self.outstanding_sends.contains_key(&send_id) {
            return Err(TransactionLifecycleMachineError::UnknownSend { send_id });
        }
        Ok(())
    }
}

/// Rejected owner, epoch, send, or lifecycle fact.
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
    /// The accepted-send fence is already outstanding.
    DuplicateSend {
        /// Duplicate send fence.
        send_id: TransactionSendId,
    },
    /// The accepted send already retained one immutable idempotent shape.
    DuplicateSendPreparation {
        /// Duplicate prepared send fence.
        send_id: TransactionSendId,
    },
    /// Replacement policy cannot act before an idempotent send shape is retained.
    SendNotPrepared {
        /// Accepted but not yet prepared send fence.
        send_id: TransactionSendId,
    },
    /// A stale or foreign execution generation supplied a failure fact.
    SendAttemptMismatch {
        /// Current execution generation.
        expected: TransactionSendAttempt,
        /// Generation supplied with the fact.
        supplied: TransactionSendAttempt,
    },
    /// The core-owned send execution generation domain is exhausted.
    SendAttemptExhausted,
    /// No outstanding send owns the supplied terminal fact.
    UnknownSend {
        /// Unknown or already-settled send fence.
        send_id: TransactionSendId,
    },
    /// Commit cannot overtake accepted sends.
    OutstandingSends {
        /// Exact outstanding count retained without mutation.
        count: usize,
    },
    /// Abort-required state rejects commit and new send admission.
    AbortRequired,
    /// The core-owned transaction epoch domain is exhausted.
    EpochExhausted,
}

impl fmt::Display for TransactionLifecycleMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transaction lifecycle rejected fact: {self:?}")
    }
}

impl std::error::Error for TransactionLifecycleMachineError {}
