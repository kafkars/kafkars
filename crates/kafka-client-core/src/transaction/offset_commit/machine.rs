//! Capacity-one deterministic owner for transactional offset transfer.

use core::fmt;

use crate::{Deadline, GroupPositionFence, TransactionEpoch};

use super::{
    TransactionOffsetCommitEndBarrier, TransactionOffsetCommitId, TransactionOffsetCommitStage,
    TransactionOffsetCommitState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PendingTransactionOffsetCommit {
    pub(super) epoch: TransactionEpoch,
    pub(super) operation_id: TransactionOffsetCommitId,
    pub(super) deadline: Deadline,
    pub(super) group_fence: GroupPositionFence,
}

/// Producer-lifetime owner of at most one transactional offset transfer.
#[derive(Debug)]
pub struct TransactionOffsetCommitMachine {
    pub(super) state: TransactionOffsetCommitState,
    pub(super) next_operation_id: Option<TransactionOffsetCommitId>,
    pub(super) pending: Option<PendingTransactionOffsetCommit>,
}

impl TransactionOffsetCommitMachine {
    /// Creates one idle capacity-one owner.
    pub const fn new() -> Self {
        Self {
            state: TransactionOffsetCommitState::Idle,
            next_operation_id: Some(TransactionOffsetCommitId::initial()),
            pending: None,
        }
    }

    /// Returns the exact current ownership stage.
    pub const fn state(&self) -> TransactionOffsetCommitState {
        self.state
    }

    /// Returns the unsettled operation identity, when present.
    pub const fn operation_id(&self) -> Option<TransactionOffsetCommitId> {
        match self.pending {
            Some(pending) => Some(pending.operation_id),
            None => None,
        }
    }

    /// Returns the original absolute deadline, when unsettled.
    pub const fn deadline(&self) -> Option<Deadline> {
        match self.pending {
            Some(pending) => Some(pending.deadline),
            None => None,
        }
    }

    /// Returns the exact consumer assignment fence, when unsettled.
    pub const fn group_fence(&self) -> Option<GroupPositionFence> {
        match self.pending {
            Some(pending) => Some(pending.group_fence),
            None => None,
        }
    }

    /// Reports whether the exact transaction may begin `EndTxn`.
    pub fn preflight_end(
        &self,
        epoch: TransactionEpoch,
    ) -> Result<TransactionOffsetCommitEndBarrier, TransactionOffsetCommitMachineError> {
        let Some(pending) = self.pending else {
            return Ok(TransactionOffsetCommitEndBarrier::Ready);
        };
        Self::require_epoch(pending, epoch)?;
        Ok(TransactionOffsetCommitEndBarrier::Unsettled {
            operation_id: pending.operation_id,
            state: self.state,
        })
    }

    pub(super) fn require_epoch(
        pending: PendingTransactionOffsetCommit,
        supplied: TransactionEpoch,
    ) -> Result<(), TransactionOffsetCommitMachineError> {
        if pending.epoch == supplied {
            Ok(())
        } else {
            Err(TransactionOffsetCommitMachineError::EpochMismatch {
                expected: pending.epoch,
                supplied,
            })
        }
    }
}

impl Default for TransactionOffsetCommitMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Rejected identity, correlation, or lifecycle fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitMachineError {
    /// The current stage cannot accept the supplied fact.
    InvalidState {
        /// Exact state retained without mutation.
        state: TransactionOffsetCommitState,
    },
    /// No offset-transfer operation owns the supplied terminal fact.
    NoOperation,
    /// A stale transaction fence supplied the fact.
    EpochMismatch {
        /// Retained active transaction fence.
        expected: TransactionEpoch,
        /// Supplied transaction fence.
        supplied: TransactionEpoch,
    },
    /// A stale or foreign operation identity supplied the fact.
    OperationMismatch {
        /// Retained nonreused identity.
        expected: TransactionOffsetCommitId,
        /// Supplied identity.
        supplied: TransactionOffsetCommitId,
    },
    /// A terminal fact named the wrong request stage.
    StageMismatch {
        /// Stage implied by the retained state.
        expected: TransactionOffsetCommitStage,
        /// Stage supplied with the fact.
        supplied: TransactionOffsetCommitStage,
    },
    /// No later nonreused operation identity can be represented.
    IdentityExhausted,
}

impl fmt::Display for TransactionOffsetCommitMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transaction offset commit rejected fact: {self:?}"
        )
    }
}

impl std::error::Error for TransactionOffsetCommitMachineError {}
