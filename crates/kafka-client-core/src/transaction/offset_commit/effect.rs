//! Ordered mechanism requests emitted by transactional offset-transfer policy.

use crate::{Deadline, GroupPositionFence, TransactionEpoch};

use super::{TransactionOffsetCommitId, TransactionOffsetCommitTerminal};

/// One concrete request emitted by deterministic offset-transfer policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitEffect {
    /// Submit `AddOffsetsToTxn` with the exact retained correlation.
    SubmitAddOffsets {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Original absolute deadline.
        deadline: Deadline,
        /// Exact consumer assignment fence.
        group_fence: GroupPositionFence,
    },
    /// Submit `TxnOffsetCommit` with the same deadline and fences.
    SubmitTxnOffsetCommit {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Original absolute deadline.
        deadline: Deadline,
        /// Exact consumer assignment fence.
        group_fence: GroupPositionFence,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Original absolute deadline retained through settlement.
        deadline: Deadline,
        /// Exact consumer assignment fence retained through settlement.
        group_fence: GroupPositionFence,
        /// Core-owned terminal decision.
        terminal: TransactionOffsetCommitTerminal,
    },
}

/// Ordered result of one atomic offset-transfer transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionOffsetCommitTransition {
    effect: Option<TransactionOffsetCommitEffect>,
}

impl TransactionOffsetCommitTransition {
    /// Creates a transition with no mechanism request.
    pub const fn none() -> Self {
        Self { effect: None }
    }

    /// Creates a transition containing one ordered mechanism request.
    pub const fn one(effect: TransactionOffsetCommitEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional mechanism request.
    pub fn into_effect(self) -> Option<TransactionOffsetCommitEffect> {
        self.effect
    }
}
