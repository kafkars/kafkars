//! Closed identities, stages, states, and consequences for transactional offset transfer.

/// Core-owned nonreused identity for one transactional offset-transfer attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransactionOffsetCommitId(u64);

impl TransactionOffsetCommitId {
    pub(super) const fn initial() -> Self {
        Self(1)
    }

    pub(super) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn from_raw_for_test(value: u64) -> Self {
        Self(value)
    }

    /// Returns the core-owned monotonic scalar.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Kafka request stage within one transactional offset transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitStage {
    /// Add the exact consumer group to the active transaction.
    AddOffsets,
    /// Commit the exact assignment-fenced offsets within that transaction.
    TxnOffsetCommit,
}

/// Current ownership stage for the capacity-one offset-transfer owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitState {
    /// No transactional offset operation remains unsettled.
    Idle,
    /// One `AddOffsetsToTxn` request awaits driver admission.
    AddOffsetsAdmitted,
    /// The driver owns `AddOffsetsToTxn` and core awaits its terminal.
    AddOffsetsAwaiting,
    /// One `TxnOffsetCommit` request awaits driver admission.
    TxnOffsetCommitAdmitted,
    /// The driver owns `TxnOffsetCommit` and core awaits its terminal.
    TxnOffsetCommitAwaiting,
}

/// Transaction health consequence of one driver-accepted request failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitConsequence {
    /// The active transaction may now only be aborted.
    AbortRequired,
    /// Transactional producer ownership is irrecoverably fenced.
    Fatal,
}

/// Sole terminal decision for one transactional offset-transfer attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitTerminal {
    /// Both broker request stages succeeded.
    Succeeded,
    /// The named stage was definitely rejected before driver ownership.
    RejectedNotSent {
        /// Stage whose request never reached driver ownership.
        stage: TransactionOffsetCommitStage,
    },
    /// A driver-accepted stage failed with an explicit transaction consequence.
    Failed {
        /// Stage whose accepted request failed.
        stage: TransactionOffsetCommitStage,
        /// Required lifecycle consequence.
        consequence: TransactionOffsetCommitConsequence,
    },
}

/// Explicit transaction-end preflight fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitEndBarrier {
    /// No offset transfer prevents `EndTxn`.
    Ready,
    /// One exact offset transfer must terminally drain first.
    Unsettled {
        /// Nonreused operation preventing transaction end.
        operation_id: TransactionOffsetCommitId,
        /// Exact stage retaining request ownership.
        state: TransactionOffsetCommitState,
    },
}
