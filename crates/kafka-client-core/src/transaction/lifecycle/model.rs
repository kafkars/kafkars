//! Scalar identities and closed outcomes for one active transaction.

/// Nonreused core-owned fence for one active transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionEpoch(u64);

impl TransactionEpoch {
    pub(super) const fn initial() -> Self {
        Self(1)
    }

    pub(super) const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the core-owned monotonic scalar.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Kafka `EndTxn` intent selected by deterministic policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEndMode {
    /// Make the transaction's writes visible atomically.
    Commit,
    /// Discard the transaction's writes atomically.
    Abort,
}

/// Whether one `EndTxn` terminal has a public operation observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEndObservation {
    /// Explicit commit or abort owns a reserved public completion.
    Observed,
    /// Owner loss runs abort cleanup without a public success terminal.
    BestEffort,
}

/// Deterministic terminal consequence of one submitted `EndTxn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEndOutcome {
    /// Kafka accepted the requested commit or abort.
    Succeeded,
    /// Settlement irrecoverably fenced the transactional owner.
    Fatal,
}

/// Publicly observable terminal for an explicit commit or abort operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleTerminal {
    /// Explicit commit completed successfully.
    Committed,
    /// Explicit abort completed successfully.
    Aborted,
    /// The owner became permanently unusable.
    Fatal,
}
