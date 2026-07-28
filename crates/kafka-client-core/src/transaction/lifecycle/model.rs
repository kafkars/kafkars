//! Scalar identities and closed outcomes for one active transaction.

use crate::{Deadline, ProducerBrokerFailure, TransactionPartition, TransactionalProducerIdentity};

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

/// Engine-assigned identity for one accepted transactional send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionSendId(u64);

impl TransactionSendId {
    /// Creates one send fence from its engine-assigned scalar.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the engine-assigned scalar.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation fence for one exact transactional send execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionSendAttempt(u32);

impl TransactionSendAttempt {
    /// Returns the first execution generation for one accepted send.
    pub const fn initial() -> Self {
        Self(0)
    }

    pub(super) const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the core-owned execution generation.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// One transaction-owned partition-local sequence range assigned before encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionSequenceLease {
    base_sequence: i32,
    record_count: u32,
}

impl TransactionSequenceLease {
    /// Validates one nonempty transactional Kafka sequence range.
    pub const fn try_new(base_sequence: i32, record_count: u32) -> Option<Self> {
        if base_sequence < 0 || record_count == 0 {
            None
        } else {
            Some(Self {
                base_sequence,
                record_count,
            })
        }
    }

    /// Returns the first Kafka sequence encoded into the transactional batch.
    pub const fn base_sequence(self) -> i32 {
        self.base_sequence
    }

    /// Returns the exact number of records fenced by this transaction-owned lease.
    pub const fn record_count(self) -> u32 {
        self.record_count
    }
}

/// Immutable idempotent identity retained across one transactional send replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionSendIdentity {
    producer: TransactionalProducerIdentity,
    partition: TransactionPartition,
    sequence: TransactionSequenceLease,
    deadline: Deadline,
}

impl TransactionSendIdentity {
    /// Joins the exact producer, partition, sequence, and original operation deadline.
    pub const fn new(
        producer: TransactionalProducerIdentity,
        partition: TransactionPartition,
        sequence: TransactionSequenceLease,
        deadline: Deadline,
    ) -> Self {
        Self {
            producer,
            partition,
            sequence,
            deadline,
        }
    }

    /// Returns the broker-issued transactional producer identity.
    pub const fn producer(self) -> TransactionalProducerIdentity {
        self.producer
    }

    /// Returns the exact partition that owns the sequence lease.
    pub const fn partition(self) -> TransactionPartition {
        self.partition
    }

    /// Returns the unchanged partition-local sequence range.
    pub const fn sequence(self) -> TransactionSequenceLease {
        self.sequence
    }

    /// Returns the original absolute public-operation deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }
}

/// Normalized failure shape considered by transactional send replacement policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionSendAttemptFailure {
    /// A correlated Kafka broker rejection with exact semantic classification.
    Broker(ProducerBrokerFailure),
    /// No correlated broker response proves that retrying the exact shape is safe.
    Uncertain,
}

/// Transaction consequence of one accepted send terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionSendOutcome {
    /// The send completed without changing transaction health.
    Succeeded,
    /// The accepted operation failed before Kafka could append its batch.
    ///
    /// The public send reports failure, while the transaction remains healthy
    /// and may commit after every other accepted send settles.
    FailedHealthy,
    /// The send failed and the transaction may now only be aborted.
    AbortRequired,
    /// The send irrecoverably fenced the transactional owner.
    Fatal,
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
