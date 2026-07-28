//! Stable ownership and terminal facts for partition enrollment.

use std::sync::Arc;

use kafka_client_core::{DeliveryStatus, TransactionEpoch};

use crate::producer::materialization::TransactionalMaterializationBatch;

/// Fixed capacity and retained-topic-byte bounds for one transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionPartitionEnrollmentLimits {
    max_partitions: usize,
    max_topic_bytes: usize,
}

impl TransactionPartitionEnrollmentLimits {
    /// Creates nonzero count and retained-name-byte bounds.
    pub(crate) const fn try_new(max_partitions: usize, max_topic_bytes: usize) -> Option<Self> {
        if max_partitions == 0 || max_topic_bytes == 0 {
            None
        } else {
            Some(Self {
                max_partitions,
                max_topic_bytes,
            })
        }
    }

    pub(super) const fn max_partitions(self) -> usize {
        self.max_partitions
    }

    pub(super) const fn max_topic_bytes(self) -> usize {
        self.max_topic_bytes
    }
}

/// Failure to reserve one bounded enrollment owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionPartitionEnrollmentStartError {
    /// Initialized owner identity unexpectedly lacks a transactional ID.
    EmptyTransactionalId,
    /// The fixed enrolled-set allocation could not be reserved.
    RetainedBytes,
}

/// Exact-epoch lifecycle violation at enrollment-set activation or release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionPartitionEnrollmentEpochError {
    /// A second transaction was activated before the first ended.
    AlreadyActive,
    /// No active transaction can match the supplied epoch.
    NotActive,
    /// The supplied end epoch does not match the enrolled set.
    EpochMismatch,
    /// Pending call, terminal, or enrollment ownership prevents the transition.
    Unsettled,
}

/// One exact topic-partition retained in the enrollment set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TransactionPartitionEnrollmentTarget {
    topic: Arc<str>,
    partition: i32,
}

impl TransactionPartitionEnrollmentTarget {
    pub(super) const fn new(topic: Arc<str>, partition: i32) -> Self {
        Self { topic, partition }
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    pub(super) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(super) fn retained_topic_bytes(&self) -> usize {
        self.topic.len()
    }
}

/// Local rejection retaining the exact transactional batch.
#[must_use = "local rejection retains the exact transactional batch"]
pub(crate) struct TransactionPartitionEnrollmentAdmissionFailure {
    kind: TransactionPartitionEnrollmentFailureKind,
    batch: TransactionalMaterializationBatch,
}

impl TransactionPartitionEnrollmentAdmissionFailure {
    pub(in crate::transaction) const fn new(
        kind: TransactionPartitionEnrollmentFailureKind,
        batch: TransactionalMaterializationBatch,
    ) -> Self {
        Self { kind, batch }
    }

    /// Returns the exact local rejection fact.
    pub(crate) const fn kind(&self) -> TransactionPartitionEnrollmentFailureKind {
        self.kind
    }

    /// Restores the unchanged batch rejected before uncertain transport ownership.
    pub(crate) fn into_batch(self) -> TransactionalMaterializationBatch {
        self.batch
    }
}

/// Stable reason one enrollment did not authorize transactional Produce.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionPartitionEnrollmentFailureKind {
    /// Another enrollment or its terminal occupies the sole slot.
    Busy,
    /// The supplied transaction epoch is not active.
    EpochMismatch,
    /// The batch carries a different broker-issued producer identity.
    OwnerMismatch,
    /// Topic or partition shape is invalid.
    InvalidTarget,
    /// The exact enrolled-partition count bound is full.
    Capacity,
    /// Retaining the target would exceed its topic-name byte envelope.
    RetainedBytes,
    /// The original send deadline elapsed before or during enrollment.
    DeadlineElapsed,
    /// The tracked request was rejected before driver ownership.
    DriverRejected,
    /// An accepted tracked request ended in transport failure.
    Transport,
    /// Exact v3 protocol compatibility was unavailable.
    Compatibility,
    /// Selected version, correlation, or response shape was invalid.
    InvalidResponse,
    /// The accepted call lost its driver completion owner.
    DriverClosed,
    /// Kafka returned one exact signed partition rejection.
    Broker {
        /// Exact signed Kafka error code.
        code: i16,
        /// Whether Kafka fenced the initialized producer identity.
        fenced: bool,
    },
}

impl TransactionPartitionEnrollmentFailureKind {
    pub(super) const fn is_fatal(self) -> bool {
        matches!(self, Self::Broker { fenced: true, .. })
    }
}

/// Explicit proof that the exact batch target is enrolled for this owner.
#[must_use = "an enrollment fence must be handed to transactional Produce or reclaimed"]
pub(crate) struct TransactionPartitionEnrollmentFence {
    epoch: TransactionEpoch,
    batch: TransactionalMaterializationBatch,
}

impl TransactionPartitionEnrollmentFence {
    pub(in crate::transaction) const fn new(
        epoch: TransactionEpoch,
        batch: TransactionalMaterializationBatch,
    ) -> Self {
        Self { epoch, batch }
    }

    /// Returns the nonreused transaction epoch that owns this enrollment.
    pub(crate) const fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    /// Transfers the exact enrolled batch into transactional Produce handoff.
    pub(crate) fn into_batch(self) -> TransactionalMaterializationBatch {
        self.batch
    }
}

/// Result of bounded enrollment admission.
#[must_use = "pending enrollment must be driven to one terminal"]
pub(crate) enum TransactionPartitionEnrollmentAdmission {
    /// This exact target was already enrolled in the active epoch.
    Enrolled(TransactionPartitionEnrollmentFence),
    /// The sole terminal slot and pending call owner now retain the batch.
    Pending,
}

/// Exactly one deterministic enrollment consequence.
#[must_use = "an enrollment terminal must settle the transactional send"]
pub(crate) enum TransactionPartitionEnrollmentTerminal {
    /// Kafka enrolled the target and produced an exact epoch fence.
    Enrolled(TransactionPartitionEnrollmentFence),
    /// Definitely-unsent local failure restored the unchanged batch.
    Rejected(TransactionPartitionEnrollmentAdmissionFailure),
    /// Enrollment uncertainty requires the active transaction to abort.
    AbortRequired {
        /// Stable failure category.
        kind: TransactionPartitionEnrollmentFailureKind,
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
        /// Exact batch that did not reach Produce.
        batch: TransactionalMaterializationBatch,
    },
    /// Exact broker fencing made the initialized owner unusable.
    Fatal {
        /// Exact fenced broker category and signed code.
        kind: TransactionPartitionEnrollmentFailureKind,
        /// Exact batch that did not reach Produce.
        batch: TransactionalMaterializationBatch,
    },
}

/// At-most-one action performed by one owner turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionPartitionEnrollmentTurn {
    /// No local submission or terminal action was ready.
    Idle,
    /// Exactly one submission, rejection, or settlement action ran.
    Progress,
}
