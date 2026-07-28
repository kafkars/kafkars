//! Stable stage, delivery, consequence, and failure facts for transactional offsets.

use kafka_client_core::{DeliveryStatus, TransactionOffsetCommitStage};

use crate::transaction::offset_commit::{
    TransactionOffsetCommitFailure as InternalFailure,
    TransactionOffsetCommitFailureKind as InternalKind,
    TransactionOffsetCommitOutcome as InternalOutcome,
};

/// Kafka request stage that terminally settled the offset transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsStage {
    /// Adding the consumer group to the active transaction.
    AddOffsets,
    /// Writing the assignment-fenced offsets.
    CommitOffsets,
}

/// Authoritative transport certainty for a failed stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsDeliveryStatus {
    /// The stage definitely did not enter transport ownership.
    NotSent,
    /// Kafka may have observed the stage.
    PossiblySent,
}

/// Effect of the failure on the active transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsConsequence {
    /// The transaction remains eligible to continue or commit.
    FailedHealthy,
    /// The transaction may now only be aborted.
    AbortRequired,
    /// The initialized producer owner is permanently unusable.
    Fatal,
}

/// Stable reason one accepted transactional offset transfer failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected before transport ownership.
    DriverRejected,
    /// Fixed local execution or terminal capacity was unavailable.
    Backpressure,
    /// The broker cannot execute the required protocol version.
    Compatibility,
    /// A response was malformed or did not match the exact request.
    InvalidResponse,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka returned an exact signed broker rejection.
    Broker,
    /// Accepted operation identity or stage correlation failed.
    Correlation,
    /// The driver closed before stable settlement.
    DriverClosed,
}

/// Exact failed stage, certainty, broker code, and transaction consequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionOffsetsFailure {
    stage: TransactionOffsetsStage,
    kind: TransactionOffsetsFailureKind,
    delivery: TransactionOffsetsDeliveryStatus,
    broker_code: Option<i16>,
    consequence: TransactionOffsetsConsequence,
}

impl TransactionOffsetsFailure {
    /// Returns the exact request stage that failed.
    pub const fn stage(self) -> TransactionOffsetsStage {
        self.stage
    }
    /// Returns the stable failure category.
    pub const fn kind(self) -> TransactionOffsetsFailureKind {
        self.kind
    }
    /// Returns authoritative transport certainty.
    pub const fn delivery(self) -> TransactionOffsetsDeliveryStatus {
        self.delivery
    }
    /// Returns Kafka's exact signed broker code when supplied.
    pub const fn broker_code(self) -> Option<i16> {
        self.broker_code
    }
    /// Returns the deterministic transaction-health consequence.
    pub const fn consequence(self) -> TransactionOffsetsConsequence {
        self.consequence
    }
}

/// Exactly one public terminal for an accepted offset transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetsOutcome {
    /// Both Kafka request stages succeeded.
    Succeeded,
    /// One exact stage failed with explicit certainty and consequence.
    Failed(TransactionOffsetsFailure),
}

pub(super) fn translate_offset_outcome(outcome: InternalOutcome) -> TransactionOffsetsOutcome {
    match outcome {
        InternalOutcome::Succeeded => TransactionOffsetsOutcome::Succeeded,
        InternalOutcome::RejectedNotSent { stage, failure } => {
            failed(stage, failure, TransactionOffsetsConsequence::FailedHealthy)
        }
        InternalOutcome::AbortRequired { stage, failure } => {
            failed(stage, failure, TransactionOffsetsConsequence::AbortRequired)
        }
        InternalOutcome::Fatal { stage, failure } => {
            failed(stage, failure, TransactionOffsetsConsequence::Fatal)
        }
    }
}

fn failed(
    stage: TransactionOffsetCommitStage,
    failure: InternalFailure,
    consequence: TransactionOffsetsConsequence,
) -> TransactionOffsetsOutcome {
    let (kind, broker_code) = match failure.kind() {
        InternalKind::DeadlineElapsed => (TransactionOffsetsFailureKind::DeadlineElapsed, None),
        InternalKind::DriverRejected => (TransactionOffsetsFailureKind::DriverRejected, None),
        InternalKind::Allocation => (TransactionOffsetsFailureKind::Backpressure, None),
        InternalKind::Compatibility => (TransactionOffsetsFailureKind::Compatibility, None),
        InternalKind::InvalidResponse => (TransactionOffsetsFailureKind::InvalidResponse, None),
        InternalKind::Transport => (TransactionOffsetsFailureKind::Transport, None),
        InternalKind::Broker { code, .. } => (TransactionOffsetsFailureKind::Broker, Some(code)),
        InternalKind::Correlation => (TransactionOffsetsFailureKind::Correlation, None),
        InternalKind::DriverShutdown => (TransactionOffsetsFailureKind::DriverClosed, None),
    };
    TransactionOffsetsOutcome::Failed(TransactionOffsetsFailure {
        stage: match stage {
            TransactionOffsetCommitStage::AddOffsets => TransactionOffsetsStage::AddOffsets,
            TransactionOffsetCommitStage::TxnOffsetCommit => TransactionOffsetsStage::CommitOffsets,
        },
        kind,
        delivery: match failure.delivery() {
            DeliveryStatus::NotSent => TransactionOffsetsDeliveryStatus::NotSent,
            DeliveryStatus::PossiblySent => TransactionOffsetsDeliveryStatus::PossiblySent,
        },
        broker_code,
        consequence,
    })
}
