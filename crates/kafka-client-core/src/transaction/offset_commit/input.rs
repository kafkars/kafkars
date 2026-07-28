//! Normalized correlated facts accepted by transactional offset-transfer policy.

use crate::TransactionEpoch;

use super::{
    TransactionOffsetCommitConsequence, TransactionOffsetCommitId, TransactionOffsetCommitStage,
};

/// One external fact applied to the exact retained offset-transfer attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOffsetCommitInput {
    /// The driver accepted ownership of the named request.
    DriverAccepted {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Exact request stage.
        stage: TransactionOffsetCommitStage,
    },
    /// The driver definitely rejected the named request before ownership.
    DriverRejected {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Exact request stage.
        stage: TransactionOffsetCommitStage,
    },
    /// Kafka accepted the named driver-owned request.
    Succeeded {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Exact request stage.
        stage: TransactionOffsetCommitStage,
    },
    /// A driver-owned request was definitely rejected after its causal route refresh.
    RetryableFailed {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Exact request stage authorized for replacement.
        stage: TransactionOffsetCommitStage,
    },
    /// The named driver-owned request failed.
    AcceptedFailed {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Nonreused offset-transfer identity.
        operation_id: TransactionOffsetCommitId,
        /// Exact request stage.
        stage: TransactionOffsetCommitStage,
        /// Required transaction-lifecycle consequence.
        consequence: TransactionOffsetCommitConsequence,
    },
}

impl TransactionOffsetCommitInput {
    /// Returns the exact transaction, operation, and request-stage correlation.
    pub const fn correlation(
        self,
    ) -> (
        TransactionEpoch,
        TransactionOffsetCommitId,
        TransactionOffsetCommitStage,
    ) {
        match self {
            Self::DriverAccepted {
                epoch,
                operation_id,
                stage,
            }
            | Self::DriverRejected {
                epoch,
                operation_id,
                stage,
            }
            | Self::Succeeded {
                epoch,
                operation_id,
                stage,
            }
            | Self::RetryableFailed {
                epoch,
                operation_id,
                stage,
            }
            | Self::AcceptedFailed {
                epoch,
                operation_id,
                stage,
                ..
            } => (epoch, operation_id, stage),
        }
    }
}
