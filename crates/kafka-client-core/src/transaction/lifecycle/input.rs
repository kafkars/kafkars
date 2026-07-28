//! Normalized facts accepted by deterministic transaction lifecycle policy.

use crate::OperationId;

use super::{TransactionEndOutcome, TransactionEpoch};

/// One external fact applied to a fenced transactional owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleInput {
    /// Begins one local transaction without a broker request.
    Begin,
    /// Requests commit through one already-reserved public completion.
    Commit {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Stable commit operation identity.
        operation_id: OperationId,
    },
    /// Requests abort through one already-reserved public completion.
    Abort {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Stable abort operation identity.
        operation_id: OperationId,
    },
    /// Authorizes replacement of an exactly rejected `EndTxn` after route refresh.
    EndRetryableBrokerRejected {
        /// Ending transaction fence retained by the original public operation.
        epoch: TransactionEpoch,
    },
    /// Applies the terminal consequence of the sole requested `EndTxn`.
    EndSettled {
        /// Ending transaction fence.
        epoch: TransactionEpoch,
        /// Normalized settlement consequence.
        outcome: TransactionEndOutcome,
    },
    /// Reports loss of the unique public producer or transaction owner.
    OwnerLost,
}
