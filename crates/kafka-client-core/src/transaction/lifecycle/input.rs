//! Normalized facts accepted by deterministic transaction lifecycle policy.

use crate::{Moment, OperationId};

use super::{
    TransactionEndOutcome, TransactionEpoch, TransactionSendAttempt, TransactionSendAttemptFailure,
    TransactionSendId, TransactionSendIdentity, TransactionSendOutcome,
};

/// One external fact applied to a fenced transactional owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleInput {
    /// Begins one local transaction without a broker request.
    Begin,
    /// Records a send only after its terminal capacity has been reserved.
    SendAccepted {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Unique accepted-send fence.
        send_id: TransactionSendId,
    },
    /// Retains the immutable idempotent shape before its first broker execution.
    SendPrepared {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Unique accepted-send fence.
        send_id: TransactionSendId,
        /// Exact producer, partition, sequence, and original deadline.
        identity: TransactionSendIdentity,
    },
    /// Applies one correlated failed execution to bounded replacement policy.
    SendAttemptFailed {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Unique accepted-send fence.
        send_id: TransactionSendId,
        /// Exact execution generation that produced the failure.
        attempt: TransactionSendAttempt,
        /// Fresh monotonic observation captured by the effect interpreter.
        now: Moment,
        /// Closed failure shape considered for safe replacement.
        failure: TransactionSendAttemptFailure,
    },
    /// Applies the terminal consequence of one accepted send.
    SendSettled {
        /// Active transaction fence.
        epoch: TransactionEpoch,
        /// Unique accepted-send fence.
        send_id: TransactionSendId,
        /// Effect on transaction health.
        outcome: TransactionSendOutcome,
    },
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
