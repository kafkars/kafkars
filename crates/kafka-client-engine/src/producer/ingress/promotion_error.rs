//! Typed dormant-promotion outcomes and exact recovery owners.

use kafka_client_core::OperationId;

use crate::{
    ProducerDeliveryObserver, ProducerSendStartFailure,
    producer::{
        ProducerHostInvariantError, ProducerRejectionReason,
        pending::{
            PendingAdmission, PendingAttemptAcceptFailure, PendingAttemptRestoreFailure,
            PendingAttemptSettleFailure, PendingLocalFailure, PendingNotificationJob,
            PendingPromotionAttempt, PendingRecordRestoreFailure, PendingStartFailure,
            ProducerSendFailure, turn_error::PendingTurnFailure,
        },
    },
};

/// Diagnostic that must accompany a dormant promotion into future live recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingPromotionInvariant {
    Host(ProducerHostInvariantError),
    UnexpectedRejection(ProducerRejectionReason),
}

/// One resolved FIFO attempt; notification owners remain with the caller.
#[must_use = "resolved pending promotion may retain bytes or an exact notification job"]
#[allow(dead_code, reason = "live host turn will consume all outcome owners")]
pub(crate) enum PendingPromotionResolution {
    Accepted(PendingAcceptedResolution),
    Restored,
    Abandoned(PendingAdmission),
    Local(PendingLocalFailure),
    Start(PendingStartResolution),
}

/// Accepted operation facts awaiting notification submission.
#[must_use = "accepted promotion retains the exact pending notification job"]
pub(crate) struct PendingAcceptedResolution {
    operation_id: Option<OperationId>,
    notification: PendingNotificationJob,
    invariant: Option<PendingPromotionInvariant>,
}

impl PendingAcceptedResolution {
    pub(super) const fn new(
        operation_id: Option<OperationId>,
        notification: PendingNotificationJob,
        invariant: Option<PendingPromotionInvariant>,
    ) -> Self {
        Self {
            operation_id,
            notification,
            invariant,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Option<OperationId>,
        PendingNotificationJob,
        Option<PendingPromotionInvariant>,
    ) {
        (self.operation_id, self.notification, self.invariant)
    }
}

/// Start failure plus any host fault that requires later live recovery.
#[must_use = "start resolution retains the exact admission and notification job"]
#[allow(dead_code, reason = "live notifier will consume the retained fields")]
pub(crate) struct PendingStartResolution {
    failure: PendingStartFailure,
    invariant: Option<PendingPromotionInvariant>,
}

#[allow(dead_code, reason = "live notifier will consume the retained fields")]
impl PendingStartResolution {
    pub(super) const fn new(
        failure: PendingStartFailure,
        invariant: Option<PendingPromotionInvariant>,
    ) -> Self {
        Self { failure, invariant }
    }

    pub(crate) fn into_parts(self) -> (PendingStartFailure, Option<PendingPromotionInvariant>) {
        (self.failure, self.invariant)
    }
}

/// Bounded scan result containing zero or one resolved FIFO attempt.
#[must_use = "promotion progress may retain one resolved linear owner"]
pub(crate) struct PendingPromotionProgress {
    inspected: usize,
    remaining: bool,
    resolution: Option<PendingPromotionResolution>,
}

impl PendingPromotionProgress {
    pub(super) const fn new(
        inspected: usize,
        remaining: bool,
        resolution: Option<PendingPromotionResolution>,
    ) -> Self {
        Self {
            inspected,
            remaining,
            resolution,
        }
    }

    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    pub(crate) const fn remaining(&self) -> bool {
        self.remaining
    }

    pub(crate) fn into_resolution(self) -> Option<PendingPromotionResolution> {
        self.resolution
    }
}

/// Accepted ownership retained when attempt bookkeeping cannot commit.
#[must_use = "accepted observer and promotion attempt require recovery"]
#[allow(dead_code, reason = "future shard fatal slot consumes this owner")]
pub(crate) struct PendingAcceptedCommitFailure {
    pub(crate) error: super::super::pending::PendingAttemptStateError,
    pub(crate) attempt: Box<PendingPromotionAttempt>,
    pub(crate) observer: ProducerDeliveryObserver,
    pub(crate) operation_id: Option<OperationId>,
    pub(crate) invariant: Option<PendingPromotionInvariant>,
}

/// Dormant failure whose exact owner must enter the future shard fatal slot.
#[must_use = "promotion failure retains linear ownership for live recovery"]
#[allow(dead_code, reason = "future shard fatal slot consumes these owners")]
pub(crate) enum PendingPromotionFailure {
    Closed,
    Take(Box<PendingTurnFailure>),
    Detach {
        error: super::super::pending::PendingAttemptStateError,
        attempt: Box<PendingPromotionAttempt>,
    },
    RecordRestore {
        attempt: Box<PendingPromotionAttempt>,
        failure: Box<PendingRecordRestoreFailure>,
    },
    Restore(Box<PendingAttemptRestoreFailure>),
    AcceptedCommit(Box<PendingAcceptedCommitFailure>),
    Accept {
        failure: Box<PendingAttemptAcceptFailure>,
        operation_id: Option<OperationId>,
        invariant: Option<PendingPromotionInvariant>,
    },
    Local(Box<PendingAttemptSettleFailure<ProducerSendFailure>>),
    Start(Box<PendingAttemptSettleFailure<ProducerSendStartFailure>>),
    Fatal {
        invariant: PendingPromotionInvariant,
        attempt: Box<PendingPromotionAttempt>,
    },
}
