//! Narrow fakeable seam around one tracked `AddPartitionsToTxn` call.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{DeliveryStatus, TransactionEpoch};

use super::model::TransactionPartitionEnrollmentFailureKind;

const CONCURRENT_TRANSACTIONS: i16 = 51;

/// Immutable owner and target facts for one exact submission.
pub(super) struct TransactionPartitionEnrollmentRequest<'a> {
    pub(super) epoch: TransactionEpoch,
    pub(super) transactional_id: &'a str,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) topic: &'a Arc<str>,
    pub(super) partition: i32,
    pub(super) deadline: Instant,
}

/// Normalized terminal before deterministic owner settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionPartitionEnrollmentPortFact {
    Enrolled,
    RetryableConcurrentTransactions {
        kind: TransactionPartitionEnrollmentFailureKind,
        delivery: DeliveryStatus,
    },
    RetryableCoordinatorLoss {
        kind: TransactionPartitionEnrollmentFailureKind,
        delivery: DeliveryStatus,
    },
    Failed {
        kind: TransactionPartitionEnrollmentFailureKind,
        delivery: DeliveryStatus,
    },
}

impl TransactionPartitionEnrollmentPortFact {
    pub(super) const fn broker_rejection(
        kind: TransactionPartitionEnrollmentFailureKind,
        retry_safe_after_refresh: bool,
    ) -> Self {
        match kind {
            TransactionPartitionEnrollmentFailureKind::Broker {
                code: CONCURRENT_TRANSACTIONS,
                ..
            } => Self::RetryableConcurrentTransactions {
                kind,
                delivery: DeliveryStatus::PossiblySent,
            },
            TransactionPartitionEnrollmentFailureKind::Broker { code: 14..=16, .. }
                if retry_safe_after_refresh =>
            {
                Self::RetryableCoordinatorLoss {
                    kind,
                    delivery: DeliveryStatus::PossiblySent,
                }
            }
            _ => Self::Failed {
                kind,
                delivery: DeliveryStatus::PossiblySent,
            },
        }
    }

    pub(super) const fn with_delivery_floor(self, floor: DeliveryStatus) -> Self {
        match self {
            Self::Enrolled => Self::Enrolled,
            Self::RetryableConcurrentTransactions { kind, delivery } => {
                Self::RetryableConcurrentTransactions {
                    kind,
                    delivery: weaken_delivery(floor, delivery),
                }
            }
            Self::RetryableCoordinatorLoss { kind, delivery } => Self::RetryableCoordinatorLoss {
                kind,
                delivery: weaken_delivery(floor, delivery),
            },
            Self::Failed { kind, delivery } => Self::Failed {
                kind,
                delivery: weaken_delivery(floor, delivery),
            },
        }
    }
}

pub(super) const fn weaken_delivery(left: DeliveryStatus, right: DeliveryStatus) -> DeliveryStatus {
    if matches!(left, DeliveryStatus::PossiblySent) || matches!(right, DeliveryStatus::PossiblySent)
    {
        DeliveryStatus::PossiblySent
    } else {
        DeliveryStatus::NotSent
    }
}

/// Linear route evidence retained until owner settlement accepts the fact.
pub(super) trait TransactionPartitionEnrollmentPortEvidence {
    fn epoch(&self) -> TransactionEpoch;

    fn fact(&self) -> TransactionPartitionEnrollmentPortFact;

    fn discard(self: Box<Self>);
}

pub(super) fn evidence_fact(
    epoch: TransactionEpoch,
    evidence: &dyn TransactionPartitionEnrollmentPortEvidence,
    deadline_elapsed: bool,
) -> TransactionPartitionEnrollmentPortFact {
    if evidence.epoch() != epoch {
        return TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        };
    }
    match (deadline_elapsed, evidence.fact()) {
        (false, fact) => fact,
        (
            true,
            TransactionPartitionEnrollmentPortFact::RetryableConcurrentTransactions {
                delivery,
                ..
            }
            | TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss { delivery, .. }
            | TransactionPartitionEnrollmentPortFact::Failed { delivery, .. },
        ) => TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::DeadlineElapsed,
            delivery,
        },
        (true, TransactionPartitionEnrollmentPortFact::Enrolled) => {
            TransactionPartitionEnrollmentPortFact::Failed {
                kind: TransactionPartitionEnrollmentFailureKind::InvalidResponse,
                delivery: DeliveryStatus::PossiblySent,
            }
        }
    }
}

/// One bounded nonblocking accepted-call observation.
pub(super) enum TransactionPartitionEnrollmentPortCallPoll {
    Pending,
    Progress,
    DeadlineElapsed(Box<dyn TransactionPartitionEnrollmentPortEvidence>),
    Terminal(Box<dyn TransactionPartitionEnrollmentPortEvidence>),
}

/// One accepted tracked call retained until exactly one terminal.
pub(super) trait TransactionPartitionEnrollmentPortCall: Send {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionPartitionEnrollmentPortCallPoll;

    fn discard_after_driver_shutdown(self: Box<Self>);
}

/// Concrete or fake submission boundary.
pub(super) trait TransactionPartitionEnrollmentPort {
    fn submit(
        &mut self,
        request: TransactionPartitionEnrollmentRequest<'_>,
    ) -> Result<Box<dyn TransactionPartitionEnrollmentPortCall>, ()>;
}
