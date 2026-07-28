//! Narrow fakeable seam around one tracked `AddPartitionsToTxn` call.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{DeliveryStatus, TransactionEpoch};

use super::model::TransactionPartitionEnrollmentFailureKind;

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
    RetryableCoordinatorLoss {
        kind: TransactionPartitionEnrollmentFailureKind,
        delivery: DeliveryStatus,
    },
    Failed {
        kind: TransactionPartitionEnrollmentFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Linear route evidence retained until owner settlement accepts the fact.
pub(super) trait TransactionPartitionEnrollmentPortEvidence {
    fn epoch(&self) -> TransactionEpoch;

    fn fact(&self) -> TransactionPartitionEnrollmentPortFact;

    fn discard(self: Box<Self>);
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
