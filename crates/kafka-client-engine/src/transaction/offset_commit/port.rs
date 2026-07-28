//! Narrow mechanism port for two generated transactional offset requests.

use kafka_client_core::{
    DeliveryStatus, TransactionEpoch, TransactionOffsetCommitConsequence,
    TransactionOffsetCommitId, TransactionOffsetCommitStage, TransactionalProducerIdentity,
};

use super::{
    input::{TransactionOffsetCommitGroup, TransactionOffsetCommitOffset},
    model::TransactionOffsetCommitFailureKind,
};

pub(super) struct TransactionOffsetCommitPortRequest<'a> {
    pub(super) epoch: TransactionEpoch,
    pub(super) operation_id: TransactionOffsetCommitId,
    pub(super) stage: TransactionOffsetCommitStage,
    pub(super) transactional_id: &'a str,
    pub(super) producer: TransactionalProducerIdentity,
    pub(super) group: &'a TransactionOffsetCommitGroup,
    pub(super) offsets: &'a [TransactionOffsetCommitOffset],
    pub(super) deadline: std::time::Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionOffsetCommitPortFact {
    Succeeded,
    RetryableCoordinatorLoss {
        kind: TransactionOffsetCommitFailureKind,
        delivery: DeliveryStatus,
    },
    Failed {
        consequence: TransactionOffsetCommitConsequence,
        kind: TransactionOffsetCommitFailureKind,
        delivery: DeliveryStatus,
    },
}

pub(super) trait TransactionOffsetCommitPortEvidence: Send {
    fn correlation(
        &self,
    ) -> (
        TransactionEpoch,
        TransactionOffsetCommitId,
        TransactionOffsetCommitStage,
    );
    fn fact(&self) -> TransactionOffsetCommitPortFact;
    fn discard(self: Box<Self>);
}

pub(super) enum TransactionOffsetCommitPortCallPoll {
    Pending,
    Progress,
    DeadlineElapsed(Box<dyn TransactionOffsetCommitPortEvidence>),
    Terminal(Box<dyn TransactionOffsetCommitPortEvidence>),
}

pub(super) trait TransactionOffsetCommitPortCall: Send {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionOffsetCommitPortCallPoll;
    fn recover_after_driver_shutdown(
        self: Box<Self>,
    ) -> Box<dyn TransactionOffsetCommitPortEvidence>;
}

pub(super) trait TransactionOffsetCommitPort {
    fn submit(
        &mut self,
        request: TransactionOffsetCommitPortRequest<'_>,
    ) -> Result<
        Box<dyn TransactionOffsetCommitPortCall>,
        (TransactionOffsetCommitFailureKind, DeliveryStatus),
    >;
}
