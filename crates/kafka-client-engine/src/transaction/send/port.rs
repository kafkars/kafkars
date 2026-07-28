//! Fakeable tracked transactional Produce submission and evidence boundary.

use kafka_client_core::{
    DeliveryStatus, Moment, ProducerAttemptFailureKind, TransactionEpoch, TransactionSendAttempt,
    TransactionSendId,
};

use crate::{
    clock::OperationDeadline,
    driver::{
        DriverOwner,
        transaction_produce::{
            TransactionProduceCall, TransactionProduceRouteRefreshPoll, TransactionProduceTerminal,
            TransactionProduceTerminalFact,
        },
    },
    protocol::produce::MaterializedProduce,
};

pub(in crate::transaction) struct TransactionSendProduceRequest<'a> {
    pub(in crate::transaction) epoch: TransactionEpoch,
    pub(in crate::transaction) send_id: TransactionSendId,
    pub(in crate::transaction) attempt: TransactionSendAttempt,
    pub(in crate::transaction) transactional_id: &'a str,
    pub(in crate::transaction) materialized: &'a MaterializedProduce,
    pub(in crate::transaction) now: Moment,
    pub(in crate::transaction) deadline: OperationDeadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::transaction) struct TransactionSendProduceSubmissionFailure {
    pub(in crate::transaction) kind: ProducerAttemptFailureKind,
    pub(in crate::transaction) delivery: DeliveryStatus,
}

pub(in crate::transaction) trait TransactionSendProduceEvidence:
    Send
{
    fn attempt(&self) -> TransactionSendAttempt;

    fn fact(&self) -> TransactionProduceTerminalFact;

    fn poll_route_refresh(&mut self, driver: &DriverOwner) -> TransactionProduceRouteRefreshPoll;

    fn discard(self: Box<Self>);
}

pub(in crate::transaction) trait TransactionSendProduceCall: Send {
    fn try_terminal(&mut self) -> Option<Box<dyn TransactionSendProduceEvidence>>;

    fn recover_after_driver_shutdown(self: Box<Self>) -> Box<dyn TransactionSendProduceEvidence>;
}

pub(in crate::transaction) trait TransactionSendProducePort {
    fn submit(
        &mut self,
        request: TransactionSendProduceRequest<'_>,
    ) -> Result<Box<dyn TransactionSendProduceCall>, TransactionSendProduceSubmissionFailure>;
}

pub(super) struct DriverTransactionSendProducePort<'a> {
    driver: &'a DriverOwner,
}

impl<'a> DriverTransactionSendProducePort<'a> {
    pub(super) const fn new(driver: &'a DriverOwner) -> Self {
        Self { driver }
    }
}

impl TransactionSendProducePort for DriverTransactionSendProducePort<'_> {
    fn submit(
        &mut self,
        request: TransactionSendProduceRequest<'_>,
    ) -> Result<Box<dyn TransactionSendProduceCall>, TransactionSendProduceSubmissionFailure> {
        TransactionProduceCall::submit(
            self.driver,
            request.epoch,
            request.send_id,
            request.attempt,
            request.transactional_id,
            request.materialized,
            request.now,
            request.deadline,
        )
        .map(|call| Box::new(DriverTransactionSendProduceCall(call)) as Box<_>)
        .map_err(|failure| TransactionSendProduceSubmissionFailure {
            kind: failure.failure_kind(),
            delivery: failure.delivery(),
        })
    }
}

struct DriverTransactionSendProduceCall(TransactionProduceCall);

impl TransactionSendProduceCall for DriverTransactionSendProduceCall {
    fn try_terminal(&mut self) -> Option<Box<dyn TransactionSendProduceEvidence>> {
        self.0
            .try_terminal()
            .map(|terminal| Box::new(DriverTransactionSendProduceEvidence(terminal)) as Box<_>)
    }

    fn recover_after_driver_shutdown(self: Box<Self>) -> Box<dyn TransactionSendProduceEvidence> {
        Box::new(DriverTransactionSendProduceEvidence(
            self.0.recover_after_driver_shutdown(),
        ))
    }
}

struct DriverTransactionSendProduceEvidence(TransactionProduceTerminal);

impl TransactionSendProduceEvidence for DriverTransactionSendProduceEvidence {
    fn attempt(&self) -> TransactionSendAttempt {
        self.0.attempt()
    }

    fn fact(&self) -> TransactionProduceTerminalFact {
        self.0.fact()
    }

    fn poll_route_refresh(&mut self, driver: &DriverOwner) -> TransactionProduceRouteRefreshPoll {
        self.0.poll_route_refresh(driver)
    }

    fn discard(self: Box<Self>) {
        self.0.discard();
    }
}
