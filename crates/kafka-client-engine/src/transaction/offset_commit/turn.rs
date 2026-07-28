//! Completion-first two-stage submission and terminal polling.

use std::sync::Arc;

use kafka_client_core::{
    Moment, TransactionOffsetCommitId, TransactionOffsetCommitInput, TransactionOffsetCommitStage,
    TransactionalProducerIdentity,
};

use crate::{
    completion::CompletionId,
    driver::DriverOwner,
    transaction::{
        TransactionLifecycleHost, offset_commit::driver_port::DriverTransactionOffsetCommitPort,
    },
};

use super::{
    input::TransactionOffsetCommitRequest,
    model::{TransactionOffsetCommitHostError, TransactionOffsetCommitResult},
    owner::TransactionOffsetCommitOwner,
    port::{
        TransactionOffsetCommitPort, TransactionOffsetCommitPortCall,
        TransactionOffsetCommitPortCallPoll, TransactionOffsetCommitPortEvidence,
        TransactionOffsetCommitPortRequest,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitTurn {
    Idle,
    Progress,
}

pub(super) struct PendingTransactionOffsetCommit {
    pub(super) completion_id: CompletionId,
    pub(super) operation_id: TransactionOffsetCommitId,
    pub(super) transactional_id: Arc<str>,
    pub(super) producer: TransactionalProducerIdentity,
    pub(super) request: TransactionOffsetCommitRequest,
    pub(super) retry_not_before: Option<kafka_client_core::Deadline>,
    pub(super) retries_started: u32,
}

pub(super) enum TransactionOffsetCommitSlot {
    Vacant,
    Ready(PendingTransactionOffsetCommit, TransactionOffsetCommitStage),
    Calling(
        PendingTransactionOffsetCommit,
        TransactionOffsetCommitStage,
        Box<dyn TransactionOffsetCommitPortCall>,
    ),
    Settling(
        PendingTransactionOffsetCommit,
        TransactionOffsetCommitStage,
        Box<dyn TransactionOffsetCommitPortEvidence>,
    ),
    Terminal(CompletionId, TransactionOffsetCommitResult),
    Published,
}

impl TransactionOffsetCommitOwner {
    pub(crate) fn turn(
        &mut self,
        lifecycle: &mut TransactionLifecycleHost,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionOffsetCommitTurn, TransactionOffsetCommitHostError> {
        self.turn_with_port(
            lifecycle,
            now,
            &mut DriverTransactionOffsetCommitPort::new(driver),
        )
    }

    fn turn_with_port(
        &mut self,
        lifecycle: &mut TransactionLifecycleHost,
        now: Moment,
        port: &mut dyn TransactionOffsetCommitPort,
    ) -> Result<TransactionOffsetCommitTurn, TransactionOffsetCommitHostError> {
        if self.turn_completion()? {
            return Ok(TransactionOffsetCommitTurn::Progress);
        }
        let slot = core::mem::replace(&mut self.slot, TransactionOffsetCommitSlot::Vacant);
        match slot {
            TransactionOffsetCommitSlot::Vacant
            | TransactionOffsetCommitSlot::Terminal(_, _)
            | TransactionOffsetCommitSlot::Published => {
                self.slot = slot;
                Ok(TransactionOffsetCommitTurn::Idle)
            }
            TransactionOffsetCommitSlot::Ready(mut pending, stage) => {
                if pending
                    .retry_not_before
                    .is_some_and(|not_before| !not_before.is_elapsed_at(now))
                {
                    self.slot = TransactionOffsetCommitSlot::Ready(pending, stage);
                    Ok(TransactionOffsetCommitTurn::Idle)
                } else {
                    pending.retry_not_before = None;
                    self.submit(pending, stage, now, port)
                }
            }
            TransactionOffsetCommitSlot::Calling(pending, stage, mut call) => {
                match call.poll(pending.request.deadline().core().is_elapsed_at(now)) {
                    TransactionOffsetCommitPortCallPoll::Pending => {
                        self.slot = TransactionOffsetCommitSlot::Calling(pending, stage, call);
                        Ok(TransactionOffsetCommitTurn::Idle)
                    }
                    TransactionOffsetCommitPortCallPoll::Progress => {
                        self.slot = TransactionOffsetCommitSlot::Calling(pending, stage, call);
                        Ok(TransactionOffsetCommitTurn::Progress)
                    }
                    TransactionOffsetCommitPortCallPoll::DeadlineElapsed(evidence) => {
                        self.slot = TransactionOffsetCommitSlot::Settling(
                            pending,
                            stage,
                            super::deadline_evidence::deadline_evidence(evidence),
                        );
                        Ok(TransactionOffsetCommitTurn::Progress)
                    }
                    TransactionOffsetCommitPortCallPoll::Terminal(evidence) => {
                        self.slot = TransactionOffsetCommitSlot::Settling(pending, stage, evidence);
                        Ok(TransactionOffsetCommitTurn::Progress)
                    }
                }
            }
            TransactionOffsetCommitSlot::Settling(pending, stage, evidence) => {
                self.settle_evidence(pending, stage, evidence, lifecycle, Some(now))?;
                Ok(TransactionOffsetCommitTurn::Progress)
            }
        }
    }

    fn submit(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
        now: Moment,
        port: &mut dyn TransactionOffsetCommitPort,
    ) -> Result<TransactionOffsetCommitTurn, TransactionOffsetCommitHostError> {
        if pending.request.deadline().core().is_elapsed_at(now) {
            self.reject_ready(
                pending,
                stage,
                super::model::TransactionOffsetCommitFailure::new(
                    super::model::TransactionOffsetCommitFailureKind::DeadlineElapsed,
                    kafka_client_core::DeliveryStatus::NotSent,
                ),
            )?;
            return Ok(TransactionOffsetCommitTurn::Progress);
        }
        let request = TransactionOffsetCommitPortRequest {
            epoch: pending.request.epoch(),
            operation_id: pending.operation_id,
            stage,
            transactional_id: &pending.transactional_id,
            producer: pending.producer,
            group: pending.request.group(),
            offsets: pending.request.offsets(),
            deadline: pending.request.deadline().transport(),
        };
        match port.submit(request) {
            Ok(call) => {
                self.machine
                    .apply(TransactionOffsetCommitInput::DriverAccepted {
                        epoch: pending.request.epoch(),
                        operation_id: pending.operation_id,
                        stage,
                    })?;
                self.slot = TransactionOffsetCommitSlot::Calling(pending, stage, call);
            }
            Err((kind, delivery)) => {
                self.reject_ready(
                    pending,
                    stage,
                    super::model::TransactionOffsetCommitFailure::new(kind, delivery),
                )?;
            }
        }
        Ok(TransactionOffsetCommitTurn::Progress)
    }
}
