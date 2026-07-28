//! Concrete generated-v4 and tracked-driver adapter for offset transfer.

use std::sync::Arc;

use kafka_client_core::{DeliveryStatus, TransactionOffsetCommitStage};

use crate::{
    driver::{
        DriverOwner,
        transaction_offsets::{
            TransactionAddOffsetsCall, TransactionOffsetCommitCall, TransactionOffsetCommitPoll,
            TransactionOffsetCommitTarget,
        },
    },
    protocol::transaction::TransactionGroupIdentityRef,
};

use super::{
    driver_evidence::{self, Correlation},
    model::TransactionOffsetCommitFailureKind,
    port::{
        TransactionOffsetCommitPort, TransactionOffsetCommitPortCall,
        TransactionOffsetCommitPortCallPoll, TransactionOffsetCommitPortEvidence,
        TransactionOffsetCommitPortRequest,
    },
};

pub(super) struct DriverTransactionOffsetCommitPort<'a> {
    driver: &'a DriverOwner,
}

impl<'a> DriverTransactionOffsetCommitPort<'a> {
    pub(super) const fn new(driver: &'a DriverOwner) -> Self {
        Self { driver }
    }
}

impl TransactionOffsetCommitPort for DriverTransactionOffsetCommitPort<'_> {
    fn submit(
        &mut self,
        request: TransactionOffsetCommitPortRequest<'_>,
    ) -> Result<
        Box<dyn TransactionOffsetCommitPortCall>,
        (TransactionOffsetCommitFailureKind, DeliveryStatus),
    > {
        let correlation = (request.epoch, request.operation_id, request.stage);
        let call = match request.stage {
            TransactionOffsetCommitStage::AddOffsets => TransactionOffsetDriverCall::Add(
                TransactionAddOffsetsCall::submit(
                    self.driver,
                    request.transactional_id,
                    request.producer.producer_id(),
                    request.producer.producer_epoch(),
                    request.group.group_id(),
                    request.deadline,
                )
                .map_err(|_| not_sent())?,
            ),
            TransactionOffsetCommitStage::TxnOffsetCommit => {
                let mut targets = Vec::new();
                targets
                    .try_reserve_exact(request.offsets.len())
                    .map_err(|_| {
                        (
                            TransactionOffsetCommitFailureKind::Allocation,
                            DeliveryStatus::NotSent,
                        )
                    })?;
                targets.extend(request.offsets.iter().map(|offset| {
                    TransactionOffsetCommitTarget::new(
                        Arc::clone(offset.topic()),
                        offset.partition(),
                        offset.next_offset(),
                        offset.leader_epoch(),
                        offset.metadata().map(Arc::clone),
                    )
                }));
                let group = TransactionGroupIdentityRef::new(
                    request.group.group_id(),
                    request.group.generation_id(),
                    request.group.member_id(),
                    request.group.group_instance_id(),
                );
                TransactionOffsetDriverCall::Commit(
                    TransactionOffsetCommitCall::submit(
                        self.driver,
                        request.transactional_id,
                        request.producer.producer_id(),
                        request.producer.producer_epoch(),
                        group,
                        targets,
                        request.deadline,
                    )
                    .map_err(|_| not_sent())?,
                )
            }
        };
        Ok(Box::new(DriverTransactionOffsetCommitCall {
            correlation,
            call,
        }))
    }
}

struct DriverTransactionOffsetCommitCall {
    correlation: Correlation,
    call: TransactionOffsetDriverCall,
}

enum TransactionOffsetDriverCall {
    Add(TransactionAddOffsetsCall),
    Commit(TransactionOffsetCommitCall),
}

impl TransactionOffsetCommitPortCall for DriverTransactionOffsetCommitCall {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionOffsetCommitPortCallPoll {
        if deadline_elapsed {
            let evidence = match &mut self.call {
                TransactionOffsetDriverCall::Add(call) => call
                    .expire_refresh()
                    .map(|terminal| driver_evidence::add(self.correlation, terminal)),
                TransactionOffsetDriverCall::Commit(call) => call
                    .expire_refresh()
                    .map(|terminal| driver_evidence::commit(self.correlation, terminal)),
            };
            if let Some(evidence) = evidence {
                return TransactionOffsetCommitPortCallPoll::DeadlineElapsed(evidence);
            }
        }
        let evidence = match &mut self.call {
            TransactionOffsetDriverCall::Add(call) => match call.poll() {
                crate::driver::transaction_offsets::TransactionAddOffsetsPoll::Pending => {
                    return TransactionOffsetCommitPortCallPoll::Pending;
                }
                crate::driver::transaction_offsets::TransactionAddOffsetsPoll::Progress => {
                    return TransactionOffsetCommitPortCallPoll::Progress;
                }
                crate::driver::transaction_offsets::TransactionAddOffsetsPoll::Terminal(Ok(
                    terminal,
                )) => driver_evidence::add(self.correlation, terminal),
                crate::driver::transaction_offsets::TransactionAddOffsetsPoll::Terminal(Err(
                    _closed,
                )) => driver_evidence::closed(self.correlation),
            },
            TransactionOffsetDriverCall::Commit(call) => match call.poll() {
                TransactionOffsetCommitPoll::Pending => {
                    return TransactionOffsetCommitPortCallPoll::Pending;
                }
                TransactionOffsetCommitPoll::Progress => {
                    return TransactionOffsetCommitPortCallPoll::Progress;
                }
                TransactionOffsetCommitPoll::Terminal(Ok(terminal)) => {
                    driver_evidence::commit(self.correlation, terminal)
                }
                TransactionOffsetCommitPoll::Terminal(Err(_closed)) => {
                    driver_evidence::closed(self.correlation)
                }
            },
        };
        TransactionOffsetCommitPortCallPoll::Terminal(evidence)
    }

    fn recover_after_driver_shutdown(
        self: Box<Self>,
    ) -> Box<dyn TransactionOffsetCommitPortEvidence> {
        let correlation = self.correlation;
        match self.call {
            TransactionOffsetDriverCall::Add(call) => {
                if let Some(recovered) = call.recover_after_driver_shutdown()
                    && let Some(terminal) = recovered.into_terminal()
                {
                    return driver_evidence::add(correlation, terminal);
                }
            }
            TransactionOffsetDriverCall::Commit(call) => {
                if let Some(recovered) = call.recover_after_driver_shutdown()
                    && let Some(terminal) = recovered.into_terminal()
                {
                    return driver_evidence::commit(correlation, terminal);
                }
            }
        }
        driver_evidence::closed(correlation)
    }
}

const fn not_sent() -> (TransactionOffsetCommitFailureKind, DeliveryStatus) {
    (
        TransactionOffsetCommitFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    )
}
