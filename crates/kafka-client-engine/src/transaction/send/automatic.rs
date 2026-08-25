//! Automatic transactional partition lookup and delayed sequence acquisition.

use kafka_client_core::{Moment, TransactionSendId};

use crate::{
    completion::CompletionId,
    driver::{DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicRouteViewCall},
    transaction::TransactionLifecycleHostError,
};

#[cfg(test)]
use crate::producer::ProducerPartitionSource;

mod resolution;

use super::{
    aggregate::TransactionSendAggregate,
    input::{TransactionSendAdmissionFailure, TransactionSendAdmissionFailureKind},
    model::TransactionSendTurn,
    owner::TransactionSendOwner,
    partitioning::{TransactionPartitioningFailure, normalize_topic_view_failure},
    turn::{PendingTransactionPartitioning, TransactionSendSlot},
};

impl TransactionSendOwner {
    #[expect(
        clippy::result_large_err,
        reason = "automatic admission rejection returns the exact caller-owned record"
    )]
    pub(super) fn accept_automatic(
        &mut self,
        lifecycle: &mut dyn TransactionSendAggregate,
        send_id: TransactionSendId,
        completion_id: CompletionId,
    ) -> Result<(), TransactionSendAdmissionFailure> {
        let epoch = self.reserved().epoch();
        if let Err(error) = lifecycle.accept_unsequenced_send(epoch, send_id) {
            return Err(self.rollback(TransactionSendAdmissionFailureKind::Lifecycle(error)));
        }
        self.next_send_id = send_id
            .get()
            .checked_add(1)
            .map(TransactionSendId::from_raw);
        let (request, reserved_completion_id) = self.take_reserved();
        debug_assert_eq!(reserved_completion_id, completion_id);
        self.slot = TransactionSendSlot::AwaitingPartition(PendingTransactionPartitioning {
            completion_id,
            epoch,
            send_id,
            request,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn apply_partitioning_for_test(
        &mut self,
        source: &dyn ProducerPartitionSource,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        let TransactionSendSlot::AwaitingPartition(pending) =
            core::mem::replace(&mut self.slot, TransactionSendSlot::Vacant)
        else {
            unreachable!("test partition view requires one waiting automatic send")
        };
        self.apply_partitioning(pending, source, lifecycle)
    }

    pub(super) fn submit_partitioning(
        &mut self,
        pending: PendingTransactionPartitioning,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        if pending.request.deadline().core().is_elapsed_at(now) {
            self.finish_partitioning(
                pending,
                TransactionPartitioningFailure::DeadlineElapsed,
                lifecycle,
            )?;
            return Ok(TransactionSendTurn::Progress);
        }
        match TopicRouteViewCall::submit(
            driver,
            pending.request.topic(),
            pending.request.deadline().transport(),
        ) {
            Ok(call) => self.slot = TransactionSendSlot::Partitioning(pending, call),
            Err(error) if error.kind() == TopicPartitionCountAdmissionFailureKind::Full => {
                self.slot = TransactionSendSlot::AwaitingPartition(pending);
                return Ok(TransactionSendTurn::Idle);
            }
            Err(_error) => self.finish_partitioning(
                pending,
                TransactionPartitioningFailure::MetadataUnavailable { broker_code: None },
                lifecycle,
            )?,
        }
        Ok(TransactionSendTurn::Progress)
    }

    pub(super) fn poll_partitioning(
        &mut self,
        pending: PendingTransactionPartitioning,
        mut call: TopicRouteViewCall,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        let Some(result) = call.try_terminal() else {
            self.slot = TransactionSendSlot::Partitioning(pending, call);
            return Ok(TransactionSendTurn::Idle);
        };
        match result {
            Ok(view) => self.apply_partitioning(pending, &view, lifecycle)?,
            Err(failure) => {
                self.finish_partitioning(
                    pending,
                    normalize_topic_view_failure(failure),
                    lifecycle,
                )?;
            }
        }
        Ok(TransactionSendTurn::Progress)
    }
}
