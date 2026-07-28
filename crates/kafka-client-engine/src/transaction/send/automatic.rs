//! Automatic transactional partition lookup and delayed sequence acquisition.

use kafka_client_core::{DeliveryStatus, Moment, TransactionSendId, TransactionSendOutcome};

use crate::{
    completion::CompletionId,
    driver::{DriverOwner, ProducerTopicViewCall, TopicPartitionCountAdmissionFailureKind},
    producer::materialization::TransactionalMaterializationBatch,
    transaction::{
        TransactionLifecycleHostError,
        partition_enrollment::TransactionPartitionEnrollmentAdmission,
    },
};

use super::{
    aggregate::TransactionSendAggregate,
    input::{TransactionSendAdmissionFailure, TransactionSendAdmissionFailureKind},
    model::{TransactionSendFailure, TransactionSendFailureKind, TransactionSendTurn},
    owner::TransactionSendOwner,
    partitioning::{TransactionPartitioningFailure, normalize_topic_view_failure},
    turn::{PendingTransactionPartitioning, PendingTransactionSend, TransactionSendSlot},
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
        source: &dyn kafka_client_core::partitioning::TopicPartitionSource,
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
        match ProducerTopicViewCall::submit(
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
        mut call: ProducerTopicViewCall,
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

    fn apply_partitioning(
        &mut self,
        mut pending: PendingTransactionPartitioning,
        source: &dyn kafka_client_core::partitioning::TopicPartitionSource,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        let identity = match lifecycle.producer_identity() {
            Ok(identity) => identity,
            Err(_error) => {
                return self.finish_partitioning(
                    pending,
                    TransactionPartitioningFailure::MetadataUnavailable { broker_code: None },
                    lifecycle,
                );
            }
        };
        let selection = match self.partitioners.select(&pending.request, source) {
            Ok(selection) => selection,
            Err(failure) => return self.finish_partitioning(pending, failure, lifecycle),
        };
        if !pending.request.assign_partition(selection.partition) {
            return self.finish_partitioning(
                pending,
                TransactionPartitioningFailure::MetadataUnavailable { broker_code: None },
                lifecycle,
            );
        }
        let partition = pending
            .request
            .partition()
            .unwrap_or_else(|| unreachable!("selected request owns one partition"));
        let raw_partition = i32::try_from(partition.partition().get()).unwrap_or_else(|_| {
            unreachable!("core topic partition count is Java signed-int representable")
        });
        let sequence = match lifecycle.sequence_accepted_send(
            pending.epoch,
            pending.send_id,
            partition,
            pending.request.record_count(),
        ) {
            Ok(sequence) => sequence,
            Err(_error) => {
                return self.finish_partitioning(
                    pending,
                    TransactionPartitioningFailure::MetadataUnavailable { broker_code: None },
                    lifecycle,
                );
            }
        };
        let (_, partition, topic, records, max_batch_bytes, deadline) =
            pending.request.into_parts();
        let batch = TransactionalMaterializationBatch::new(
            topic,
            raw_partition,
            records,
            max_batch_bytes,
            identity,
            sequence,
        );
        let resolved = PendingTransactionSend {
            completion_id: pending.completion_id,
            epoch: pending.epoch,
            send_id: pending.send_id,
            partition,
            sequence,
            deadline,
            topic_id: partition.topic_id(),
            sticky: selection.sticky,
            prepared: None,
        };
        match lifecycle.enroll(pending.epoch, batch, deadline) {
            Ok(TransactionPartitionEnrollmentAdmission::Pending) => {
                self.slot = TransactionSendSlot::Enrolling(resolved);
            }
            Ok(TransactionPartitionEnrollmentAdmission::Enrolled(fence)) => {
                self.slot = TransactionSendSlot::Ready(resolved, fence.into_batch());
            }
            Err(failure) => {
                let kind = failure.kind();
                drop(failure.into_batch());
                lifecycle.settle_unproduced(
                    pending.epoch,
                    pending.send_id,
                    partition,
                    sequence,
                    TransactionSendOutcome::FailedHealthy,
                )?;
                self.slot = TransactionSendSlot::Terminal(
                    pending.completion_id,
                    super::model::TransactionSendTerminal::FailedHealthy {
                        epoch: pending.epoch,
                        send_id: pending.send_id,
                        failure: TransactionSendFailure::new(
                            TransactionSendFailureKind::Enrollment(kind),
                            DeliveryStatus::NotSent,
                        ),
                    },
                );
            }
        }
        Ok(())
    }
}
