//! Automatic transactional partition resolution and enrollment handoff.

use kafka_client_core::{DeliveryStatus, TransactionSendOutcome};

use crate::{
    producer::{ProducerPartitionSource, materialization::TransactionalMaterializationBatch},
    transaction::{
        TransactionLifecycleHostError,
        partition_enrollment::TransactionPartitionEnrollmentAdmission,
    },
};

use super::super::{
    aggregate::TransactionSendAggregate,
    model::{TransactionSendFailure, TransactionSendFailureKind, TransactionSendTerminal},
    owner::TransactionSendOwner,
    partitioning::TransactionPartitioningFailure,
    turn::{PendingTransactionPartitioning, PendingTransactionSend, TransactionSendSlot},
};

impl TransactionSendOwner {
    #[expect(
        clippy::too_many_lines,
        reason = "one transition preserves partition selection, sequencing, and enrollment ownership"
    )]
    pub(super) fn apply_partitioning(
        &mut self,
        mut pending: PendingTransactionPartitioning,
        source: &dyn ProducerPartitionSource,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        let Ok(identity) = lifecycle.producer_identity() else {
            return self.finish_partitioning(
                pending,
                TransactionPartitioningFailure::MetadataUnavailable { broker_code: None },
                lifecycle,
            );
        };
        if pending
            .request
            .expected_topic_uuid()
            .is_some_and(|expected| source.kafka_topic_uuid() != Some(expected))
        {
            return self.finish_partitioning(
                pending,
                TransactionPartitioningFailure::TopicIdentityMismatch,
                lifecycle,
            );
        }
        let (partition, sticky) = if let Some(partition) = pending.request.partition() {
            (partition, false)
        } else {
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
            (partition, selection.sticky)
        };
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
        let expected_topic_uuid = pending.request.expected_topic_uuid();
        let validated_topic_generation = expected_topic_uuid.map(|_| source.generation());
        let (_, partition, topic, records, max_batch_bytes, deadline) =
            pending.request.into_parts();
        let batch = TransactionalMaterializationBatch::new(
            topic,
            raw_partition,
            records,
            max_batch_bytes,
            identity,
            sequence,
        )
        .with_expected_topic_identity(expected_topic_uuid, validated_topic_generation);
        let resolved = PendingTransactionSend {
            completion_id: pending.completion_id,
            epoch: pending.epoch,
            send_id: pending.send_id,
            partition,
            sequence,
            deadline,
            topic_id: partition.topic_id(),
            expected_topic_uuid,
            sticky,
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
                    TransactionSendTerminal::FailedHealthy {
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
