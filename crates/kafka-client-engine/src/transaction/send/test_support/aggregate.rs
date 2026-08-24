//! Fake aggregate delegation across lifecycle, enrollment, and retry ownership.

use std::sync::Arc;

use kafka_client_core::{
    DeliveryStatus, Moment, TransactionEpoch, TransactionPartition, TransactionSendAttempt,
    TransactionSendAttemptFailure, TransactionSendId, TransactionSendIdentity,
    TransactionSendOutcome, TransactionSequenceLease, TransactionSequenceSettlement,
    TransactionalProducerIdentity,
};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    producer::materialization::TransactionalMaterializationBatch,
    transaction::{
        TransactionLifecycleHostError, TransactionLifecycleTurn, TransactionSendReplacement,
        partition_enrollment::{
            TransactionPartitionEnrollmentAdmission,
            TransactionPartitionEnrollmentAdmissionFailure,
            TransactionPartitionEnrollmentFailureKind, TransactionPartitionEnrollmentTerminal,
        },
    },
};

use super::{super::aggregate::TransactionSendAggregate, FakeAggregate};

impl TransactionSendAggregate for FakeAggregate {
    fn transactional_id_owner(&self) -> Result<Arc<str>, TransactionLifecycleHostError> {
        self.host.transactional_id_owner()
    }

    fn producer_identity(
        &self,
    ) -> Result<TransactionalProducerIdentity, TransactionLifecycleHostError> {
        self.host.producer_identity()
    }

    fn accept_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError> {
        self.host
            .accept_send_with_sequence(epoch, send_id, partition, record_count)
    }

    fn accept_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.host.accept_unsequenced_send(epoch, send_id)
    }

    fn sequence_accepted_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError> {
        self.host
            .sequence_accepted_send(epoch, send_id, partition, record_count)
    }

    fn settle_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.host.settle_unsequenced_send(epoch, send_id, outcome)
    }

    fn enroll(
        &mut self,
        epoch: TransactionEpoch,
        batch: TransactionalMaterializationBatch,
        deadline: OperationDeadline,
    ) -> Result<
        TransactionPartitionEnrollmentAdmission,
        TransactionPartitionEnrollmentAdmissionFailure,
    > {
        if self.local_enrollment {
            self.host.try_enroll_partition(epoch, batch, deadline)
        } else {
            self.captured = Some(batch);
            Ok(TransactionPartitionEnrollmentAdmission::Pending)
        }
    }

    fn drive_enrollment(
        &mut self,
        _now: Moment,
        _driver: &DriverOwner,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        Ok(TransactionLifecycleTurn::Idle)
    }

    fn take_enrollment_terminal(&mut self) -> Option<TransactionPartitionEnrollmentTerminal> {
        self.terminal.take()
    }

    fn settle_unproduced(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.host
            .settle_unproduced_send(epoch, send_id, partition, lease, outcome)
    }

    fn settle_accepted(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        settlement: TransactionSequenceSettlement,
    ) -> Result<TransactionSendOutcome, TransactionLifecycleHostError> {
        let outcome = self
            .host
            .settle_accepted_send(epoch, send_id, partition, lease, settlement)?;
        self.log
            .lock()
            .unwrap_or_else(|error| panic!("log: {error:?}"))
            .push("settle");
        Ok(outcome)
    }

    fn prepare_send_attempt(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        identity: TransactionSendIdentity,
    ) -> Result<TransactionSendAttempt, TransactionLifecycleHostError> {
        self.prepared_identities.push(identity);
        self.host.prepare_send_attempt(epoch, send_id, identity)
    }

    fn authorize_send_replacement(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        attempt: TransactionSendAttempt,
        now: Moment,
        failure: TransactionSendAttemptFailure,
    ) -> Result<Option<TransactionSendReplacement>, TransactionLifecycleHostError> {
        self.host
            .authorize_send_replacement(epoch, send_id, attempt, now, failure)
    }

    fn recover_after_driver_shutdown(&mut self) -> Result<(), TransactionLifecycleHostError> {
        if let Some(batch) = self.captured.take() {
            self.terminal = Some(TransactionPartitionEnrollmentTerminal::AbortRequired {
                kind: TransactionPartitionEnrollmentFailureKind::DriverClosed,
                delivery: DeliveryStatus::PossiblySent,
                batch,
            });
        }
        Ok(())
    }
}
