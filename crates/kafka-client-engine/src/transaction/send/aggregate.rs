//! Narrow adapter over frozen lifecycle, enrollment, and sequencing ownership.

use std::sync::Arc;

use kafka_client_core::{
    Moment, TransactionEpoch, TransactionPartition, TransactionSendAttempt,
    TransactionSendAttemptFailure, TransactionSendId, TransactionSendIdentity,
    TransactionSendOutcome, TransactionSequenceLease, TransactionSequenceSettlement,
    TransactionalProducerIdentity,
};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    producer::materialization::TransactionalMaterializationBatch,
    transaction::{
        TransactionLifecycleHost, TransactionLifecycleHostError, TransactionLifecycleTurn,
        TransactionSendReplacement,
        partition_enrollment::{
            TransactionPartitionEnrollmentAdmission,
            TransactionPartitionEnrollmentAdmissionFailure, TransactionPartitionEnrollmentTerminal,
        },
    },
};

pub(in crate::transaction) trait TransactionSendAggregate {
    fn transactional_id_owner(&self) -> Result<Arc<str>, TransactionLifecycleHostError>;

    fn producer_identity(
        &self,
    ) -> Result<TransactionalProducerIdentity, TransactionLifecycleHostError>;

    fn accept_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError>;

    fn accept_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<(), TransactionLifecycleHostError>;

    fn sequence_accepted_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError>;

    fn settle_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError>;

    fn enroll(
        &mut self,
        epoch: TransactionEpoch,
        batch: TransactionalMaterializationBatch,
        deadline: OperationDeadline,
    ) -> Result<
        TransactionPartitionEnrollmentAdmission,
        TransactionPartitionEnrollmentAdmissionFailure,
    >;

    fn drive_enrollment(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError>;

    fn take_enrollment_terminal(&mut self) -> Option<TransactionPartitionEnrollmentTerminal>;

    fn settle_unproduced(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError>;

    fn settle_accepted(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        settlement: TransactionSequenceSettlement,
    ) -> Result<TransactionSendOutcome, TransactionLifecycleHostError>;

    fn prepare_send_attempt(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        identity: TransactionSendIdentity,
    ) -> Result<TransactionSendAttempt, TransactionLifecycleHostError>;

    fn authorize_send_replacement(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        attempt: TransactionSendAttempt,
        now: Moment,
        failure: TransactionSendAttemptFailure,
    ) -> Result<Option<TransactionSendReplacement>, TransactionLifecycleHostError>;

    fn recover_after_driver_shutdown(&mut self) -> Result<(), TransactionLifecycleHostError>;
}

impl TransactionSendAggregate for TransactionLifecycleHost {
    fn transactional_id_owner(&self) -> Result<Arc<str>, TransactionLifecycleHostError> {
        self.transactional_id_owner()
    }

    fn producer_identity(
        &self,
    ) -> Result<TransactionalProducerIdentity, TransactionLifecycleHostError> {
        self.producer_identity()
    }

    fn accept_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError> {
        self.accept_send_with_sequence(epoch, send_id, partition, record_count)
    }

    fn accept_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.accept_unsequenced_send(epoch, send_id)
    }

    fn sequence_accepted_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        record_count: usize,
    ) -> Result<TransactionSequenceLease, TransactionLifecycleHostError> {
        self.sequence_accepted_send(epoch, send_id, partition, record_count)
    }

    fn settle_unsequenced_send(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.settle_unsequenced_send(epoch, send_id, outcome)
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
        self.try_enroll_partition(epoch, batch, deadline)
    }

    fn drive_enrollment(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        self.turn(now, driver)
    }

    fn take_enrollment_terminal(&mut self) -> Option<TransactionPartitionEnrollmentTerminal> {
        self.take_enrollment_terminal()
    }

    fn settle_unproduced(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        outcome: TransactionSendOutcome,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.settle_unproduced_send(epoch, send_id, partition, lease, outcome)
    }

    fn settle_accepted(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: TransactionPartition,
        lease: TransactionSequenceLease,
        settlement: TransactionSequenceSettlement,
    ) -> Result<TransactionSendOutcome, TransactionLifecycleHostError> {
        self.settle_accepted_send(epoch, send_id, partition, lease, settlement)
    }

    fn prepare_send_attempt(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        identity: TransactionSendIdentity,
    ) -> Result<TransactionSendAttempt, TransactionLifecycleHostError> {
        self.prepare_send_attempt(epoch, send_id, identity)
    }

    fn authorize_send_replacement(
        &mut self,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        attempt: TransactionSendAttempt,
        now: Moment,
        failure: TransactionSendAttemptFailure,
    ) -> Result<Option<TransactionSendReplacement>, TransactionLifecycleHostError> {
        self.authorize_send_replacement(epoch, send_id, attempt, now, failure)
    }

    fn recover_after_driver_shutdown(&mut self) -> Result<(), TransactionLifecycleHostError> {
        self.recover_enrollment_after_driver_shutdown();
        Ok(())
    }
}
