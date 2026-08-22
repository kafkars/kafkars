//! Exact-epoch local admission before tracked driver ownership.

use std::sync::Arc;

use kafka_client_core::{DeliveryStatus, TransactionEpoch};

use crate::{
    clock::OperationDeadline, producer::materialization::TransactionalMaterializationBatch,
};

use super::{
    host::{PendingEnrollment, TransactionPartitionEnrollmentOwner},
    model::{
        TransactionPartitionEnrollmentAdmission, TransactionPartitionEnrollmentAdmissionFailure,
        TransactionPartitionEnrollmentEpochError, TransactionPartitionEnrollmentFailureKind,
        TransactionPartitionEnrollmentFence, TransactionPartitionEnrollmentTarget,
    },
};

impl TransactionPartitionEnrollmentOwner {
    /// Proves that activation can follow a successful lifecycle Begin unchanged.
    pub(crate) fn preflight_activate_epoch(
        &self,
    ) -> Result<(), TransactionPartitionEnrollmentEpochError> {
        if self.active_epoch.is_some() {
            return Err(TransactionPartitionEnrollmentEpochError::AlreadyActive);
        }
        if self.pending.is_some() || self.terminal.is_some() || !self.enrolled.is_empty() {
            return Err(TransactionPartitionEnrollmentEpochError::Unsettled);
        }
        Ok(())
    }

    /// Activates one nonreused core transaction epoch over an empty set.
    pub(crate) fn activate_epoch(
        &mut self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionPartitionEnrollmentEpochError> {
        self.preflight_activate_epoch()?;
        self.active_epoch = Some(epoch);
        Ok(())
    }

    /// Proves exact-epoch release can follow successful `EndTxn` settlement.
    pub(crate) fn preflight_release_epoch(
        &self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionPartitionEnrollmentEpochError> {
        let Some(active) = self.active_epoch else {
            return Err(TransactionPartitionEnrollmentEpochError::NotActive);
        };
        if active != epoch {
            return Err(TransactionPartitionEnrollmentEpochError::EpochMismatch);
        }
        if self.pending.is_some() || self.terminal.is_some() {
            return Err(TransactionPartitionEnrollmentEpochError::Unsettled);
        }
        Ok(())
    }

    /// Clears the enrolled set only after exact `EndTxn` settlement.
    pub(crate) fn release_epoch(
        &mut self,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionPartitionEnrollmentEpochError> {
        self.preflight_release_epoch(epoch)?;
        self.enrolled.clear();
        self.retained_topic_bytes = 0;
        self.active_epoch = None;
        Ok(())
    }

    /// Transfers one exact batch into immediate or pending enrollment ownership.
    pub(crate) fn try_enroll(
        &mut self,
        epoch: TransactionEpoch,
        batch: TransactionalMaterializationBatch,
        deadline: OperationDeadline,
    ) -> Result<
        TransactionPartitionEnrollmentAdmission,
        TransactionPartitionEnrollmentAdmissionFailure,
    > {
        if let Err(kind) = self.validate_admission(epoch, &batch) {
            return Err(local_failure(kind, batch));
        }
        if self.is_enrolled(batch.topic(), batch.partition()) {
            return Ok(TransactionPartitionEnrollmentAdmission::Enrolled(
                TransactionPartitionEnrollmentFence::new(epoch, batch),
            ));
        }
        if let Err(kind) = self.validate_retention(&batch) {
            return Err(local_failure(kind, batch));
        }
        self.pending = Some(PendingEnrollment {
            epoch,
            target: TransactionPartitionEnrollmentTarget::new(
                Arc::clone(batch.topic()),
                batch.partition(),
            ),
            deadline,
            retry_not_before: None,
            retries_started: 0,
            delivery_floor: DeliveryStatus::NotSent,
            batch: Some(batch),
            call: None,
        });
        Ok(TransactionPartitionEnrollmentAdmission::Pending)
    }

    fn validate_admission(
        &self,
        epoch: TransactionEpoch,
        batch: &TransactionalMaterializationBatch,
    ) -> Result<(), TransactionPartitionEnrollmentFailureKind> {
        if self.pending.is_some() || self.terminal.is_some() {
            return Err(TransactionPartitionEnrollmentFailureKind::Busy);
        }
        if self.active_epoch != Some(epoch) {
            return Err(TransactionPartitionEnrollmentFailureKind::EpochMismatch);
        }
        if batch.identity() != self.identity.producer() {
            return Err(TransactionPartitionEnrollmentFailureKind::OwnerMismatch);
        }
        if batch.topic().is_empty() || batch.partition() < 0 {
            return Err(TransactionPartitionEnrollmentFailureKind::InvalidTarget);
        }
        Ok(())
    }

    fn validate_retention(
        &self,
        batch: &TransactionalMaterializationBatch,
    ) -> Result<(), TransactionPartitionEnrollmentFailureKind> {
        if self.enrolled.len() == self.limits.max_partitions() {
            return Err(TransactionPartitionEnrollmentFailureKind::Capacity);
        }
        let retained = self
            .retained_topic_bytes
            .checked_add(batch.topic().len())
            .ok_or(TransactionPartitionEnrollmentFailureKind::RetainedBytes)?;
        if retained > self.limits.max_topic_bytes() {
            return Err(TransactionPartitionEnrollmentFailureKind::RetainedBytes);
        }
        Ok(())
    }

    fn is_enrolled(&self, topic: &Arc<str>, partition: i32) -> bool {
        self.enrolled
            .iter()
            .any(|target| target.topic() == topic && target.partition() == partition)
    }
}

const fn local_failure(
    kind: TransactionPartitionEnrollmentFailureKind,
    batch: TransactionalMaterializationBatch,
) -> TransactionPartitionEnrollmentAdmissionFailure {
    TransactionPartitionEnrollmentAdmissionFailure::new(kind, batch)
}
