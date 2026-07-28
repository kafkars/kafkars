//! Narrow aggregate admission and terminal handoff for partition enrollment.

use kafka_client_core::TransactionEpoch;

use crate::{
    clock::OperationDeadline,
    producer::materialization::TransactionalMaterializationBatch,
    transaction::partition_enrollment::{
        TransactionPartitionEnrollmentAdmission, TransactionPartitionEnrollmentAdmissionFailure,
        TransactionPartitionEnrollmentTerminal,
    },
};

use super::host::TransactionLifecycleHost;

impl TransactionLifecycleHost {
    #[cfg(test)]
    pub(in crate::transaction) fn settle_pending_enrolled_for_test(&mut self) {
        self.enrollment.settle_pending_enrolled_for_test();
    }

    pub(crate) fn try_enroll_partition(
        &mut self,
        epoch: TransactionEpoch,
        batch: TransactionalMaterializationBatch,
        deadline: OperationDeadline,
    ) -> Result<
        TransactionPartitionEnrollmentAdmission,
        TransactionPartitionEnrollmentAdmissionFailure,
    > {
        self.enrollment.try_enroll(epoch, batch, deadline)
    }

    pub(crate) fn take_enrollment_terminal(
        &mut self,
    ) -> Option<TransactionPartitionEnrollmentTerminal> {
        self.enrollment.take_terminal()
    }
}
