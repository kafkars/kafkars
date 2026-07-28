//! Test-only observations of retained Admin `AlterReplicaLogDirs` ownership.

use kafka_client_core::AlterReplicaLogDirAssignment;

use super::super::{AlterReplicaLogDirsHost, AlterReplicaLogDirsHostError};

impl AlterReplicaLogDirsHost {
    pub(in crate::admin::alter_replica_log_dirs) fn retain_recovered_call_for_test(
        &mut self,
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
    ) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredAlterReplicaLogDirsCall::for_test(
                broker_id,
                assignments,
                request_scratch_limit,
                result_limit,
            ));
    }

    pub(in crate::admin::alter_replica_log_dirs) fn recovered_ownership_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some() && self.operations[0].attempt.is_some()
    }

    pub(in crate::admin::alter_replica_log_dirs) fn recovered_matches_for_test(
        &self,
        broker_id: i32,
        assignments: &[AlterReplicaLogDirAssignment],
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| {
                recovered.matches_evidence(
                    broker_id,
                    assignments,
                    request_scratch_limit,
                    result_limit,
                )
            })
    }

    pub(in crate::admin::alter_replica_log_dirs) fn replace_call_with_raw_for_test(
        &mut self,
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
    ) {
        drop(self.operations[0].call.take());
        self.operations[0].raw_terminal =
            Some(crate::driver::AlterReplicaLogDirsRawTerminal::for_test(
                broker_id,
                assignments,
                request_scratch_limit,
                result_limit,
            ));
    }

    pub(in crate::admin::alter_replica_log_dirs) fn raw_is_retained_for_test(&self) -> bool {
        self.operations[0].raw_terminal.is_some()
    }

    pub(in crate::admin::alter_replica_log_dirs) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        self.settle_raw(0)
    }

    pub(in crate::admin::alter_replica_log_dirs) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::alter_replica_log_dirs) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        self.publish_terminal(0)
    }
}
