//! Test-only observations of retained Admin `DescribeReplicaLogDirs` ownership.

use super::super::{DescribeReplicaLogDirsHost, DescribeReplicaLogDirsHostError};

impl DescribeReplicaLogDirsHost {
    pub(in crate::admin::describe_replica_log_dirs) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeReplicaLogDirsCall::for_test());
    }

    pub(in crate::admin::describe_replica_log_dirs) fn recovered_call_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_replica_log_dirs) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::describe_replica_log_dirs) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), DescribeReplicaLogDirsHostError> {
        self.publish_terminal(0)
    }
}
