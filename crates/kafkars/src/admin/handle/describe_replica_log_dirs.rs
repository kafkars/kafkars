//! Selected-replica log-directory description entry point on the public admin handle.

use super::Admin;
use crate::{
    admin::{DescribeReplicaLogDirsBuilder, TopicPartitionReplica},
    bridge::admin_describe_replica_log_dirs::DescribeReplicaLogDirsAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered query for selected replica placements.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeReplicaLogDirsBuilder::submit`] is called.
    pub fn describe_replica_log_dirs<I>(&self, replicas: I) -> DescribeReplicaLogDirsBuilder
    where
        I: IntoIterator<Item = TopicPartitionReplica>,
    {
        let request = DescribeReplicaLogDirsAdminRequest::new(replicas.into_iter().collect());
        DescribeReplicaLogDirsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }
}
