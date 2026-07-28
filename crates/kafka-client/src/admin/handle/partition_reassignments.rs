//! Partition-reassignment listing builders owned by the public Admin handle.

use crate::{
    TopicPartition,
    bridge::admin_list_partition_reassignments::ListPartitionReassignmentsAdminRequest,
};

use super::super::{Admin, ListPartitionReassignmentsBuilder};

impl Admin {
    /// Builds an inert query for selected active partition reassignments.
    pub fn list_partition_reassignments<I>(&self, targets: I) -> ListPartitionReassignmentsBuilder
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        let request =
            ListPartitionReassignmentsAdminRequest::selected(targets.into_iter().collect());
        ListPartitionReassignmentsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert query for all active partition reassignments.
    pub fn list_all_partition_reassignments(&self) -> ListPartitionReassignmentsBuilder {
        ListPartitionReassignmentsBuilder::new(
            self.engine.clone(),
            ListPartitionReassignmentsAdminRequest::all_active(),
            self.engine.default_timeout(),
        )
    }
}
