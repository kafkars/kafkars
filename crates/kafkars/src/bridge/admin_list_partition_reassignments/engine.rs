//! Focused engine admission for partition-reassignment listing.

use std::time::Duration;

use super::{AdminListPartitionReassignments, ListPartitionReassignmentsAdminRequest};
use crate::bridge::admin::AdminEngine;

impl AdminEngine {
    pub(crate) fn submit_list_partition_reassignments(
        &self,
        request: ListPartitionReassignmentsAdminRequest,
        timeout: Duration,
    ) -> AdminListPartitionReassignments {
        AdminListPartitionReassignments::from_admission(
            self.handle
                .try_list_partition_reassignments(request.into_engine(), timeout),
        )
    }
}
