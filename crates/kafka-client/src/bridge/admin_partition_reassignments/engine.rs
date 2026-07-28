//! Sole engine admission seam for partition-reassignment alteration.

use std::time::Duration;

use super::{AdminAlterPartitionReassignments, AlterPartitionReassignmentsAdminRequest};
use crate::bridge::admin::AdminEngine;

impl AdminEngine {
    pub(crate) fn submit_alter_partition_reassignments(
        &self,
        request: AlterPartitionReassignmentsAdminRequest,
        timeout: Duration,
    ) -> AdminAlterPartitionReassignments {
        AdminAlterPartitionReassignments::from_admission(
            self.handle
                .try_alter_partition_reassignments(request.into_engine(), timeout),
        )
    }
}
