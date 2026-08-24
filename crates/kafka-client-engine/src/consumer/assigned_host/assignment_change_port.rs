//! Synchronized port admission for incremental direct-assignment changes.

use kafka_client_core::AssignmentEpoch;

use super::{
    AssignedConsumerAssignment, AssignedConsumerPartition,
    result::{AssignedConsumerAccepted, AssignedConsumerPortError},
    shard::AssignedConsumerPort,
};

impl AssignedConsumerPort {
    pub(crate) fn add_assignments_captured(
        &self,
        entries: Vec<AssignedConsumerAssignment>,
        deadline: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerAccepted<Option<AssignmentEpoch>>, AssignedConsumerPortError> {
        let request_wake = !entries.is_empty();
        self.admit_with_wake(request_wake, move |owner| {
            owner.add_assignments_captured(entries, deadline)
        })
    }

    pub(crate) fn remove_assignments(
        &self,
        entries: Vec<AssignedConsumerPartition>,
    ) -> Result<AssignedConsumerAccepted<Option<AssignmentEpoch>>, AssignedConsumerPortError> {
        let request_wake = !entries.is_empty();
        self.admit_with_wake(request_wake, move |owner| {
            owner.remove_assignments(&entries)
        })
    }
}
