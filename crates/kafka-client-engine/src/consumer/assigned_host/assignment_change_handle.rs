//! Public handle admission for incremental direct-assignment changes.

use std::time::Duration;

use super::{
    AssignedConsumerAddAssignmentsCapture, AssignedConsumerAssignment, AssignedConsumerHandle,
    AssignedConsumerPartition, AssignedConsumerTryChangeAssignmentAccepted,
    AssignedConsumerTryChangeAssignmentError,
};

impl AssignedConsumerHandle {
    /// Attempts an immediate, all-or-nothing addition preserving every survivor fence.
    pub fn try_add_assignments(
        &mut self,
        entries: Vec<AssignedConsumerAssignment>,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError>
    {
        let capture = self.capture_add_assignments(resolution_timeout)?;
        capture.try_add_assignments(entries)
    }

    /// Captures the addition deadline before caller-owned input conversion.
    pub fn capture_add_assignments(
        &mut self,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerAddAssignmentsCapture<'_>, AssignedConsumerTryChangeAssignmentError>
    {
        let deadline = self
            .port
            .capture_assignment_deadline(resolution_timeout)
            .map_err(|error| AssignedConsumerTryChangeAssignmentError::from_port(&error))?;
        Ok(AssignedConsumerAddAssignmentsCapture::bind_addition_deadline_to_handle(self, deadline))
    }

    pub(super) fn try_add_assignments_captured(
        &mut self,
        entries: Vec<AssignedConsumerAssignment>,
        deadline: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError>
    {
        self.port
            .add_assignments_captured(entries, deadline)
            .map(AssignedConsumerTryChangeAssignmentAccepted::from_port)
            .map_err(|error| AssignedConsumerTryChangeAssignmentError::from_port(&error))
    }

    /// Attempts an immediate, deadline-free removal preserving every survivor fence.
    pub fn try_remove_assignments(
        &mut self,
        entries: Vec<AssignedConsumerPartition>,
    ) -> Result<AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError>
    {
        self.port
            .remove_assignments(entries)
            .map(AssignedConsumerTryChangeAssignmentAccepted::from_port)
            .map_err(|error| AssignedConsumerTryChangeAssignmentError::from_port(&error))
    }
}
