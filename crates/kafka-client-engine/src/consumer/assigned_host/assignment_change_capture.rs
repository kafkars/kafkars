//! Linear deadline capture for one incremental direct-assignment addition.

use crate::clock::DeadlineCapture;

use super::{
    AssignedConsumerAssignment, AssignedConsumerHandle,
    assignment_change_result::{
        AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError,
    },
};

/// One absolute addition deadline bound to one mutably borrowed handle.
#[must_use = "dropping abandons the captured deadline without admitting assignment work"]
pub struct AssignedConsumerAddAssignmentsCapture<'handle> {
    handle: &'handle mut AssignedConsumerHandle,
    deadline: DeadlineCapture,
}

impl<'handle> AssignedConsumerAddAssignmentsCapture<'handle> {
    pub(super) const fn bind_addition_deadline_to_handle(
        handle: &'handle mut AssignedConsumerHandle,
        deadline: DeadlineCapture,
    ) -> Self {
        Self { handle, deadline }
    }

    /// Consumes this capture while attempting one atomic incremental addition.
    pub fn try_add_assignments(
        self,
        entries: Vec<AssignedConsumerAssignment>,
    ) -> Result<AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError>
    {
        self.handle
            .try_add_assignments_captured(entries, self.deadline)
    }
}

impl std::fmt::Debug for AssignedConsumerAddAssignmentsCapture<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerAddAssignmentsCapture")
            .finish_non_exhaustive()
    }
}
