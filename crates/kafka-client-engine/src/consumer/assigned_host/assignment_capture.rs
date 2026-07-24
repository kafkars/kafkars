//! Linear deadline capture bound to one assigned-consumer handle and operation.

use crate::clock::DeadlineCapture;

use super::{
    AssignedConsumerAssignment, AssignedConsumerHandle,
    assignment_result::{
        AssignedConsumerTryReplaceAssignmentAccepted, AssignedConsumerTryReplaceAssignmentError,
    },
};

/// One absolute assignment deadline bound to one mutably borrowed handle.
///
/// Consuming this token is the only way to admit the corresponding assignment,
/// so safe Rust prevents reuse or admission through another handle.
#[must_use = "dropping abandons the captured deadline without admitting assignment work"]
pub struct AssignedConsumerAssignmentCapture<'handle> {
    handle: &'handle mut AssignedConsumerHandle,
    deadline: DeadlineCapture,
}

impl<'handle> AssignedConsumerAssignmentCapture<'handle> {
    pub(super) const fn bind_deadline_to_handle(
        handle: &'handle mut AssignedConsumerHandle,
        deadline: DeadlineCapture,
    ) -> Self {
        Self { handle, deadline }
    }

    /// Consumes this exact capture while attempting all-or-nothing admission.
    pub fn try_replace_assignment(
        self,
        entries: Vec<AssignedConsumerAssignment>,
    ) -> Result<
        AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError,
    > {
        self.handle
            .try_replace_assignment_captured(entries, self.deadline)
    }
}

impl std::fmt::Debug for AssignedConsumerAssignmentCapture<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerAssignmentCapture")
            .finish_non_exhaustive()
    }
}
