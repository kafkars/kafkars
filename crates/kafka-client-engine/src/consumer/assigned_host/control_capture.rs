//! Linear resume and seek deadlines bound to one assigned-consumer handle.

use crate::clock::DeadlineCapture;

use super::{
    AssignedConsumerAssignmentEpoch, AssignedConsumerControlAccepted, AssignedConsumerControlError,
    AssignedConsumerHandle, AssignedConsumerPartition, AssignedConsumerStartPosition,
};

/// One absolute resume deadline bound to one mutably borrowed handle.
#[must_use = "dropping abandons the captured deadline without admitting resume"]
pub struct AssignedConsumerResumeCapture<'handle> {
    handle: &'handle mut AssignedConsumerHandle,
    deadline: DeadlineCapture,
}

impl<'handle> AssignedConsumerResumeCapture<'handle> {
    pub(super) const fn bind_deadline_to_handle(
        handle: &'handle mut AssignedConsumerHandle,
        deadline: DeadlineCapture,
    ) -> Self {
        Self { handle, deadline }
    }

    /// Consumes this exact capture while attempting one fenced resume.
    pub fn try_resume(
        self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        self.handle
            .try_resume_captured(epoch, partition, self.deadline)
    }
}

/// One absolute seek deadline bound to one mutably borrowed handle.
#[must_use = "dropping abandons the captured deadline without admitting seek"]
pub struct AssignedConsumerSeekCapture<'handle> {
    handle: &'handle mut AssignedConsumerHandle,
    deadline: DeadlineCapture,
}

impl<'handle> AssignedConsumerSeekCapture<'handle> {
    pub(super) const fn bind_deadline_to_handle(
        handle: &'handle mut AssignedConsumerHandle,
        deadline: DeadlineCapture,
    ) -> Self {
        Self { handle, deadline }
    }

    /// Consumes this exact capture while attempting one fenced seek.
    pub fn try_seek(
        self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
        position: AssignedConsumerStartPosition,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        self.handle
            .try_seek_captured(epoch, partition, position, self.deadline)
    }
}

impl std::fmt::Debug for AssignedConsumerResumeCapture<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerResumeCapture")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AssignedConsumerSeekCapture<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerSeekCapture")
            .finish_non_exhaustive()
    }
}
