//! Linear group-resume deadline captured before facade target conversion.

use std::time::Duration;

use crate::{
    clock::DeadlineCapture,
    consumer::{
        GroupConsumerHandle,
        group_control::{
            GroupConsumerControlAccepted, GroupConsumerControlError, GroupConsumerPartition,
        },
    },
};

const DEFAULT_GROUP_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Stable failure before one group-resume deadline could be captured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerResumeCaptureErrorKind {
    /// The synchronized engine clock cannot capture a resume boundary.
    HostUnavailable,
}

/// Failure before facade target conversion or deterministic mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerResumeCaptureError {
    kind: GroupConsumerResumeCaptureErrorKind,
}

impl GroupConsumerResumeCaptureError {
    /// Returns the stable capture failure category.
    pub const fn kind(&self) -> GroupConsumerResumeCaptureErrorKind {
        self.kind
    }
}

impl core::fmt::Display for GroupConsumerResumeCaptureError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "classic-group resume deadline capture failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerResumeCaptureError {}

/// One absolute child-resolution deadline bound to the unique group handle.
#[must_use = "dropping abandons the captured deadline without admitting resume"]
pub struct GroupConsumerResumeCapture<'handle> {
    handle: &'handle mut GroupConsumerHandle,
    capture: DeadlineCapture,
}

impl GroupConsumerHandle {
    /// Captures resume time before facade target conversion.
    pub fn capture_resume(
        &mut self,
    ) -> Result<GroupConsumerResumeCapture<'_>, GroupConsumerResumeCaptureError> {
        let capture = self
            .port
            .capture_group_resume_deadline(DEFAULT_GROUP_RESOLUTION_TIMEOUT)
            .map_err(|_error| GroupConsumerResumeCaptureError {
                kind: GroupConsumerResumeCaptureErrorKind::HostUnavailable,
            })?;
        Ok(GroupConsumerResumeCapture {
            handle: self,
            capture,
        })
    }
}

impl GroupConsumerResumeCapture<'_> {
    /// Consumes this exact boundary while attempting one atomic resume batch.
    pub fn resume(
        self,
        partitions: Vec<GroupConsumerPartition>,
    ) -> Result<GroupConsumerControlAccepted, GroupConsumerControlError> {
        self.handle.resume_captured(partitions, self.capture)
    }
}

impl core::fmt::Debug for GroupConsumerResumeCapture<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerResumeCapture")
            .finish_non_exhaustive()
    }
}
