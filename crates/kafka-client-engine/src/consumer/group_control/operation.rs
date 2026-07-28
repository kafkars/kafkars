//! Immediate atomic pause and resume entry points on one unique group handle.

use crate::consumer::{
    group_control::{
        accepted::GroupConsumerControlAccepted, error::GroupConsumerControlError,
        partition::GroupConsumerPartition,
    },
    group_registration::GroupConsumerHandle,
};

impl GroupConsumerHandle {
    /// Atomically pauses one caller-ordered unique partition batch.
    pub fn pause(
        &mut self,
        partitions: Vec<GroupConsumerPartition>,
    ) -> Result<GroupConsumerControlAccepted, GroupConsumerControlError> {
        if partitions.is_empty() {
            return Ok(GroupConsumerControlAccepted::inert());
        }
        match self.port.try_pause_partitions(self.group_id, &partitions) {
            Ok(accepted) => Ok(GroupConsumerControlAccepted::from_port(accepted)),
            Err(error) => Err(GroupConsumerControlError::from_port(error, partitions)),
        }
    }

    /// Atomically resumes retained positions for one caller-ordered unique batch.
    pub fn resume(
        &mut self,
        partitions: Vec<GroupConsumerPartition>,
    ) -> Result<GroupConsumerControlAccepted, GroupConsumerControlError> {
        if partitions.is_empty() {
            return Ok(GroupConsumerControlAccepted::inert());
        }
        match self.capture_resume() {
            Ok(capture) => capture.resume(partitions),
            Err(error) => Err(GroupConsumerControlError::from_resume_capture(
                error, partitions,
            )),
        }
    }

    pub(super) fn resume_captured(
        &mut self,
        partitions: Vec<GroupConsumerPartition>,
        capture: crate::clock::DeadlineCapture,
    ) -> Result<GroupConsumerControlAccepted, GroupConsumerControlError> {
        if partitions.is_empty() {
            return Ok(GroupConsumerControlAccepted::inert());
        }
        match self
            .port
            .try_resume_partitions_captured(self.group_id, &partitions, capture)
        {
            Ok(accepted) => Ok(GroupConsumerControlAccepted::from_port(accepted)),
            Err(error) => Err(GroupConsumerControlError::from_port(error, partitions)),
        }
    }
}
