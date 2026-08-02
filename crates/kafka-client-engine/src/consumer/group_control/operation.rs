//! Immediate batch control and clone-shared shutdown for one group handle.

use std::sync::Arc;

use kafka_client_core::GroupId;

use crate::consumer::{
    group::{GroupConsumerCloseAuthority, GroupConsumerPort},
    group_control::{
        accepted::GroupConsumerControlAccepted, error::GroupConsumerControlError,
        partition::GroupConsumerPartition,
    },
    group_registration::GroupConsumerHandle,
};

/// Cloneable exact-group shutdown capability.
#[derive(Clone)]
pub struct GroupConsumerControl {
    group_id: GroupId,
    close_authority: Arc<GroupConsumerCloseAuthority>,
    shutdown_port: GroupConsumerPort,
}

impl GroupConsumerControl {
    /// Requests idempotent shutdown of this exact registered group.
    ///
    /// The first request retains its call-boundary deadline in the group's
    /// preallocated close authority. Repeated requests and a later explicit
    /// close converge on that same broker leave and terminal cell.
    pub fn request_shutdown(&self) {
        let Some(deadline) = self.shutdown_port.capture_control_close_deadline() else {
            return;
        };
        if self.close_authority.request(deadline) {
            self.shutdown_port.request_control_shutdown_turn();
        }
    }
}

impl core::fmt::Debug for GroupConsumerControl {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerControl")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

impl GroupConsumerHandle {
    /// Returns a cloneable exact-group shutdown capability.
    pub fn control(&self) -> GroupConsumerControl {
        GroupConsumerControl {
            group_id: self.group_id,
            close_authority: Arc::clone(&self.close_authority),
            shutdown_port: self.port.clone(),
        }
    }

    /// Requests idempotent shutdown of this exact registered group.
    ///
    /// The first request retains its call-boundary deadline in the group's
    /// preallocated close authority. Repeated requests and a later explicit
    /// close converge on that same broker leave and terminal cell.
    pub fn request_shutdown(&self) {
        self.control().request_shutdown();
    }

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
