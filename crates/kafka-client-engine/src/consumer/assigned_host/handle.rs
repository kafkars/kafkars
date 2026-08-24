//! Unique runtime-neutral application ownership of one assigned consumer.

use std::{cell::Cell, marker::PhantomData, sync::Arc, time::Duration};

use super::{
    assignment_capture::AssignedConsumerAssignmentCapture,
    assignment_result::{
        AssignedConsumerAssignmentEpoch, AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError,
    },
    control::AssignedConsumerPartition,
    control_capture::{AssignedConsumerResumeCapture, AssignedConsumerSeekCapture},
    control_result::{AssignedConsumerControlAccepted, AssignedConsumerControlError},
    delivery::{AssignedConsumerBatch, AssignedConsumerTryTakeBatchError},
    event::{AssignedConsumerEvent, AssignedConsumerTryTakeEventError},
    next_event::AssignedConsumerNextEvent,
    recv::AssignedConsumerRecv,
    result::{AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError},
    shard::AssignedConsumerPort,
};

/// Sole application-side capability for the engine's assigned consumer.
///
/// The handle is movable between threads but deliberately neither cloneable
/// nor shareable. Later consumer operations attach here without exposing the
/// synchronized port or its deterministic owner.
#[must_use = "dropping the unique handle relinquishes assigned-consumer access"]
pub struct AssignedConsumerHandle {
    pub(in crate::consumer::assigned_host) port: AssignedConsumerPort,
    lifetime: Arc<dyn Send + Sync>,
    _not_sync: PhantomData<Cell<()>>,
}

impl AssignedConsumerHandle {
    pub(super) const fn new(port: AssignedConsumerPort, lifetime: Arc<dyn Send + Sync>) -> Self {
        Self {
            port,
            lifetime,
            _not_sync: PhantomData,
        }
    }

    /// Waits for one already-authorized background Fetch delivery.
    ///
    /// This operation creates no Fetch attempt or application timeout.
    pub fn recv(&mut self) -> AssignedConsumerRecv<'_> {
        AssignedConsumerRecv::new(self)
    }

    /// Waits for one already-retained assigned-consumer failure event.
    ///
    /// This operation creates no Fetch attempt, deadline, or reactor work.
    pub fn next_event(&mut self) -> AssignedConsumerNextEvent<'_> {
        AssignedConsumerNextEvent::new(self)
    }

    /// Attempts an immediate, all-or-nothing replacement of every partition.
    pub fn try_replace_assignment(
        &mut self,
        entries: Vec<crate::consumer::AssignedConsumerAssignment>,
        resolution_timeout: Duration,
    ) -> Result<
        AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError,
    > {
        let capture = self.capture_replace_assignment(resolution_timeout)?;
        capture.try_replace_assignment(entries)
    }

    /// Captures the operation deadline before caller-owned input conversion.
    pub fn capture_replace_assignment(
        &mut self,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerAssignmentCapture<'_>, AssignedConsumerTryReplaceAssignmentError>
    {
        let deadline = self
            .port
            .capture_assignment_deadline(resolution_timeout)
            .map_err(|error| AssignedConsumerTryReplaceAssignmentError::from_port(&error))?;
        Ok(AssignedConsumerAssignmentCapture::bind_deadline_to_handle(
            self, deadline,
        ))
    }

    pub(super) fn try_replace_assignment_captured(
        &mut self,
        entries: Vec<crate::consumer::AssignedConsumerAssignment>,
        deadline: crate::clock::DeadlineCapture,
    ) -> Result<
        AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError,
    > {
        self.port
            .replace_assignment_captured(entries, deadline)
            .map(AssignedConsumerTryReplaceAssignmentAccepted::from_port)
            .map_err(|error| AssignedConsumerTryReplaceAssignmentError::from_port(&error))
    }

    /// Attempts to fence and pause one partition under the supplied control revision.
    pub fn try_pause(
        &mut self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        self.port
            .pause(epoch.into_core(), partition)
            .map(AssignedConsumerControlAccepted::from_port)
            .map_err(|error| AssignedConsumerControlError::from_port(&error))
    }

    /// Attempts to resume one paused partition under one call-boundary deadline.
    pub fn try_resume(
        &mut self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        let capture = self.capture_resume(resolution_timeout)?;
        capture.try_resume(epoch, partition)
    }

    /// Captures the resume deadline before caller-owned target conversion.
    pub fn capture_resume(
        &mut self,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerResumeCapture<'_>, AssignedConsumerControlError> {
        let deadline = self
            .port
            .capture_control_deadline(resolution_timeout)
            .map_err(|error| AssignedConsumerControlError::from_port(&error))?;
        Ok(AssignedConsumerResumeCapture::bind_deadline_to_handle(
            self, deadline,
        ))
    }

    pub(super) fn try_resume_captured(
        &mut self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
        deadline: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        self.port
            .resume_captured(epoch.into_core(), partition, deadline)
            .map(AssignedConsumerControlAccepted::from_port)
            .map_err(|error| AssignedConsumerControlError::from_port(&error))
    }

    /// Attempts to replace one partition's next position under one call-boundary deadline.
    pub fn try_seek(
        &mut self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
        position: super::AssignedConsumerStartPosition,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        let capture = self.capture_seek(resolution_timeout)?;
        capture.try_seek(epoch, partition, position)
    }

    /// Captures the seek deadline before caller-owned target and position conversion.
    pub fn capture_seek(
        &mut self,
        resolution_timeout: Duration,
    ) -> Result<AssignedConsumerSeekCapture<'_>, AssignedConsumerControlError> {
        let deadline = self
            .port
            .capture_control_deadline(resolution_timeout)
            .map_err(|error| AssignedConsumerControlError::from_port(&error))?;
        Ok(AssignedConsumerSeekCapture::bind_deadline_to_handle(
            self, deadline,
        ))
    }

    pub(super) fn try_seek_captured(
        &mut self,
        epoch: AssignedConsumerAssignmentEpoch,
        partition: AssignedConsumerPartition,
        position: super::AssignedConsumerStartPosition,
        deadline: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerControlAccepted, AssignedConsumerControlError> {
        self.port
            .seek_captured(epoch.into_core(), partition, position, deadline)
            .map(AssignedConsumerControlAccepted::from_port)
            .map_err(|error| AssignedConsumerControlError::from_port(&error))
    }

    /// Immediately transfers one ready batch, or reports that none is ready.
    ///
    /// Fetch execution is engine-owned background work. This method only
    /// observes already-authorized delivery and does not start a new timeout.
    pub fn try_take_batch(
        &mut self,
    ) -> Result<Option<AssignedConsumerBatch>, AssignedConsumerTryTakeBatchError> {
        self.port
            .take_named_delivery()
            .map(|delivery| {
                delivery
                    .map(|batch| AssignedConsumerBatch::new(batch, Arc::clone(&self.port.shared)))
            })
            .map_err(|error| AssignedConsumerTryTakeBatchError::from_port(&error))
    }

    /// Immediately transfers one retained scalar failure event, if ready.
    ///
    /// This does not wait, start Fetch work, request a reactor turn, or reopen
    /// admission after close.
    pub fn try_take_event(
        &mut self,
    ) -> Result<Option<AssignedConsumerEvent>, AssignedConsumerTryTakeEventError> {
        self.port
            .take_event()
            .map_err(|error| AssignedConsumerTryTakeEventError::from_port(&error))
    }

    /// Attempts immediate close after reserving the sole terminal-completion lane.
    pub fn try_close(
        &mut self,
    ) -> Result<AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError> {
        self.port
            .begin_close()
            .map(AssignedConsumerTryCloseAccepted::from_port)
            .map_err(|error| AssignedConsumerTryCloseError::from_port(&error))
    }
}

impl std::fmt::Debug for AssignedConsumerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerHandle")
            .field("host_retained", &Arc::strong_count(&self.lifetime))
            .finish_non_exhaustive()
    }
}
