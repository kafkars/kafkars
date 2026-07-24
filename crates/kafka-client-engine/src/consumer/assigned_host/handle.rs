//! Unique runtime-neutral application ownership of one assigned consumer.

use std::{cell::Cell, marker::PhantomData, sync::Arc, time::Duration};

use super::{
    assignment_result::{
        AssignedConsumerAssignmentEpoch, AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError,
    },
    control::AssignedConsumerPartition,
    control_result::{AssignedConsumerControlAccepted, AssignedConsumerControlError},
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
    port: AssignedConsumerPort,
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

    /// Attempts an immediate, all-or-nothing replacement of every partition.
    pub fn try_replace_assignment(
        &mut self,
        entries: Vec<crate::consumer::AssignedConsumerAssignment>,
        resolution_timeout: Duration,
    ) -> Result<
        AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError,
    > {
        self.port
            .replace_assignment(entries, resolution_timeout)
            .map(AssignedConsumerTryReplaceAssignmentAccepted::from_port)
            .map_err(|error| AssignedConsumerTryReplaceAssignmentError::from_port(&error))
    }

    /// Attempts to fence and pause one partition in the supplied assignment generation.
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
        self.port
            .resume(epoch.into_core(), partition, resolution_timeout)
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
        self.port
            .seek(epoch.into_core(), partition, position, resolution_timeout)
            .map(AssignedConsumerControlAccepted::from_port)
            .map_err(|error| AssignedConsumerControlError::from_port(&error))
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

    #[cfg(test)]
    pub(crate) fn begin_close_for_test(
        &self,
    ) -> Result<
        super::result::AssignedConsumerAccepted<super::AssignedConsumerCloseObserver>,
        super::result::AssignedConsumerPortError,
    > {
        self.port.begin_close()
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
