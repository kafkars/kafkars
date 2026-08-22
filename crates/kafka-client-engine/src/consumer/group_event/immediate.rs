//! Immediate observation through the unique classic-group capability.

use std::sync::Arc;

use kafka_client_core::GroupId;

use super::{
    GroupConsumerEvent, GroupConsumerRevocationAcknowledgeError, GroupConsumerState,
    GroupConsumerStateError, GroupConsumerStateErrorKind, GroupConsumerTryTakeEventError,
};
use crate::consumer::{
    GroupConsumerEventPortError, GroupConsumerHandle, GroupConsumerPort,
    GroupConsumerStartupFailureKind, GroupConsumerStatePortError,
};

/// Linear capability for completing one observed graceful-revocation lease.
///
/// Creating this value starts no protocol work and captures no new deadline.
/// The exact assignment epoch is supplied by the event that owns this
/// capability.
pub struct GroupConsumerRevocationControl {
    group_id: GroupId,
    port: GroupConsumerPort,
    _lifetime: Arc<dyn Send + Sync>,
}

impl GroupConsumerRevocationControl {
    /// Completes the exact assignment lease named by an observed event.
    pub fn complete(
        &mut self,
        assignment_epoch: u64,
    ) -> Result<(), GroupConsumerRevocationAcknowledgeError> {
        self.port
            .try_acknowledge_revocation(self.group_id, assignment_epoch)
            .map_err(GroupConsumerRevocationAcknowledgeError::from_port)
    }
}

impl core::fmt::Debug for GroupConsumerRevocationControl {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerRevocationControl")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

impl GroupConsumerHandle {
    /// Returns the exact retained KIP-848 terminal cause, if one exists.
    #[doc(hidden)]
    pub fn startup_failure(&self) -> Option<GroupConsumerStartupFailureKind> {
        self.port
            .try_consumer_group_startup_failure(self.group_id)
            .ok()
            .flatten()
    }

    /// Transfers one completion capability to the next observed revocation.
    pub fn revocation_control(&self) -> GroupConsumerRevocationControl {
        GroupConsumerRevocationControl {
            group_id: self.group_id,
            port: self.port.clone(),
            _lifetime: Arc::clone(&self.lifetime),
        }
    }

    /// Immediately copies the current driver-confirmed membership and assignment.
    ///
    /// `Ok(None)` means no Sync-confirmed membership is current. This starts no
    /// group work, requests no reactor turn, and does not consume rebalance events.
    pub fn state(&self) -> Result<Option<GroupConsumerState>, GroupConsumerStateError> {
        match self.port.try_group_state(self.group_id) {
            Ok(state) => Ok(state),
            Err(error) if error.is_terminal() => Ok(None),
            Err(error) => Err(GroupConsumerStateError::new(match error {
                GroupConsumerStatePortError::Lock(error) if error.is_contended() => {
                    GroupConsumerStateErrorKind::Contended
                }
                GroupConsumerStatePortError::Lock(error) if error.is_poisoned() => {
                    GroupConsumerStateErrorKind::HostUnavailable
                }
                GroupConsumerStatePortError::Registry(error) if error.is_allocation() => {
                    GroupConsumerStateErrorKind::Allocation
                }
                GroupConsumerStatePortError::Closed
                | GroupConsumerStatePortError::Lock(_)
                | GroupConsumerStatePortError::Registry(_) => {
                    GroupConsumerStateErrorKind::InternalInvariant
                }
            })),
        }
    }

    /// Immediately transfers one retained classic-group transition, if ready.
    ///
    /// This does not wait, start group work, request a reactor turn, or reopen
    /// observation after the event stream terminates.
    pub fn try_take_event(
        &mut self,
    ) -> Result<Option<GroupConsumerEvent>, GroupConsumerTryTakeEventError> {
        translate_immediate_result(self.port.try_take_event(self.group_id))
    }
}

pub(super) fn translate_immediate_result(
    result: Result<Option<GroupConsumerEvent>, GroupConsumerEventPortError>,
) -> Result<Option<GroupConsumerEvent>, GroupConsumerTryTakeEventError> {
    match result {
        Ok(event) => Ok(event),
        Err(error) if error.is_terminal() => Ok(None),
        Err(error) => Err(GroupConsumerTryTakeEventError::from_port(error)),
    }
}
