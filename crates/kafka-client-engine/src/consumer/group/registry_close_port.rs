//! Deadline-captured explicit-close admission and terminal probing for one group.

use std::{sync::Arc, time::Duration};

use kafka_client_core::GroupId;

use crate::{
    clock::ClockError,
    consumer::group_recv::{GroupConsumerRecvRegistration, GroupConsumerRecvWait},
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_leave::{GroupConsumerCloseAuthority, GroupConsumerCloseCompletion},
    registry_close::GroupRegistryCloseError,
    registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
    registry_wake::GroupConsumerShardWakeError,
};

pub(super) const DEFAULT_EXPLICIT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

impl GroupConsumerPort {
    /// Reserves terminal notification capacity before fencing group admission.
    pub(in crate::consumer) fn try_begin_close(
        &self,
        group_id: GroupId,
        authority: &Arc<GroupConsumerCloseAuthority>,
    ) -> Result<GroupConsumerCloseAdmission, GroupConsumerClosePortError> {
        let capture = self
            .clock
            .capture_deadline_after(DEFAULT_EXPLICIT_CLOSE_TIMEOUT)
            .map_err(GroupConsumerClosePortError::Clock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerClosePortError::Closed);
        }
        let registration = self
            .arm_group_recv_blocking(group_id, None, GroupConsumerRecvWait::Unlock)
            .map_err(|_error| GroupConsumerClosePortError::Notification)?;
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                self.cancel_group_recv(&mut Some(registration));
                return Err(GroupConsumerClosePortError::Lock(error));
            }
        };
        if self.shared.admission_is_closed() {
            drop(registry);
            self.cancel_group_recv(&mut Some(registration));
            return Err(GroupConsumerClosePortError::Closed);
        }
        let completion = match registry.close_group_explicit(
            group_id,
            capture.operation_deadline(),
            authority,
        ) {
            Ok(completion) => completion,
            Err(error) => {
                drop(registry);
                self.cancel_group_recv(&mut Some(registration));
                return Err(GroupConsumerClosePortError::Registry(error));
            }
        };
        drop(registry);
        Ok(GroupConsumerCloseAdmission {
            completion,
            registration,
            wake: self.shared.request_turn().err(),
        })
    }

    pub(in crate::consumer) fn capture_control_close_deadline(
        &self,
    ) -> Option<crate::clock::OperationDeadline> {
        self.clock
            .capture_deadline_after(DEFAULT_EXPLICIT_CLOSE_TIMEOUT)
            .ok()
            .map(crate::clock::DeadlineCapture::operation_deadline)
    }

    pub(in crate::consumer) fn request_control_shutdown_turn(&self) {
        let _wake_result = self.shared.request_turn();
    }

    /// Observes only whether one accepted close still retains its exact entry.
    pub(in crate::consumer) fn observe_close(
        &self,
        group_id: GroupId,
    ) -> Result<GroupConsumerCloseObservation, GroupConsumerCloseObservationError> {
        let registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(GroupConsumerShardLockError::Contended) => {
                return Ok(GroupConsumerCloseObservation::Pending);
            }
            Err(error) => return Err(GroupConsumerCloseObservationError::Lock(error)),
        };
        let observation = match registry.entry(group_id) {
            None => GroupConsumerCloseObservation::Complete,
            Some(entry)
                if entry.state == GroupConsumerEntryState::Closing
                    && entry.fault.as_ref().is_some_and(|fault| {
                        !ClassicGroupEntryFault::allows_explicit_close_progress(fault)
                    }) =>
            {
                GroupConsumerCloseObservation::Faulted
            }
            Some(entry) if entry.state == GroupConsumerEntryState::Closing => {
                GroupConsumerCloseObservation::Pending
            }
            Some(_) => GroupConsumerCloseObservation::NotAccepted,
        };
        Ok(observation)
    }
}

#[must_use = "accepted close retains its bounded notification registration"]
pub(in crate::consumer) struct GroupConsumerCloseAdmission {
    pub(in crate::consumer) completion: Arc<GroupConsumerCloseCompletion>,
    pub(in crate::consumer) registration: GroupConsumerRecvRegistration,
    wake: Option<GroupConsumerShardWakeError>,
}

impl GroupConsumerCloseAdmission {
    pub(in crate::consumer) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerClosePortError {
    Closed,
    Clock(ClockError),
    Notification,
    Lock(GroupConsumerShardLockError),
    Registry(GroupRegistryCloseError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerCloseObservation {
    Pending,
    Complete,
    Faulted,
    NotAccepted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerCloseObservationError {
    Lock(GroupConsumerShardLockError),
}
