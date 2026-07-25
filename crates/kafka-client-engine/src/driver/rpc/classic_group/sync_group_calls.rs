//! Bounded active and terminal ownership for concrete tracked `SyncGroup` calls.

use kafka_driver::RoutedCall;
use kafka_wire::{SyncGroupRequest, SyncGroupResponse};

use crate::driver::DriverOwner;

use super::{
    sync_group_settlement::{PendingSyncGroupConfirmation, SettledSyncGroupCall, SyncGroupPoll},
    sync_group_terminal::{
        RecoveredSyncGroupCall, SyncGroupAdmissionFailure, SyncGroupCallKey,
        SyncGroupCompletionFailure, SyncGroupCompletionObservation, retain_sync_group_terminal,
    },
};

pub(super) struct TrackedSyncGroupCall {
    key: SyncGroupCallKey,
    call: RoutedCall<SyncGroupResponse>,
}

impl TrackedSyncGroupCall {
    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredSyncGroupCall {
        drop(self.call);
        RecoveredSyncGroupCall::new(self.key)
    }
}

/// Linear proof that the driver accepted one exact Sync call.
#[must_use = "an accepted SyncGroup call must settle or recover after driver shutdown"]
pub(crate) struct AcceptedSyncGroupCall {
    key: SyncGroupCallKey,
}

impl AcceptedSyncGroupCall {
    const fn new(key: SyncGroupCallKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> SyncGroupCallKey {
        self.key
    }

    pub(super) fn confirm_sync_group_call_receipt(self) {
        let Self {
            key: _confirmed_key,
        } = self;
    }

    #[cfg(test)]
    pub(super) const fn from_key_for_test(key: SyncGroupCallKey) -> Self {
        Self::new(key)
    }
}

/// Preflighted ownership of exactly one bounded Sync call slot.
#[must_use = "a reserved SyncGroup call slot must be submitted or released"]
pub(crate) struct SyncGroupCallPermit<'a> {
    key: SyncGroupCallKey,
    group: &'a str,
    calls: &'a mut Vec<TrackedSyncGroupCall>,
}

impl SyncGroupCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        request: SyncGroupRequest,
    ) -> Result<AcceptedSyncGroupCall, SyncGroupAdmissionFailure> {
        let call = driver
            .submit_tracked_sync_group(self.group, request, self.key.deadline().transport())
            .map_err(|source| SyncGroupAdmissionFailure::new(self.key, source))?;
        self.calls.push(TrackedSyncGroupCall {
            key: self.key,
            call,
        });
        Ok(AcceptedSyncGroupCall::new(self.key))
    }
}

/// Why a Sync call slot could not be reserved without moving its request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncGroupCallReservationError {
    Capacity { limit: usize },
    Duplicate { key: SyncGroupCallKey },
}

/// Capacity-bounded registry of active, settled, confirming, and corrupted Sync calls.
pub(crate) struct TrackedSyncGroupCalls {
    capacity: usize,
    pub(super) calls: Vec<TrackedSyncGroupCall>,
    pub(super) settled: Option<SettledSyncGroupCall>,
    pub(super) pending_confirmation: Option<PendingSyncGroupConfirmation>,
    pub(super) completion_failure: Option<SyncGroupCompletionFailure>,
}

impl TrackedSyncGroupCalls {
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::try_new(capacity)
            .unwrap_or_else(|_error| panic!("test SyncGroup call reservation failed"))
    }

    pub(crate) fn try_new(capacity: usize) -> Result<Self, std::collections::TryReserveError> {
        let mut calls = Vec::new();
        calls.try_reserve_exact(capacity)?;
        Ok(Self {
            capacity,
            calls,
            settled: None,
            pending_confirmation: None,
            completion_failure: None,
        })
    }

    pub(crate) fn try_reserve_sync_group<'a>(
        &'a mut self,
        key: SyncGroupCallKey,
        group: &'a str,
    ) -> Result<SyncGroupCallPermit<'a>, SyncGroupCallReservationError> {
        if self.contains_cycle(key) {
            return Err(SyncGroupCallReservationError::Duplicate { key });
        }
        if self.retained_sync_group_count() >= self.capacity {
            return Err(SyncGroupCallReservationError::Capacity {
                limit: self.capacity,
            });
        }
        Ok(SyncGroupCallPermit {
            key,
            group,
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_sync_group_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_confirmation.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    fn contains_cycle(&self, key: SyncGroupCallKey) -> bool {
        self.calls
            .iter()
            .any(|call| call.key.same_cycle_identity(key))
            || self
                .settled
                .as_ref()
                .is_some_and(|call| call.key().same_cycle_identity(key))
            || self
                .pending_confirmation
                .as_ref()
                .is_some_and(|pending| pending.key().same_cycle_identity(key))
            || self
                .completion_failure
                .as_ref()
                .is_some_and(|failure| failure.key().same_cycle_identity(key))
    }

    pub(crate) fn poll_sync_group(
        &mut self,
    ) -> Result<SyncGroupPoll, SyncGroupCompletionObservation> {
        if let Some(failure) = &self.completion_failure {
            return Err(failure.observation());
        }
        if let Some(pending) = &self.pending_confirmation {
            return Ok(SyncGroupPoll::ConfirmationPending { key: pending.key() });
        }
        if let Some(settled) = &self.settled {
            return Ok(SyncGroupPoll::TerminalReady { key: settled.key() });
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(SyncGroupPoll::Idle);
        };
        let tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let failure = SyncGroupCompletionFailure::new(tracked.key, source);
                let observation = failure.observation();
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        };
        let (result, selected_version, route_token) = outcome.into_parts();
        let terminal = retain_sync_group_terminal(tracked.key, selected_version, result);
        self.settled = Some(SettledSyncGroupCall::new(terminal, route_token));
        Ok(SyncGroupPoll::TerminalReady { key: tracked.key })
    }

    #[cfg(test)]
    pub(crate) fn install_terminal_for_test(
        &mut self,
        key: SyncGroupCallKey,
        selected_version: Option<i16>,
        result: Result<SyncGroupResponse, kafka_driver::RequestError>,
    ) {
        let selected_version = selected_version.map(kafka_driver::ApiVersion::new);
        let terminal = retain_sync_group_terminal(key, selected_version, result);
        self.settled = Some(SettledSyncGroupCall::new(terminal, None));
    }

    #[cfg(test)]
    pub(crate) fn install_completion_failure_for_test(
        &mut self,
        key: SyncGroupCallKey,
        source: kafka_driver::CompletionError,
    ) {
        self.completion_failure = Some(SyncGroupCompletionFailure::new(key, source));
    }
}
