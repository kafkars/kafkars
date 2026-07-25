//! Bounded active and terminal ownership for concrete tracked `JoinGroup` calls.

use kafka_driver::RoutedCall;
use kafka_wire::JoinGroupResponse;

use crate::{driver::DriverOwner, protocol::consumer::PreparedClassicJoinGroupRequest};

use super::{
    join_group_settlement::{JoinGroupPoll, PendingJoinGroupConfirmation, SettledJoinGroupCall},
    join_group_terminal::{
        JoinGroupAdmissionFailure, JoinGroupCallKey, JoinGroupCompletionFailure,
        JoinGroupCompletionObservation, RecoveredJoinGroupCall, retain_join_group_terminal,
    },
};

pub(super) struct TrackedJoinGroupCall {
    key: JoinGroupCallKey,
    call: RoutedCall<JoinGroupResponse>,
}

impl TrackedJoinGroupCall {
    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredJoinGroupCall {
        drop(self.call);
        RecoveredJoinGroupCall::new(self.key)
    }
}

/// Linear proof that the driver accepted one exact Join call.
#[must_use = "an accepted JoinGroup call must settle or recover after driver shutdown"]
pub(crate) struct AcceptedJoinGroupCall {
    key: JoinGroupCallKey,
}

impl AcceptedJoinGroupCall {
    const fn new(key: JoinGroupCallKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> JoinGroupCallKey {
        self.key
    }

    pub(super) fn confirm_join_group_call_receipt(self) {
        let Self {
            key: _confirmed_key,
        } = self;
    }

    #[cfg(test)]
    pub(super) const fn from_key_for_test(key: JoinGroupCallKey) -> Self {
        Self::new(key)
    }
}

/// Preflighted ownership of exactly one bounded Join call slot.
#[must_use = "a reserved JoinGroup call slot must be submitted or released"]
pub(crate) struct JoinGroupCallPermit<'a> {
    key: JoinGroupCallKey,
    group: &'a str,
    calls: &'a mut Vec<TrackedJoinGroupCall>,
}

impl JoinGroupCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        request: PreparedClassicJoinGroupRequest,
    ) -> Result<AcceptedJoinGroupCall, JoinGroupAdmissionFailure> {
        let call = driver
            .submit_tracked_join_group(
                self.group,
                request.into_generated_join_group_request(),
                self.key.deadline().transport(),
            )
            .map_err(|source| JoinGroupAdmissionFailure::new(self.key, source))?;
        self.calls.push(TrackedJoinGroupCall {
            key: self.key,
            call,
        });
        Ok(AcceptedJoinGroupCall::new(self.key))
    }
}

/// Why a Join call slot could not be reserved without moving its request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinGroupCallReservationError {
    Capacity { limit: usize },
    Duplicate { key: JoinGroupCallKey },
}

/// Capacity-bounded registry of active, settled, confirming, and corrupted Join calls.
pub(crate) struct TrackedJoinGroupCalls {
    capacity: usize,
    pub(super) calls: Vec<TrackedJoinGroupCall>,
    pub(super) settled: Option<SettledJoinGroupCall>,
    pub(super) pending_confirmation: Option<PendingJoinGroupConfirmation>,
    pub(super) completion_failure: Option<JoinGroupCompletionFailure>,
}

impl TrackedJoinGroupCalls {
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::try_new(capacity)
            .unwrap_or_else(|_error| panic!("test JoinGroup call reservation failed"))
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

    pub(crate) fn try_reserve_join_group<'a>(
        &'a mut self,
        key: JoinGroupCallKey,
        group: &'a str,
    ) -> Result<JoinGroupCallPermit<'a>, JoinGroupCallReservationError> {
        if self.contains_cycle(key) {
            return Err(JoinGroupCallReservationError::Duplicate { key });
        }
        if self.retained_join_group_count() >= self.capacity {
            return Err(JoinGroupCallReservationError::Capacity {
                limit: self.capacity,
            });
        }
        Ok(JoinGroupCallPermit {
            key,
            group,
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_join_group_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_confirmation.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    fn contains_cycle(&self, key: JoinGroupCallKey) -> bool {
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

    pub(crate) fn poll_join_group(
        &mut self,
    ) -> Result<JoinGroupPoll, JoinGroupCompletionObservation> {
        if let Some(failure) = &self.completion_failure {
            return Err(failure.observation());
        }
        if let Some(pending) = &self.pending_confirmation {
            return Ok(JoinGroupPoll::ConfirmationPending { key: pending.key() });
        }
        if let Some(settled) = &self.settled {
            return Ok(JoinGroupPoll::TerminalReady { key: settled.key() });
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(JoinGroupPoll::Idle);
        };
        let tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let failure = JoinGroupCompletionFailure::new(tracked.key, source);
                let observation = failure.observation();
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        };
        let (result, selected_version, route_token) = outcome.into_parts();
        let terminal = retain_join_group_terminal(tracked.key, selected_version, result);
        self.settled = Some(SettledJoinGroupCall::new(terminal, route_token));
        Ok(JoinGroupPoll::TerminalReady { key: tracked.key })
    }

    #[cfg(test)]
    pub(crate) fn install_terminal_for_test(
        &mut self,
        key: JoinGroupCallKey,
        selected_version: Option<i16>,
        result: Result<JoinGroupResponse, kafka_driver::RequestError>,
    ) {
        let selected_version = selected_version.map(kafka_driver::ApiVersion::new);
        let terminal = retain_join_group_terminal(key, selected_version, result);
        self.settled = Some(SettledJoinGroupCall::new(terminal, None));
    }

    #[cfg(test)]
    pub(crate) fn install_completion_failure_for_test(
        &mut self,
        key: JoinGroupCallKey,
        source: kafka_driver::CompletionError,
    ) {
        self.completion_failure = Some(JoinGroupCompletionFailure::new(key, source));
    }
}
