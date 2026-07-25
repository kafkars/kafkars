//! Bounded active and terminal ownership for tracked classic Heartbeat calls.

use kafka_driver::RoutedCall;
use kafka_wire::HeartbeatResponse;

use crate::{driver::DriverOwner, protocol::consumer::PreparedClassicHeartbeatRequest};

use super::{
    heartbeat_settlement::{
        ClassicHeartbeatPoll, PendingClassicHeartbeatConfirmation, SettledClassicHeartbeatCall,
    },
    heartbeat_terminal::{
        ClassicHeartbeatAdmissionFailure, ClassicHeartbeatCallKey,
        ClassicHeartbeatCompletionFailure, ClassicHeartbeatCompletionObservation,
        RecoveredClassicHeartbeatCall, retain_classic_heartbeat_terminal,
    },
};

pub(super) struct TrackedClassicHeartbeatCall {
    key: ClassicHeartbeatCallKey,
    call: RoutedCall<HeartbeatResponse>,
}

impl TrackedClassicHeartbeatCall {
    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredClassicHeartbeatCall {
        drop(self.call);
        RecoveredClassicHeartbeatCall::new(self.key)
    }
}

/// Linear proof that the driver accepted one exact Heartbeat call.
#[must_use = "an accepted Heartbeat call must settle or recover after driver shutdown"]
pub(crate) struct AcceptedClassicHeartbeatCall {
    key: ClassicHeartbeatCallKey,
}

impl AcceptedClassicHeartbeatCall {
    const fn new(key: ClassicHeartbeatCallKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }

    pub(super) fn confirm_classic_heartbeat_call_receipt(self) {
        let Self {
            key: _confirmed_key,
        } = self;
    }

    pub(super) fn consume_classic_heartbeat_shutdown_receipt(self) {
        let Self {
            key: _recovered_key,
        } = self;
    }

    #[cfg(test)]
    pub(crate) const fn from_key_for_test(key: ClassicHeartbeatCallKey) -> Self {
        Self::new(key)
    }
}

/// Preflighted ownership of exactly one bounded Heartbeat call slot.
#[must_use = "a reserved Heartbeat call slot must be submitted or released"]
pub(crate) struct ClassicHeartbeatCallPermit<'a> {
    key: ClassicHeartbeatCallKey,
    group: &'a str,
    calls: &'a mut Vec<TrackedClassicHeartbeatCall>,
}

impl ClassicHeartbeatCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        request: PreparedClassicHeartbeatRequest,
    ) -> Result<AcceptedClassicHeartbeatCall, ClassicHeartbeatAdmissionFailure> {
        let call = driver
            .submit_tracked_classic_heartbeat(
                self.group,
                request.into_generated_heartbeat_request(),
                self.key.deadline(),
            )
            .map_err(|source| ClassicHeartbeatAdmissionFailure::new(self.key, source))?;
        self.calls.push(TrackedClassicHeartbeatCall {
            key: self.key,
            call,
        });
        Ok(AcceptedClassicHeartbeatCall::new(self.key))
    }
}

/// Why a Heartbeat call slot could not be reserved without moving its request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatCallReservationError {
    Capacity { limit: usize },
    Duplicate { key: ClassicHeartbeatCallKey },
}

/// Capacity-bounded registry of every retained Heartbeat call state.
pub(crate) struct TrackedClassicHeartbeatCalls {
    capacity: usize,
    pub(super) calls: Vec<TrackedClassicHeartbeatCall>,
    pub(super) settled: Option<SettledClassicHeartbeatCall>,
    pub(super) pending_confirmation: Option<PendingClassicHeartbeatConfirmation>,
    pub(super) completion_failure: Option<ClassicHeartbeatCompletionFailure>,
}

impl TrackedClassicHeartbeatCalls {
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::try_new(capacity)
            .unwrap_or_else(|_error| panic!("test Heartbeat call reservation failed"))
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

    pub(crate) fn try_reserve_classic_heartbeat<'a>(
        &'a mut self,
        key: ClassicHeartbeatCallKey,
        group: &'a str,
    ) -> Result<ClassicHeartbeatCallPermit<'a>, ClassicHeartbeatCallReservationError> {
        if self.contains_attempt(key) {
            return Err(ClassicHeartbeatCallReservationError::Duplicate { key });
        }
        if self.retained_classic_heartbeat_count() >= self.capacity {
            return Err(ClassicHeartbeatCallReservationError::Capacity {
                limit: self.capacity,
            });
        }
        Ok(ClassicHeartbeatCallPermit {
            key,
            group,
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_classic_heartbeat_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_confirmation.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    fn contains_attempt(&self, key: ClassicHeartbeatCallKey) -> bool {
        self.calls
            .iter()
            .any(|call| call.key.same_attempt_identity(key))
            || self
                .settled
                .as_ref()
                .is_some_and(|call| call.key().same_attempt_identity(key))
            || self
                .pending_confirmation
                .as_ref()
                .is_some_and(|pending| pending.key().same_attempt_identity(key))
            || self
                .completion_failure
                .as_ref()
                .is_some_and(|failure| failure.key().same_attempt_identity(key))
    }

    pub(crate) fn poll_classic_heartbeat(
        &mut self,
    ) -> Result<ClassicHeartbeatPoll, ClassicHeartbeatCompletionObservation> {
        if let Some(failure) = &self.completion_failure {
            return Err(failure.observation());
        }
        if let Some(pending) = &self.pending_confirmation {
            return Ok(ClassicHeartbeatPoll::ConfirmationPending { key: pending.key() });
        }
        if let Some(settled) = &self.settled {
            return Ok(ClassicHeartbeatPoll::TerminalReady { key: settled.key() });
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(ClassicHeartbeatPoll::Idle);
        };
        let tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let failure = ClassicHeartbeatCompletionFailure::new(tracked.key, source);
                let observation = failure.observation();
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        };
        let (result, selected_version, route_token) = outcome.into_parts();
        let terminal = retain_classic_heartbeat_terminal(tracked.key, selected_version, result);
        self.settled = Some(SettledClassicHeartbeatCall::new(terminal, route_token));
        Ok(ClassicHeartbeatPoll::TerminalReady { key: tracked.key })
    }

    #[cfg(test)]
    pub(crate) fn install_terminal_for_test(
        &mut self,
        key: ClassicHeartbeatCallKey,
        selected_version: Option<i16>,
        result: Result<HeartbeatResponse, kafka_driver::RequestError>,
    ) {
        let selected_version = selected_version.map(kafka_driver::ApiVersion::new);
        let terminal = retain_classic_heartbeat_terminal(key, selected_version, result);
        self.settled = Some(SettledClassicHeartbeatCall::new(terminal, None));
    }

    #[cfg(test)]
    pub(crate) fn install_completion_failure_for_test(
        &mut self,
        key: ClassicHeartbeatCallKey,
        source: kafka_driver::CompletionError,
    ) {
        self.completion_failure = Some(ClassicHeartbeatCompletionFailure::new(key, source));
    }
}
