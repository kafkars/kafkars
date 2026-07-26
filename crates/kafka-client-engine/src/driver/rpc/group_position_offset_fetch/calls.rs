//! Bounded active-call ownership, duplicate fencing, and nonblocking polling.

use kafka_client_core::GroupPositionFence;
use kafka_driver::RoutedCall;
use kafka_wire::OffsetFetchResponse;

use crate::{driver::DriverOwner, protocol::consumer::PreparedGroupOffsetFetchRequest};

use super::{
    admission::{
        GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchAdmission,
        GroupPositionOffsetFetchAdmissionFailure, GroupPositionOffsetFetchReturn,
        GroupPositionOffsetFetchReturnReason,
    },
    key::GroupPositionOffsetFetchKey,
    recovery::{
        GroupPositionOffsetFetchCompletionFailure, GroupPositionOffsetFetchCompletionObservation,
    },
    settlement::{
        GroupPositionOffsetFetchPoll, PendingGroupPositionOffsetFetchConfirmation,
        SettledGroupPositionOffsetFetchCall,
    },
    terminal::retain_group_position_offset_fetch_terminal,
};

pub(super) struct TrackedGroupPositionOffsetFetchCall {
    key: GroupPositionOffsetFetchKey,
    call: RoutedCall<OffsetFetchResponse>,
}

impl TrackedGroupPositionOffsetFetchCall {
    pub(super) fn recover_after_driver_shutdown(self) -> GroupPositionOffsetFetchKey {
        drop(self.call);
        self.key
    }
}

/// Capacity-bounded registry of active, settled, confirming, and corrupted calls.
pub(crate) struct TrackedGroupPositionOffsetFetchCalls {
    capacity: usize,
    pub(super) calls: Vec<TrackedGroupPositionOffsetFetchCall>,
    pub(super) settled: Option<SettledGroupPositionOffsetFetchCall>,
    pub(super) pending_confirmation: Option<PendingGroupPositionOffsetFetchConfirmation>,
    pub(super) completion_failure: Option<GroupPositionOffsetFetchCompletionFailure>,
}

impl TrackedGroupPositionOffsetFetchCalls {
    #[cfg(test)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self::try_new(capacity)
            .unwrap_or_else(|_error| panic!("test group position call reservation failed"))
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

    pub(crate) fn try_submit(
        &mut self,
        driver: &DriverOwner,
        key: GroupPositionOffsetFetchKey,
        group: &str,
        request: PreparedGroupOffsetFetchRequest,
    ) -> GroupPositionOffsetFetchAdmission {
        if self.contains_fence(key.fence()) {
            return GroupPositionOffsetFetchAdmission::Returned(
                GroupPositionOffsetFetchReturn::new(
                    key,
                    request,
                    GroupPositionOffsetFetchReturnReason::DuplicateFence,
                ),
            );
        }
        if self.retained_group_position_offset_fetch_count() >= self.capacity {
            return GroupPositionOffsetFetchAdmission::Returned(
                GroupPositionOffsetFetchReturn::new(
                    key,
                    request,
                    GroupPositionOffsetFetchReturnReason::Capacity {
                        limit: self.capacity,
                    },
                ),
            );
        }
        let fence = key.fence();
        let call = match driver.submit_tracked_group_position_offset_fetch(
            group,
            request.into_wire_request(),
            key.operation_deadline().transport(),
        ) {
            Ok(call) => call,
            Err(source) => {
                return GroupPositionOffsetFetchAdmission::Rejected(
                    GroupPositionOffsetFetchAdmissionFailure::new(key, source),
                );
            }
        };
        self.calls
            .push(TrackedGroupPositionOffsetFetchCall { key, call });
        GroupPositionOffsetFetchAdmission::Accepted(GroupPositionOffsetFetchAccepted::new(fence))
    }

    pub(crate) fn retained_group_position_offset_fetch_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_confirmation.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    pub(crate) fn poll_group_position_offset_fetch(
        &mut self,
    ) -> Result<GroupPositionOffsetFetchPoll, GroupPositionOffsetFetchCompletionObservation> {
        if let Some(failure) = &self.completion_failure {
            return Err(failure.observation());
        }
        if let Some(pending) = &self.pending_confirmation {
            return Ok(GroupPositionOffsetFetchPoll::ConfirmationPending {
                fence: pending.fence(),
            });
        }
        if let Some(settled) = &self.settled {
            return Ok(GroupPositionOffsetFetchPoll::TerminalReady {
                fence: settled.fence(),
            });
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(GroupPositionOffsetFetchPoll::Idle);
        };
        let tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let failure = GroupPositionOffsetFetchCompletionFailure::new(tracked.key, source);
                let observation = failure.observation();
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        };
        let fence = tracked.key.fence();
        let (result, selected_version, route_token) = outcome.into_parts();
        let terminal =
            retain_group_position_offset_fetch_terminal(tracked.key, selected_version, result);
        self.settled = Some(SettledGroupPositionOffsetFetchCall::new(
            terminal,
            route_token,
        ));
        Ok(GroupPositionOffsetFetchPoll::TerminalReady { fence })
    }

    fn contains_fence(&self, fence: GroupPositionFence) -> bool {
        self.calls.iter().any(|call| call.key.fence() == fence)
            || self
                .settled
                .as_ref()
                .is_some_and(|settled| settled.fence() == fence)
            || self
                .pending_confirmation
                .as_ref()
                .is_some_and(|pending| pending.fence() == fence)
            || self
                .completion_failure
                .as_ref()
                .is_some_and(|failure| failure.fence() == fence)
    }

    #[cfg(test)]
    pub(super) fn install_terminal_for_test(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        selected_version: Option<i16>,
        result: Result<OffsetFetchResponse, kafka_driver::RequestError>,
    ) {
        let selected_version = selected_version.map(kafka_driver::ApiVersion::new);
        let terminal = retain_group_position_offset_fetch_terminal(key, selected_version, result);
        self.settled = Some(SettledGroupPositionOffsetFetchCall::new(terminal, None));
    }

    #[cfg(test)]
    pub(super) fn install_completion_failure_for_test(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        source: kafka_driver::CompletionError,
    ) {
        self.completion_failure = Some(GroupPositionOffsetFetchCompletionFailure::new(key, source));
    }
}
