//! Borrowed `LeaveGroup` facts with exact plan and linear route evidence.

use kafka_client_core::{DeliveryStatus, RemoveConsumerGroupMembersPlan};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::LeaveGroupResponse;

use super::super::request_failure_delivery;

/// Stable engine-local classification independent of driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveConsumerGroupMembersDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for the concrete host interpreter.
pub(crate) enum RemoveConsumerGroupMembersTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a LeaveGroupResponse,
    },
    Failed {
        kind: RemoveConsumerGroupMembersDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until protocol validation and core settlement.
#[must_use = "a raw member-removal terminal owns unsettled route evidence"]
pub(crate) struct RemoveConsumerGroupMembersTerminal {
    plan: RemoveConsumerGroupMembersPlan,
    request_scratch_limit: usize,
    result_limit: usize,
    selected_version: Option<i16>,
    result: Result<LeaveGroupResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl RemoveConsumerGroupMembersTerminal {
    pub(crate) fn fact(&self) -> RemoveConsumerGroupMembersTerminalFact<'_> {
        match &self.result {
            Ok(response) => RemoveConsumerGroupMembersTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => RemoveConsumerGroupMembersTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn matches(
        &self,
        expected_plan: &RemoveConsumerGroupMembersPlan,
        expected_request_scratch_limit: usize,
        expected_result_limit: usize,
    ) -> bool {
        self.plan == *expected_plan
            && self.request_scratch_limit == expected_request_scratch_limit
            && self.result_limit == expected_result_limit
    }

    /// Deliberately releases route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            plan,
            result,
            route_token,
            ..
        } = self;
        drop(plan);
        drop(result);
        drop(route_token);
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        plan: RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        retain_remove_consumer_group_members_terminal(
            plan,
            request_scratch_limit,
            result_limit,
            Some(ApiVersion::new(5)),
            Ok(LeaveGroupResponse::default()),
            None,
        )
    }
}

pub(super) fn retain_remove_consumer_group_members_terminal(
    plan: RemoveConsumerGroupMembersPlan,
    request_scratch_limit: usize,
    result_limit: usize,
    selected_version: Option<ApiVersion>,
    result: Result<LeaveGroupResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> RemoveConsumerGroupMembersTerminal {
    RemoveConsumerGroupMembersTerminal {
        plan,
        request_scratch_limit,
        result_limit,
        selected_version: selected_version.map(ApiVersion::value),
        result,
        route_token,
    }
}

fn failure_kind(error: &RequestError) -> RemoveConsumerGroupMembersDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => RemoveConsumerGroupMembersDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => RemoveConsumerGroupMembersDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            RemoveConsumerGroupMembersDriverFailureKind::Compatibility
        }
        _ => RemoveConsumerGroupMembersDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered member-removal ownership still requires core settlement"]
pub(crate) struct RecoveredRemoveConsumerGroupMembersCall {
    plan: RemoveConsumerGroupMembersPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl RecoveredRemoveConsumerGroupMembersCall {
    pub(super) const fn new(
        plan: RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(crate) fn matches(
        &self,
        expected_plan: &RemoveConsumerGroupMembersPlan,
        expected_request_scratch_limit: usize,
        expected_result_limit: usize,
    ) -> bool {
        self.plan == *expected_plan
            && self.request_scratch_limit == expected_request_scratch_limit
            && self.result_limit == expected_result_limit
    }

    /// Consumes recovered ownership after core receives the terminal fact.
    pub(crate) fn seal(self) {
        let Self { plan, .. } = self;
        drop(plan);
    }
}
