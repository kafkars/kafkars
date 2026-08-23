//! Driver-owned tracked call and route fact for `ShareGroupHeartbeat` v1.
#![allow(
    dead_code,
    reason = "closed tracked adapter checkpoint precedes its hosted membership owner"
)]

use kafka_driver::{CompletionError, RouteFailureToken, RouteKind, RoutedCall, RoutedOutcome};
use kafka_wire::ShareGroupHeartbeatResponse;

use crate::{
    clock::OperationDeadline,
    protocol::consumer::share_group::{
        PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatOutcome,
        ShareGroupHeartbeatRequestFailure, ShareGroupHeartbeatSuccess,
        normalize_share_group_heartbeat_response, share_group_join_request,
        share_group_leave_request, share_group_steady_request,
    },
};

use super::invalidation::PendingShareCoordinatorInvalidation;
use super::{
    super::{
        super::DriverOwner,
        consumer_group_heartbeat_failure::{
            ConsumerGroupHeartbeatDriverFailureKind,
            classify_consumer_group_heartbeat_request_error,
        },
    },
    submission::ShareGroupHeartbeatSubmitError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareGroupHeartbeatCompletionError {
    Closed,
    Consumed,
    Unknown,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ShareGroupHeartbeatResolution {
    Succeeded(ShareGroupHeartbeatSuccess),
    BrokerRejected {
        error_code: i16,
        throttle_time_ms: u32,
    },
    Failed(ConsumerGroupHeartbeatDriverFailureKind),
}

/// Exact share-coordinator route evidence retained through interpretation.
pub(crate) struct ShareGroupHeartbeatRoute {
    token: Option<RouteFailureToken>,
}

impl ShareGroupHeartbeatRoute {
    pub(crate) fn into_invalidation(
        self,
        group_id: kafka_client_core::GroupId,
    ) -> Result<PendingShareCoordinatorInvalidation, Self> {
        self.into_coordinator_token()
            .map(|token| PendingShareCoordinatorInvalidation::new(group_id, token))
    }

    pub(crate) fn into_coordinator_token(self) -> Result<RouteFailureToken, Self> {
        if self.token.as_ref().map(RouteFailureToken::kind) != Some(RouteKind::Coordinator) {
            return Err(self);
        }
        let Self { token } = self;
        Ok(token.unwrap_or_else(|| unreachable!("coordinator route retains its token")))
    }

    pub(crate) fn accept(self) {
        drop(self);
    }
}

pub(crate) struct ShareGroupHeartbeatCallOutcome {
    outcome: RoutedOutcome<ShareGroupHeartbeatResponse>,
}

impl ShareGroupHeartbeatCallOutcome {
    pub(crate) fn into_resolution(
        self,
    ) -> (ShareGroupHeartbeatResolution, ShareGroupHeartbeatRoute) {
        let (result, selected_version, route_token) = self.outcome.into_parts();
        let route = ShareGroupHeartbeatRoute { token: route_token };
        let resolution = match result {
            Ok(response) => selected_version.map_or(
                ShareGroupHeartbeatResolution::Failed(
                    ConsumerGroupHeartbeatDriverFailureKind::Compatibility,
                ),
                |version| normalize_terminal(version.value(), &response),
            ),
            Err(error) => ShareGroupHeartbeatResolution::Failed(
                classify_consumer_group_heartbeat_request_error(&error),
            ),
        };
        (resolution, route)
    }
}

/// Linear driver ownership of one accepted `ShareGroupHeartbeat`.
pub(crate) struct ShareGroupHeartbeatCall {
    call: RoutedCall<ShareGroupHeartbeatResponse>,
}

impl ShareGroupHeartbeatCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        group: &str,
        request: PreparedShareGroupHeartbeatRequest,
        deadline: OperationDeadline,
    ) -> Result<Self, ShareGroupHeartbeatSubmitError> {
        driver
            .submit_tracked_share_group_heartbeat(group, request.into_generated_request(), deadline)
            .map(|call| Self { call })
    }

    pub(crate) fn try_result(
        &self,
    ) -> Option<Result<ShareGroupHeartbeatCallOutcome, ShareGroupHeartbeatCompletionError>> {
        self.call.try_result().map(|result| match result {
            Ok(outcome) => Ok(ShareGroupHeartbeatCallOutcome { outcome }),
            Err(source) => Err(match source {
                CompletionError::Closed => ShareGroupHeartbeatCompletionError::Closed,
                CompletionError::Consumed => ShareGroupHeartbeatCompletionError::Consumed,
                _ => ShareGroupHeartbeatCompletionError::Unknown,
            }),
        })
    }

    pub(crate) fn join_request(
        group: &str,
        member: &str,
        rack: Option<&str>,
        topics: &[&str],
    ) -> Result<PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure> {
        share_group_join_request(group, member, rack, topics)
    }

    pub(crate) fn steady_request(
        group: &str,
        member: &str,
        member_epoch: i32,
    ) -> Result<PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure> {
        share_group_steady_request(group, member, member_epoch)
    }

    pub(crate) fn leave_request(
        group: &str,
        member: &str,
    ) -> Result<PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure> {
        share_group_leave_request(group, member)
    }

    pub(crate) fn discard_after_driver_shutdown(self) {
        drop(self);
    }
}

fn normalize_terminal(
    selected_version: i16,
    response: &ShareGroupHeartbeatResponse,
) -> ShareGroupHeartbeatResolution {
    match normalize_share_group_heartbeat_response(selected_version, response) {
        Ok(ShareGroupHeartbeatOutcome::Succeeded(success)) => {
            ShareGroupHeartbeatResolution::Succeeded(success)
        }
        Ok(ShareGroupHeartbeatOutcome::Rejected(rejection)) => {
            ShareGroupHeartbeatResolution::BrokerRejected {
                error_code: rejection.error_code().get(),
                throttle_time_ms: rejection.throttle_time_ms(),
            }
        }
        Err(_failure) => ShareGroupHeartbeatResolution::Failed(
            ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse,
        ),
    }
}
