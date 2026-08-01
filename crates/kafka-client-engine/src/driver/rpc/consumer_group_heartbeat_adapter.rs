//! Driver-owned call, routed terminal, and failure facts for KIP-848 heartbeats.

use kafka_client_core::GroupId;
use kafka_driver::{CompletionError, RouteFailureToken, RouteKind, RoutedCall, RoutedOutcome};
use kafka_wire::ConsumerGroupHeartbeatResponse;

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatOwnedTopic,
        ConsumerGroupHeartbeatRequestFailure, ConsumerGroupHeartbeatSuccess,
        PreparedConsumerGroupHeartbeatRequest, consumer_group_join_request,
        consumer_group_leave_request, consumer_group_steady_request,
        normalize_consumer_group_heartbeat_response,
    },
};

use super::{
    super::DriverOwner,
    classic_group::PendingClassicCoordinatorInvalidation,
    consumer_group_heartbeat_failure::{
        ConsumerGroupHeartbeatDriverFailureKind, classify_consumer_group_heartbeat_request_error,
    },
    consumer_group_heartbeat_submission::ConsumerGroupHeartbeatSubmitError,
};

/// Exact completion-cell failure retained by one KIP-848 membership owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatCompletionError {
    Closed,
    Consumed,
    Unknown,
}

/// Exact generated-free terminal ready for KIP-848 policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatResolution {
    Succeeded(ConsumerGroupHeartbeatSuccess),
    BrokerRejected {
        error_code: i16,
        throttle_time_ms: u32,
    },
    Failed(ConsumerGroupHeartbeatDriverFailureKind),
}

/// Linear coordinator-route authority retained through terminal interpretation.
pub(crate) struct ConsumerGroupHeartbeatRoute {
    token: Option<RouteFailureToken>,
}

impl ConsumerGroupHeartbeatRoute {
    /// Transfers exact coordinator-route authority into bounded invalidation ownership.
    #[expect(
        clippy::result_large_err,
        reason = "a rejected transfer must return the exact linear route authority intact"
    )]
    pub(crate) fn into_coordinator_invalidation(
        self,
        group_id: GroupId,
    ) -> Result<PendingClassicCoordinatorInvalidation, Self> {
        if self.token.as_ref().map(RouteFailureToken::kind) != Some(RouteKind::Coordinator) {
            return Err(self);
        }
        let Self { token } = self;
        let Some(token) = token else {
            unreachable!("coordinator route kind requires a retained token")
        };
        Ok(PendingClassicCoordinatorInvalidation::new(group_id, token))
    }

    /// Explicitly accepts the observed route without requesting invalidation.
    pub(crate) fn accept(self) {
        drop(self);
    }
}

/// Exact routed terminal retained until membership policy observes it.
pub(crate) struct ConsumerGroupHeartbeatCallOutcome {
    outcome: RoutedOutcome<ConsumerGroupHeartbeatResponse>,
}

impl ConsumerGroupHeartbeatCallOutcome {
    /// Normalizes the generated response while retaining its route authority.
    pub(crate) fn into_resolution(
        self,
    ) -> (
        ConsumerGroupHeartbeatResolution,
        ConsumerGroupHeartbeatRoute,
    ) {
        let (result, selected_version, route_token) = self.outcome.into_parts();
        let route = ConsumerGroupHeartbeatRoute { token: route_token };
        let resolution = match result {
            Ok(response) => match selected_version {
                Some(version) => normalize_terminal(version.value(), &response),
                None => ConsumerGroupHeartbeatResolution::Failed(
                    ConsumerGroupHeartbeatDriverFailureKind::Compatibility,
                ),
            },
            Err(error) => ConsumerGroupHeartbeatResolution::Failed(
                classify_consumer_group_heartbeat_request_error(&error),
            ),
        };
        (resolution, route)
    }
}

/// Linear driver ownership of one accepted KIP-848 heartbeat.
pub(crate) struct ConsumerGroupHeartbeatCall {
    call: RoutedCall<ConsumerGroupHeartbeatResponse>,
}

impl ConsumerGroupHeartbeatCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        group: &str,
        request: PreparedConsumerGroupHeartbeatRequest,
        deadline: OperationDeadline,
    ) -> Result<Self, ConsumerGroupHeartbeatSubmitError> {
        driver
            .submit_tracked_consumer_group_heartbeat(
                group,
                request.into_generated_request(),
                deadline,
            )
            .map(|call| Self { call })
    }

    pub(crate) fn try_result(
        &self,
    ) -> Option<Result<ConsumerGroupHeartbeatCallOutcome, ConsumerGroupHeartbeatCompletionError>>
    {
        self.call.try_result().map(|result| match result {
            Ok(outcome) => Ok(ConsumerGroupHeartbeatCallOutcome { outcome }),
            Err(source) => Err(match source {
                CompletionError::Closed => ConsumerGroupHeartbeatCompletionError::Closed,
                CompletionError::Consumed => ConsumerGroupHeartbeatCompletionError::Consumed,
                _ => ConsumerGroupHeartbeatCompletionError::Unknown,
            }),
        })
    }

    pub(crate) fn join_request(
        group: &str,
        member: Option<&str>,
        instance_id: Option<&str>,
        rebalance_timeout_ms: u32,
        topics: &[&str],
    ) -> Result<PreparedConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestFailure> {
        consumer_group_join_request(group, member, instance_id, rebalance_timeout_ms, topics)
    }

    pub(crate) fn steady_request(
        group: &str,
        member: &str,
        member_epoch: i32,
        owned_topics: Option<&[ConsumerGroupHeartbeatOwnedTopic]>,
    ) -> Result<PreparedConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestFailure> {
        consumer_group_steady_request(group, member, member_epoch, owned_topics)
    }

    pub(crate) fn leave_request(
        group: &str,
        member: &str,
    ) -> Result<PreparedConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestFailure> {
        consumer_group_leave_request(group, member)
    }

    pub(crate) fn discard_after_driver_shutdown(self) {
        drop(self);
    }
}

fn normalize_terminal(
    selected_version: i16,
    response: &ConsumerGroupHeartbeatResponse,
) -> ConsumerGroupHeartbeatResolution {
    match normalize_consumer_group_heartbeat_response(selected_version, response) {
        Ok(ConsumerGroupHeartbeatOutcome::Succeeded(success)) => {
            ConsumerGroupHeartbeatResolution::Succeeded(success)
        }
        Ok(ConsumerGroupHeartbeatOutcome::Rejected(rejection)) => {
            ConsumerGroupHeartbeatResolution::BrokerRejected {
                error_code: rejection.error_code().get(),
                throttle_time_ms: rejection.throttle_time_ms(),
            }
        }
        Err(_failure) => ConsumerGroupHeartbeatResolution::Failed(
            ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse,
        ),
    }
}
