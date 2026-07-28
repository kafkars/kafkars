//! Driver-owned call, terminal, route, and failure facts for classic-group leave.

use kafka_client_core::GroupId;
use kafka_driver::{
    CompletionError, Delivery, RouteFailureToken, RouteKind, RoutedCall, RoutedOutcome,
};
use kafka_wire::LeaveGroupResponse;

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        ClassicLeaveGroupOutcome, PreparedClassicLeaveGroupRequest,
        normalize_classic_leave_group_response,
    },
};

use super::{
    super::DriverOwner,
    classic_group::PendingClassicCoordinatorInvalidation,
    classic_group_leave_failure::{ClassicGroupLeaveDriverFailureKind, classify_request_error},
    leave_group_submission::LeaveGroupSubmitError,
};

/// Exact completion-cell failure retained by a classic-group leave owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicGroupLeaveCompletionError {
    Closed,
    Consumed,
    Unknown,
}

/// Driver-neutral result of one exact classic-group leave terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicGroupLeaveResolution {
    Succeeded,
    BrokerRejected(i16),
    Failed {
        kind: ClassicGroupLeaveDriverFailureKind,
        definitely_not_sent: bool,
    },
}

/// Linear coordinator-route authority retained through leave policy.
#[derive(Debug)]
pub(crate) struct ClassicGroupLeaveRoute {
    token: Option<RouteFailureToken>,
}

impl ClassicGroupLeaveRoute {
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

    pub(crate) fn accept(self) {
        drop(self);
    }
}

/// Exact routed terminal retained until close policy observes its deadline.
pub(crate) struct ClassicGroupLeaveOutcome {
    outcome: RoutedOutcome<LeaveGroupResponse>,
}

impl ClassicGroupLeaveOutcome {
    /// Normalizes the generated response while preserving its route capability.
    pub(crate) fn into_resolution(self) -> (ClassicGroupLeaveResolution, ClassicGroupLeaveRoute) {
        let (result, selected_version, route_token) = self.outcome.into_parts();
        let route = ClassicGroupLeaveRoute { token: route_token };
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                return (
                    ClassicGroupLeaveResolution::Failed {
                        kind: classify_request_error(&error),
                        definitely_not_sent: error.delivery() == Delivery::NotSent,
                    },
                    route,
                );
            }
        };
        let Some(version) = selected_version else {
            return (
                ClassicGroupLeaveResolution::Failed {
                    kind: ClassicGroupLeaveDriverFailureKind::Compatibility,
                    definitely_not_sent: false,
                },
                route,
            );
        };
        let resolution = match normalize_classic_leave_group_response(version.value(), &response) {
            Ok(ClassicLeaveGroupOutcome::Succeeded { .. }) => {
                ClassicGroupLeaveResolution::Succeeded
            }
            Ok(ClassicLeaveGroupOutcome::Rejected { error_code, .. }) => {
                ClassicGroupLeaveResolution::BrokerRejected(error_code.get())
            }
            Err(_error) => ClassicGroupLeaveResolution::Failed {
                kind: ClassicGroupLeaveDriverFailureKind::InvalidResponse,
                definitely_not_sent: false,
            },
        };
        (resolution, route)
    }
}

/// Linear driver ownership of one accepted classic-group leave.
pub(crate) struct ClassicGroupLeaveCall {
    call: RoutedCall<LeaveGroupResponse>,
}

impl ClassicGroupLeaveCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        group: &str,
        request: PreparedClassicLeaveGroupRequest,
        deadline: OperationDeadline,
    ) -> Result<Self, LeaveGroupSubmitError> {
        driver
            .submit_tracked_leave_group(
                group,
                request.into_generated_leave_group_request(),
                deadline,
            )
            .map(|call| Self { call })
    }

    pub(crate) fn try_result(
        &self,
    ) -> Option<Result<ClassicGroupLeaveOutcome, ClassicGroupLeaveCompletionError>> {
        self.call.try_result().map(|result| match result {
            Ok(outcome) => Ok(ClassicGroupLeaveOutcome { outcome }),
            Err(source) => Err(match source {
                CompletionError::Closed => ClassicGroupLeaveCompletionError::Closed,
                CompletionError::Consumed => ClassicGroupLeaveCompletionError::Consumed,
                _ => ClassicGroupLeaveCompletionError::Unknown,
            }),
        })
    }
}
