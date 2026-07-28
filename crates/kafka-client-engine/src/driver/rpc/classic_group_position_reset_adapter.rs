//! Driver-owned call and route facts for one classic-group position reset.

use std::time::Instant;

use kafka_client_core::PartitionIndex;
use kafka_driver::{CompletionError, RouteFailureToken, RoutedCall, RoutedOutcome};
use kafka_wire::{ListOffsetsRequest, ListOffsetsResponse};

use crate::protocol::consumer::ListOffsetsIsolation;

use super::{
    super::DriverOwner,
    list_offsets_submission::ListOffsetsSubmitError,
    list_offsets_terminal::{ListOffsetsResolution, normalize_list_offsets_terminal},
};

/// Exact completion-cell failure retained by a reset owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicGroupPositionResetCompletionError {
    Closed,
    Consumed,
    Unknown,
}

/// Linear route capability retained after reset response normalization.
pub(crate) struct ClassicGroupPositionResetRoute {
    _token: Option<RouteFailureToken>,
}

/// Exact generated terminal retained until the reset owner applies core policy.
pub(crate) struct ClassicGroupPositionResetOutcome {
    outcome: RoutedOutcome<ListOffsetsResponse>,
}

impl ClassicGroupPositionResetOutcome {
    pub(crate) fn into_resolution(
        self,
        topic: &str,
        partition: PartitionIndex,
        isolation: ListOffsetsIsolation,
    ) -> (ListOffsetsResolution, ClassicGroupPositionResetRoute) {
        let (result, version, route_token) = self.outcome.into_parts();
        (
            normalize_list_offsets_terminal(topic, partition, isolation, version, result),
            ClassicGroupPositionResetRoute {
                _token: route_token,
            },
        )
    }
}

/// Linear driver ownership of one accepted reset lookup.
pub(crate) struct ClassicGroupPositionResetCall {
    call: RoutedCall<ListOffsetsResponse>,
}

impl ClassicGroupPositionResetCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        topic: &str,
        partition: i32,
        request: ListOffsetsRequest,
        deadline: Instant,
    ) -> Result<Self, ListOffsetsSubmitError> {
        driver
            .submit_tracked_list_offsets(topic, partition, request, deadline)
            .map(|call| Self { call })
    }

    pub(crate) fn try_result(
        &self,
    ) -> Option<Result<ClassicGroupPositionResetOutcome, ClassicGroupPositionResetCompletionError>>
    {
        self.call.try_result().map(|result| match result {
            Ok(outcome) => Ok(ClassicGroupPositionResetOutcome { outcome }),
            Err(source) => Err(match source {
                CompletionError::Closed => ClassicGroupPositionResetCompletionError::Closed,
                CompletionError::Consumed => ClassicGroupPositionResetCompletionError::Consumed,
                _ => ClassicGroupPositionResetCompletionError::Unknown,
            }),
        })
    }
}
