//! Driver-owned call and route facts for one classic-group position reset.

use std::time::Instant;

use kafka_client_core::{PartitionIndex, PositionResolutionAttemptFailure};
use kafka_driver::{
    Call, CompletionError, PartitionId, RouteFailureToken, RoutedCall, RoutedOutcome, TopicName,
    TopicView, TopicViewError,
};
use kafka_wire::{ListOffsetsRequest, ListOffsetsResponse};

use crate::protocol::consumer::ListOffsetsIsolation;

use super::super::{
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
    outcome: Result<RoutedOutcome<ListOffsetsResponse>, PositionResolutionAttemptFailure>,
}

impl ClassicGroupPositionResetOutcome {
    pub(crate) fn into_resolution(
        self,
        topic: &str,
        partition: PartitionIndex,
        isolation: ListOffsetsIsolation,
    ) -> (ListOffsetsResolution, ClassicGroupPositionResetRoute) {
        let outcome = match self.outcome {
            Ok(outcome) => outcome,
            Err(failure) => {
                return (
                    ListOffsetsResolution::Failed(failure),
                    ClassicGroupPositionResetRoute { _token: None },
                );
            }
        };
        let (result, version, route_token) = outcome.into_parts();
        (
            normalize_list_offsets_terminal(topic, partition, isolation, version, result),
            ClassicGroupPositionResetRoute {
                _token: route_token,
            },
        )
    }
}

/// Linear ownership of fresh routing and one reset lookup under the same deadline.
pub(crate) struct ClassicGroupPositionResetCall {
    topic: TopicName,
    partition: i32,
    request: Option<ListOffsetsRequest>,
    deadline: Instant,
    state: ResetCallState,
}

enum ResetCallState {
    Observe(Call<Result<TopicView, TopicViewError>>),
    Refresh(Call<Result<TopicView, TopicViewError>>),
    Request(RoutedCall<ListOffsetsResponse>),
    Complete,
}

impl ClassicGroupPositionResetCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        topic: &str,
        partition: i32,
        request: ListOffsetsRequest,
        deadline: Instant,
    ) -> Result<Self, ListOffsetsSubmitError> {
        let topic =
            TopicName::new(topic.to_owned()).map_err(ListOffsetsSubmitError::InvalidTopic)?;
        PartitionId::new(partition).map_err(ListOffsetsSubmitError::InvalidPartition)?;
        let call = driver
            .driver
            .topic_view(topic.clone(), deadline)
            .map_err(ListOffsetsSubmitError::Driver)?;
        Ok(Self {
            topic,
            partition,
            request: Some(request),
            deadline,
            state: ResetCallState::Observe(call),
        })
    }

    pub(crate) fn try_result(
        &mut self,
        driver: &DriverOwner,
    ) -> Option<Result<ClassicGroupPositionResetOutcome, ClassicGroupPositionResetCompletionError>>
    {
        let refresh = matches!(self.state, ResetCallState::Refresh(_));
        let result = match &self.state {
            ResetCallState::Request(call) => {
                let result = call.try_result()?;
                self.state = ResetCallState::Complete;
                return Some(
                    result
                        .map(|outcome| ClassicGroupPositionResetOutcome {
                            outcome: Ok(outcome),
                        })
                        .map_err(completion_error),
                );
            }
            ResetCallState::Observe(call) | ResetCallState::Refresh(call) => call.try_result()?,
            ResetCallState::Complete => return None,
        };
        self.state = ResetCallState::Complete;
        let view = match result {
            Ok(Ok(view)) if view.topic() == &self.topic => view,
            Ok(Ok(_)) => {
                return Some(Ok(failed(
                    PositionResolutionAttemptFailure::InvalidResponse,
                )));
            }
            Ok(Err(error)) => return Some(Ok(failed(topic_view_failure(error)))),
            Err(error) => return Some(Err(completion_error(error))),
        };
        let has_leader = (0..view.available_len()).any(|index| {
            view.available_at(index)
                .is_some_and(|entry| entry.partition().get() == self.partition)
        });
        // Rejoining can retain a cached leader that is now offline. Observe its
        // generation, then obtain a newer view before admitting ListOffsets.
        // A leaderless election result also requires a newer view, never a new deadline.
        if !refresh || !has_leader {
            match driver.driver.topic_view_newer_than(
                self.topic.clone(),
                view.generation(),
                self.deadline,
            ) {
                Ok(call) => self.state = ResetCallState::Refresh(call),
                Err(_) => {
                    return Some(Ok(failed(PositionResolutionAttemptFailure::DriverRejected)));
                }
            }
            return None;
        }
        let request = self.request.take()?;
        match driver.submit_tracked_list_offsets(
            self.topic.as_str(),
            self.partition,
            request,
            self.deadline,
        ) {
            Ok(call) => self.state = ResetCallState::Request(call),
            Err(_) => return Some(Ok(failed(PositionResolutionAttemptFailure::DriverRejected))),
        }
        None
    }
}

const fn failed(failure: PositionResolutionAttemptFailure) -> ClassicGroupPositionResetOutcome {
    ClassicGroupPositionResetOutcome {
        outcome: Err(failure),
    }
}

const fn completion_error(source: CompletionError) -> ClassicGroupPositionResetCompletionError {
    match source {
        CompletionError::Closed => ClassicGroupPositionResetCompletionError::Closed,
        CompletionError::Consumed => ClassicGroupPositionResetCompletionError::Consumed,
        _ => ClassicGroupPositionResetCompletionError::Unknown,
    }
}

#[allow(
    unreachable_patterns,
    reason = "published driver errors are non-exhaustive"
)]
fn topic_view_failure(error: TopicViewError) -> PositionResolutionAttemptFailure {
    match error {
        TopicViewError::DeadlineExceeded => PositionResolutionAttemptFailure::DeadlineElapsed,
        TopicViewError::Unavailable | TopicViewError::RefreshFailed => {
            PositionResolutionAttemptFailure::Transport
        }
        TopicViewError::Broker { error_code } => core::num::NonZeroI16::new(error_code).map_or(
            PositionResolutionAttemptFailure::InvalidResponse,
            PositionResolutionAttemptFailure::Broker,
        ),
        TopicViewError::MalformedMetadata => PositionResolutionAttemptFailure::InvalidResponse,
        _ => PositionResolutionAttemptFailure::DriverRejected,
    }
}
