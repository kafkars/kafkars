//! Exhaustive translation from retained core facts into stable named events.

use kafka_client_core::{
    FetchFailure, FetchFence, FetchThrottleFailure, PositionFence,
    PositionResolutionAttemptFailure, PositionResolutionFailure,
};

use super::super::super::assigned_event::AssignedConsumerEvent as RetainedEvent;
use super::{
    AssignedConsumerEvent, AssignedConsumerFetchFailure, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchFence, AssignedConsumerFetchThrottleFailure,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailure, AssignedConsumerPositionResolutionFailureKind,
};

pub(in crate::consumer::assigned_host) fn translate_retained_event(
    event: RetainedEvent,
) -> AssignedConsumerEvent {
    match event {
        RetainedEvent::PositionResolutionFailed {
            topic,
            fence,
            failure,
        } => AssignedConsumerEvent::PositionResolutionFailed(
            AssignedConsumerPositionResolutionFailure {
                fence: position_fence(topic, fence),
                kind: match failure {
                    PositionResolutionFailure::DeadlineElapsed => {
                        AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed
                    }
                    PositionResolutionFailure::Attempt(attempt) => match attempt {
                        PositionResolutionAttemptFailure::DeadlineElapsed => {
                            AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed
                        }
                        PositionResolutionAttemptFailure::DriverRejected => {
                            AssignedConsumerPositionResolutionFailureKind::DriverRejected
                        }
                        PositionResolutionAttemptFailure::Transport => {
                            AssignedConsumerPositionResolutionFailureKind::Transport
                        }
                        PositionResolutionAttemptFailure::Broker(code) => {
                            AssignedConsumerPositionResolutionFailureKind::Broker(code.get())
                        }
                        PositionResolutionAttemptFailure::Compatibility => {
                            AssignedConsumerPositionResolutionFailureKind::Compatibility
                        }
                        PositionResolutionAttemptFailure::InvalidResponse => {
                            AssignedConsumerPositionResolutionFailureKind::InvalidResponse
                        }
                        PositionResolutionAttemptFailure::ResponseTooLarge => {
                            AssignedConsumerPositionResolutionFailureKind::ResponseTooLarge
                        }
                    },
                    PositionResolutionFailure::ThrottleDeadlineOverflow => {
                        AssignedConsumerPositionResolutionFailureKind::ThrottleDeadlineOverflow
                    }
                },
            },
        ),
        RetainedEvent::FetchThrottleFailed {
            topic,
            fence,
            failure,
        } => AssignedConsumerEvent::FetchThrottleFailed(AssignedConsumerFetchThrottleFailure {
            fence: fetch_fence(topic, fence),
            kind: match failure {
                FetchThrottleFailure::DeadlineOverflow => {
                    AssignedConsumerFetchThrottleFailureKind::DeadlineOverflow
                }
            },
        }),
        RetainedEvent::FetchFailed {
            topic,
            fence,
            failure,
        } => AssignedConsumerEvent::FetchFailed(AssignedConsumerFetchFailure {
            fence: fetch_fence(topic, fence),
            kind: match failure {
                FetchFailure::DeadlineElapsed => AssignedConsumerFetchFailureKind::DeadlineElapsed,
                FetchFailure::DriverRejected => AssignedConsumerFetchFailureKind::DriverRejected,
                FetchFailure::Transport => AssignedConsumerFetchFailureKind::Transport,
                FetchFailure::Broker(code) => AssignedConsumerFetchFailureKind::Broker(code.get()),
                FetchFailure::Compatibility => AssignedConsumerFetchFailureKind::Compatibility,
                FetchFailure::InvalidResponse => AssignedConsumerFetchFailureKind::InvalidResponse,
                FetchFailure::ResponseTooLarge => {
                    AssignedConsumerFetchFailureKind::ResponseTooLarge
                }
            },
        }),
    }
}

fn fetch_fence(topic: std::sync::Arc<str>, fence: FetchFence) -> AssignedConsumerFetchFence {
    AssignedConsumerFetchFence {
        position: position_fence(topic, fence.position()),
        fetch_revision: fence.revision().get(),
    }
}

fn position_fence(
    topic: std::sync::Arc<str>,
    fence: PositionFence,
) -> AssignedConsumerPositionFence {
    AssignedConsumerPositionFence {
        topic,
        partition: fence.partition().partition().get().cast_signed(),
        assignment_epoch: fence.assignment_epoch().get(),
        position_epoch: fence.position_epoch().get(),
    }
}
