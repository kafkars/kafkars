//! Exhaustive translation from engine events into stable facade values.

use kafka_client_engine::{
    AssignedConsumerEvent as EngineEvent, AssignedConsumerFetchFailureKind as EngineFetchFailure,
    AssignedConsumerFetchFence as EngineFetchFence,
    AssignedConsumerFetchThrottleFailureKind as EngineFetchThrottleFailure,
    AssignedConsumerPositionFence as EnginePositionFence,
    AssignedConsumerPositionResolutionFailureKind as EnginePositionFailure,
};

use crate::consumer::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind,
};

pub(super) fn translate_assigned_event(event: EngineEvent) -> AssignedConsumerEvent {
    match event {
        EngineEvent::PositionResolutionFailed(failure) => {
            AssignedConsumerEvent::PositionResolutionFailed {
                fence: translate_position_fence(failure.fence()),
                kind: translate_position_failure(failure.kind()),
            }
        }
        EngineEvent::FetchThrottleFailed(failure) => AssignedConsumerEvent::FetchThrottleFailed {
            fence: translate_fetch_fence(failure.fence()),
            kind: translate_fetch_throttle_failure(failure.kind()),
        },
        EngineEvent::FetchFailed(failure) => AssignedConsumerEvent::FetchFailed {
            fence: translate_fetch_fence(failure.fence()),
            kind: translate_fetch_failure(failure.kind()),
        },
    }
}

pub(super) const fn translate_position_failure(
    failure: EnginePositionFailure,
) -> AssignedConsumerPositionResolutionFailureKind {
    match failure {
        EnginePositionFailure::DeadlineElapsed => {
            AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed
        }
        EnginePositionFailure::DriverRejected => {
            AssignedConsumerPositionResolutionFailureKind::DriverRejected
        }
        EnginePositionFailure::Transport => {
            AssignedConsumerPositionResolutionFailureKind::Transport
        }
        EnginePositionFailure::Broker(code) => {
            AssignedConsumerPositionResolutionFailureKind::Broker(code)
        }
        EnginePositionFailure::Compatibility => {
            AssignedConsumerPositionResolutionFailureKind::Compatibility
        }
        EnginePositionFailure::InvalidResponse => {
            AssignedConsumerPositionResolutionFailureKind::InvalidResponse
        }
        EnginePositionFailure::ResponseTooLarge => {
            AssignedConsumerPositionResolutionFailureKind::ResponseTooLarge
        }
        EnginePositionFailure::ThrottleDeadlineOverflow => {
            AssignedConsumerPositionResolutionFailureKind::ThrottleDeadlineOverflow
        }
    }
}

pub(super) const fn translate_fetch_throttle_failure(
    failure: EngineFetchThrottleFailure,
) -> AssignedConsumerFetchThrottleFailureKind {
    match failure {
        EngineFetchThrottleFailure::DeadlineOverflow => {
            AssignedConsumerFetchThrottleFailureKind::DeadlineOverflow
        }
    }
}

pub(super) const fn translate_fetch_failure(
    failure: EngineFetchFailure,
) -> AssignedConsumerFetchFailureKind {
    match failure {
        EngineFetchFailure::DeadlineElapsed => AssignedConsumerFetchFailureKind::DeadlineElapsed,
        EngineFetchFailure::DriverRejected => AssignedConsumerFetchFailureKind::DriverRejected,
        EngineFetchFailure::Transport => AssignedConsumerFetchFailureKind::Transport,
        EngineFetchFailure::Broker(code) => AssignedConsumerFetchFailureKind::Broker(code),
        EngineFetchFailure::Compatibility => AssignedConsumerFetchFailureKind::Compatibility,
        EngineFetchFailure::InvalidResponse => AssignedConsumerFetchFailureKind::InvalidResponse,
        EngineFetchFailure::ResponseTooLarge => AssignedConsumerFetchFailureKind::ResponseTooLarge,
    }
}

fn translate_fetch_fence(fence: &EngineFetchFence) -> AssignedConsumerFetchFence {
    AssignedConsumerFetchFence::from_parts(
        translate_position_fence(fence.position()),
        fence.fetch_revision(),
    )
}

fn translate_position_fence(fence: &EnginePositionFence) -> AssignedConsumerPositionFence {
    AssignedConsumerPositionFence::from_parts(
        fence.topic().to_owned(),
        fence.partition(),
        fence.assignment_epoch(),
        fence.position_epoch(),
    )
}
