//! Exhaustive retained-to-public event translation scenarios.

use std::{num::NonZeroI16, sync::Arc};

use kafka_client_core::{
    FetchFailure, FetchThrottleFailure, PositionResolutionAttemptFailure, PositionResolutionFailure,
};

use super::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionResolutionFailureKind,
};

#[test]
fn every_position_failure_category_translates_without_fallbacks() {
    let Some(code) = NonZeroI16::new(-42) else {
        panic!("negative broker code is nonzero");
    };
    let cases = [
        (
            PositionResolutionFailure::DeadlineElapsed,
            AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed,
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::DeadlineElapsed),
            AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed,
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::DriverRejected),
            AssignedConsumerPositionResolutionFailureKind::DriverRejected,
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::Transport),
            AssignedConsumerPositionResolutionFailureKind::Transport,
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::Broker(code)),
            AssignedConsumerPositionResolutionFailureKind::Broker(-42),
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::Compatibility),
            AssignedConsumerPositionResolutionFailureKind::Compatibility,
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::InvalidResponse),
            AssignedConsumerPositionResolutionFailureKind::InvalidResponse,
        ),
        (
            PositionResolutionFailure::Attempt(PositionResolutionAttemptFailure::ResponseTooLarge),
            AssignedConsumerPositionResolutionFailureKind::ResponseTooLarge,
        ),
        (
            PositionResolutionFailure::ThrottleDeadlineOverflow,
            AssignedConsumerPositionResolutionFailureKind::ThrottleDeadlineOverflow,
        ),
    ];

    for (core, expected) in cases {
        let event = super::translate::translate_retained_event(
            crate::consumer::assigned_event::AssignedConsumerEvent::PositionResolutionFailed {
                topic: Arc::from("orders"),
                fence: core_fetch_fence().position(),
                failure: core,
            },
        );
        let AssignedConsumerEvent::PositionResolutionFailed(failure) = event else {
            panic!("position failure");
        };
        assert_eq!(failure.kind(), expected);
        assert_eq!(failure.fence().topic(), "orders");
    }
}

#[test]
fn fetch_translation_preserves_revision_and_signed_broker_code() {
    let throttle = super::translate::translate_retained_event(retained_fetch_throttle());
    let AssignedConsumerEvent::FetchThrottleFailed(failure) = throttle else {
        panic!("Fetch throttle failure");
    };
    assert_eq!(
        failure.kind(),
        AssignedConsumerFetchThrottleFailureKind::DeadlineOverflow
    );

    let event = super::translate::translate_retained_event(retained_fetch());
    let AssignedConsumerEvent::FetchFailed(failure) = event else {
        panic!("Fetch failure");
    };
    let core_fence = core_fetch_fence();
    assert_eq!(failure.fence().position().topic(), "orders");
    assert_eq!(
        failure.fence().position().partition(),
        core_fence
            .position()
            .partition()
            .partition()
            .get()
            .cast_signed()
    );
    assert_eq!(
        failure.fence().fetch_revision(),
        core_fence.revision().get()
    );
    assert_eq!(
        failure.kind(),
        AssignedConsumerFetchFailureKind::Broker(-42)
    );
}

fn retained_fetch() -> crate::consumer::assigned_event::AssignedConsumerEvent {
    crate::consumer::assigned_event::AssignedConsumerEvent::FetchFailed {
        topic: Arc::from("orders"),
        fence: core_fetch_fence(),
        failure: FetchFailure::Broker(
            NonZeroI16::new(-42).unwrap_or_else(|| panic!("nonzero broker code")),
        ),
    }
}

fn retained_fetch_throttle() -> crate::consumer::assigned_event::AssignedConsumerEvent {
    crate::consumer::assigned_event::AssignedConsumerEvent::FetchThrottleFailed {
        topic: Arc::from("orders"),
        fence: core_fetch_fence(),
        failure: FetchThrottleFailure::DeadlineOverflow,
    }
}

fn core_fetch_fence() -> kafka_client_core::FetchFence {
    crate::consumer::fetch_store_test::fences()[0]
}
