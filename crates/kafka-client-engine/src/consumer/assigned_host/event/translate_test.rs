//! Exhaustive retained-to-public event translation scenarios.

use std::{num::NonZeroI16, sync::Arc, time::Duration};

use kafka_client_core::{
    AssignedConsumerEffect, FetchFailure, FetchThrottleFailure, PositionResolutionFailure,
    StartPosition,
};

use super::super::super::{
    assigned_host::shard_test::setup, assigned_owner_effect::FrontEffect,
    assigned_owner_test::input,
};
use super::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionResolutionFailureKind,
};

#[test]
fn fifo_translation_preserves_the_exact_named_position_fence() {
    let (owner, port, _wake) = setup();
    let _accepted = port
        .replace_assignment(
            vec![input("orders", 3, StartPosition::Beginning)],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    owner
        .try_with_owner(|assigned| {
            let Some(AssignedConsumerEffect::ResolvePosition { fence, .. }) =
                assigned.effects.front().copied()
            else {
                panic!("position claim");
            };
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            assigned
                .effects
                .push_back(AssignedConsumerEffect::PositionResolutionFailed {
                    fence,
                    failure: PositionResolutionFailure::AttemptFailed,
                });
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
        })
        .unwrap_or_else(|error| panic!("owner slot: {error:?}"));

    let event = port
        .take_event()
        .unwrap_or_else(|error| panic!("take event: {error:?}"))
        .unwrap_or_else(|| panic!("ready event"));
    let AssignedConsumerEvent::PositionResolutionFailed(failure) = event else {
        panic!("position failure");
    };
    assert_eq!(failure.fence().topic(), "orders");
    assert_eq!(failure.fence().partition(), 3);
    assert_eq!(failure.fence().assignment_epoch(), 1);
    assert_eq!(failure.fence().position_epoch(), 1);
    assert_eq!(
        failure.kind(),
        AssignedConsumerPositionResolutionFailureKind::AttemptFailed
    );
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
