//! Terminal assigned-consumer event ownership and close scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    AssignedConsumerEffect, FetchFailure, FetchFence, FetchThrottleFailure, NextFetchOffset,
    PositionFence, PositionResolutionAttemptFailure, PositionResolutionFailure, StartPosition,
};

use super::{
    assigned_event::AssignedConsumerEvent,
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_effect::FrontEffect,
    assigned_owner_test::{input, owner},
};

#[test]
fn terminal_failure_effects_transfer_to_exact_fifo_events() {
    let mut owner = owner(3);
    owner
        .replace_assignment(
            vec![
                input("orders", 0, StartPosition::Beginning),
                input("orders", 1, StartPosition::Offset(offset(10))),
                input("orders", 2, StartPosition::Offset(offset(20))),
            ],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let mut position = None;
    let mut fetches = Vec::new();
    while let Some(effect) = owner.effects.front().copied() {
        match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => position = Some(fence),
            AssignedConsumerEffect::FetchReady { fence, .. } => fetches.push(fence),
            _ => panic!("unexpected assignment effect: {effect:?}"),
        }
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
    let position = position.unwrap_or_else(|| panic!("position fence"));
    assert_eq!(fetches.len(), 2);

    owner
        .effects
        .push_back(AssignedConsumerEffect::PositionResolutionFailed {
            fence: position,
            failure: PositionResolutionFailure::Attempt(
                PositionResolutionAttemptFailure::Transport,
            ),
        });
    owner
        .effects
        .push_back(AssignedConsumerEffect::FetchThrottleFailed {
            fence: fetches[0],
            failure: FetchThrottleFailure::DeadlineOverflow,
        });
    owner
        .effects
        .push_back(AssignedConsumerEffect::FetchFailed {
            fence: fetches[1],
            failure: FetchFailure::Transport,
        });
    drain_effects(&mut owner);

    assert_position_event(owner.take_event(), position);
    assert_fetch_throttle_event(owner.take_event(), fetches[0]);
    assert_fetch_event(owner.take_event(), fetches[1]);
    assert!(owner.take_event().is_none());
}

#[test]
fn ready_events_do_not_block_close_but_claimed_slots_do() {
    let mut owner = owner(1);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let fence = match owner.effects.pop_front() {
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) => fence,
        effect => panic!("initial Fetch claim, got {effect:?}"),
    };

    assert_eq!(owner.events.retained(), (1, 0));
    assert!(!owner.is_quiescent());
    let topic = Arc::clone(
        owner
            .topics
            .name(fence.position().partition().topic_id())
            .unwrap_or_else(|error| panic!("topic: {error:?}")),
    );
    owner
        .events
        .retain_terminal(
            topic,
            AssignedConsumerEffect::FetchFailed {
                fence,
                failure: FetchFailure::Transport,
            },
        )
        .unwrap_or_else(|(error, _topic)| panic!("retain event: {error:?}"));

    assert_eq!(owner.events.retained(), (0, 1));
    assert!(owner.is_quiescent());
    let _observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    drain_effects(&mut owner);
    assert!(owner.progress_close());
    drain_effects(&mut owner);
    assert!(owner.close_completed());
    assert_eq!(owner.events.retained(), (0, 1));
}

#[test]
fn elapsed_public_deadline_retains_the_immediate_terminal_event() {
    let mut owner = owner(1);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::ZERO,
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));

    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::PositionResolutionFailed {
            failure: PositionResolutionFailure::DeadlineElapsed,
            ..
        })
    ));
    drain_effects(&mut owner);
    assert!(matches!(
        owner.take_event(),
        Some(AssignedConsumerEvent::PositionResolutionFailed {
            topic,
            failure: PositionResolutionFailure::DeadlineElapsed,
            ..
        }) if topic.as_ref() == "orders"
    ));
    assert_eq!(owner.events.retained(), (0, 0));
    assert!(owner.fault.is_none());
}

fn drain_effects(owner: &mut AssignedConsumerOwner) {
    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
}

fn assert_position_event(event: Option<AssignedConsumerEvent>, fence: PositionFence) {
    assert!(matches!(
        event,
        Some(AssignedConsumerEvent::PositionResolutionFailed {
            topic,
            fence: actual,
            failure: PositionResolutionFailure::Attempt(
                PositionResolutionAttemptFailure::Transport,
            ),
        }) if topic.as_ref() == "orders" && actual == fence
    ));
}

fn assert_fetch_throttle_event(event: Option<AssignedConsumerEvent>, fence: FetchFence) {
    assert!(matches!(
        event,
        Some(AssignedConsumerEvent::FetchThrottleFailed {
            topic,
            fence: actual,
            failure: FetchThrottleFailure::DeadlineOverflow,
        }) if topic.as_ref() == "orders" && actual == fence
    ));
}

fn assert_fetch_event(event: Option<AssignedConsumerEvent>, fence: FetchFence) {
    assert!(matches!(
        event,
        Some(AssignedConsumerEvent::FetchFailed {
            topic,
            fence: actual,
            failure: FetchFailure::Transport,
        }) if topic.as_ref() == "orders" && actual == fence
    ));
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
