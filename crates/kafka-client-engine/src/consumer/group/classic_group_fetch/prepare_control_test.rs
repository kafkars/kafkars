//! Throttle-timer interpretation and control-fencing scenarios.

use kafka_client_core::{AssignedConsumerEffect, Deadline, Moment};

use crate::clock::MonotonicClock;

use super::{
    ClassicGroupFetchFront, ClassicGroupFetchOwner,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn positive_activation_throttle_arms_observation_deadline_without_attempt_capture() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 1, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));

    assert!(matches!(
        owner.front_effect_for_test(),
        Some(AssignedConsumerEffect::ArmFetchThrottle {
            deadline,
            ..
        }) if deadline == Deadline::from_tick(1_000_041)
    ));
    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(owner.pending_count_for_test(), 0);
    assert_eq!(owner.timer_count_for_test(), 1);
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(1_000_041)));
}

#[test]
fn control_fences_armed_timer_before_consuming_exact_effect() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 1, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let clock = MonotonicClock::new();
    let armed = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("armed Fetch retained"));
    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &clock),
        ClassicGroupFetchFront::Interpreted
    );
    let AssignedConsumerEffect::ArmFetchThrottle { fence, .. } = armed else {
        panic!("test support retains armed Fetch identity");
    };
    let control = AssignedConsumerEffect::Revoke {
        assignment_epoch: fence.position().assignment_epoch(),
        partition: fence.position().partition(),
    };
    owner.effects.push_front(control);

    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &clock),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(owner.timer_count_for_test(), 0);
    assert_eq!(owner.next_deadline(), None);
    assert!(owner.fault().is_none());
}

#[test]
fn faulted_owner_suppresses_retained_timer_deadline() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 1, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let clock = MonotonicClock::new();
    let armed = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("armed Fetch retained"));
    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &clock),
        ClassicGroupFetchFront::Interpreted
    );
    let AssignedConsumerEffect::ArmFetchThrottle { fence, .. } = armed else {
        panic!("test support retains armed Fetch identity");
    };
    owner.fetches.install_fault_for_test();
    owner.effects.push_front(AssignedConsumerEffect::Revoke {
        assignment_epoch: fence.position().assignment_epoch(),
        partition: fence.position().partition(),
    });

    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &clock),
        ClassicGroupFetchFront::Idle
    );
    assert_eq!(owner.timer_count_for_test(), 1);
    assert!(owner.fault().is_some());
    assert_eq!(owner.next_deadline(), None);
}
