//! Atomic group Fetch pause and retained-position resume scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedTopicPartition, DeliveryOwnership, Moment, PartitionIndex,
    StartPosition, TopicId,
};

use crate::{clock::MonotonicClock, consumer::group_seek::GroupConsumerSeekCompletion};

use super::{
    ClassicGroupFetchControlError, ClassicGroupFetchFront, ClassicGroupFetchOwner,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn pause_fences_every_old_fetch_and_repeated_pause_is_idempotent() {
    let fence = position_fence(7);
    let catalog = catalog(&["orders"]);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(41),
                0,
                vec![committed(1, 0, 17), committed(1, 1, 23)],
            ),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let mut old = Vec::new();
    while let Some(AssignedConsumerEffect::FetchReady { fence, .. }) = owner.front_effect_for_test()
    {
        old.push(fence);
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }
    let targets = [partition(1), partition(0)];

    let paused = owner
        .pause_partitions(fence, &targets)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));

    assert_eq!(paused.effects(), 2);
    assert!(!paused.fault_retained());
    for old in old {
        assert_eq!(
            owner.machine.delivery_ownership(old),
            Ok(DeliveryOwnership::Superseded)
        );
    }
    while owner.front_effect_for_test().is_some() {
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }
    let repeated = owner
        .pause_partitions(fence, &targets)
        .unwrap_or_else(|error| panic!("repeated pause: {error:?}"));
    assert_eq!(repeated.effects(), 0);
}

#[test]
fn resume_requires_pause_controls_to_drain_then_restores_caller_order() {
    let fence = position_fence(7);
    let catalog = catalog(&["orders"]);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(41),
                0,
                vec![committed(1, 0, 17), committed(1, 1, 23)],
            ),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    while owner.front_effect_for_test().is_some() {
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }
    let targets = [partition(1), partition(0)];
    owner
        .pause_partitions(fence, &targets)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));

    assert_eq!(
        owner.resume_partitions(fence, &targets, resolution_capture()),
        Err(ClassicGroupFetchControlError::Pending)
    );
    while owner.front_effect_for_test().is_some() {
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }

    let resumed = owner
        .resume_partitions(fence, &targets, resolution_capture())
        .unwrap_or_else(|error| panic!("resume: {error:?}"));
    assert_eq!(resumed.effects(), 2);
    assert!(matches!(
        owner.effects.iter().collect::<Vec<_>>().as_slice(),
        [
            AssignedConsumerEffect::FetchReady { fence: second, .. },
            AssignedConsumerEffect::FetchReady { fence: first, .. },
        ] if second.position().partition() == partition(1)
            && first.position().partition() == partition(0)
    ));
}

#[test]
fn symbolic_position_sought_while_paused_resumes_with_exact_new_boundary() {
    let fence = position_fence(7);
    let catalog = catalog(&["orders"]);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 0, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    owner
        .pause_partitions(fence, &[partition(0)])
        .unwrap_or_else(|error| panic!("pause: {error:?}"));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    owner
        .seek_partition(
            fence,
            partition(0),
            StartPosition::Beginning,
            resolution_capture(),
            Arc::new(GroupConsumerSeekCompletion::pending()),
        )
        .unwrap_or_else(|_error| panic!("paused seek"));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    let capture = resolution_capture();

    let resumed = owner
        .resume_partitions(fence, &[partition(0)], capture)
        .unwrap_or_else(|error| panic!("symbolic resume: {error:?}"));

    assert_eq!(resumed.effects(), 1);
    assert!(matches!(
        owner.front_effect_for_test(),
        Some(AssignedConsumerEffect::ResolvePosition { deadline, .. })
            if deadline == capture.deadline()
    ));
    assert_eq!(owner.raw_position_deadlines.len(), 1);
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert!(owner.raw_position_deadlines.is_empty());
    assert_eq!(owner.pending_positions.len(), 1);
    assert_eq!(
        owner.pending_positions[0].deadline,
        capture.operation_deadline()
    );
}

const fn partition(index: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(index))
}

fn resolution_capture() -> crate::clock::DeadlineCapture {
    MonotonicClock::new()
        .capture_deadline_after(std::time::Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("resolution capture: {error:?}"))
}
