//! Fenced classic-group seek admission and paused-position scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedTopicPartition, DeliveryOwnership, Moment, NextFetchOffset,
    PartitionIndex, StartPosition, TopicId,
};

use crate::{
    clock::MonotonicClock,
    consumer::group_seek::{
        GroupConsumerSeekCompletion, GroupConsumerSeekCompletionObservation,
        GroupConsumerSeekTerminal,
    },
};

use super::{
    ClassicGroupFetchFront, ClassicGroupFetchOwner,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn explicit_offset_completes_after_core_fences_old_fetch() {
    let fence = position_fence(7);
    let catalog = catalog(&["orders"]);
    let mut owner = active_owner(fence);
    let AssignedConsumerEffect::FetchReady { fence: old, .. } = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("old Fetch"))
    else {
        panic!("activation starts Fetch");
    };
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    let completion = Arc::new(GroupConsumerSeekCompletion::pending());

    owner
        .seek_partition(
            fence,
            partition(),
            StartPosition::Offset(
                NextFetchOffset::try_from_raw(42).unwrap_or_else(|| panic!("offset")),
            ),
            capture(),
            Arc::clone(&completion),
        )
        .unwrap_or_else(|_error| panic!("seek admission"));

    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Terminal(GroupConsumerSeekTerminal::Succeeded)
    );
    assert_eq!(
        owner.machine.delivery_ownership(old),
        Ok(DeliveryOwnership::Superseded)
    );
    assert!(matches!(
        owner.effects.iter().collect::<Vec<_>>().as_slice(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset, .. }
        ] if next_offset.get() == 42
    ));
}

#[test]
fn active_symbolic_seek_retains_original_deadline_until_list_offsets_terminal() {
    let fence = position_fence(7);
    let catalog = catalog(&["orders"]);
    let mut owner = active_owner(fence);
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    let completion = Arc::new(GroupConsumerSeekCompletion::pending());
    let capture = capture();
    owner
        .seek_partition(
            fence,
            partition(),
            StartPosition::Beginning,
            capture,
            Arc::clone(&completion),
        )
        .unwrap_or_else(|_error| panic!("seek admission"));

    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Pending
    );
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(owner.pending_positions.len(), 1);
    assert_eq!(
        owner.pending_positions[0].deadline,
        capture.operation_deadline()
    );
}

#[test]
fn paused_explicit_offset_stays_paused_and_completes_without_list_offsets() {
    let fence = position_fence(7);
    let catalog = catalog(&["orders"]);
    let mut owner = active_owner(fence);
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    owner
        .pause_partitions(fence, &[partition()])
        .unwrap_or_else(|_error| panic!("pause"));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    let completion = Arc::new(GroupConsumerSeekCompletion::pending());

    owner
        .seek_partition(
            fence,
            partition(),
            StartPosition::Offset(
                NextFetchOffset::try_from_raw(91).unwrap_or_else(|| panic!("offset")),
            ),
            capture(),
            Arc::clone(&completion),
        )
        .unwrap_or_else(|_error| panic!("paused seek"));

    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Terminal(GroupConsumerSeekTerminal::Succeeded)
    );
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::Suspend { .. })
    ));
    assert!(owner.pending_positions.is_empty());
}

fn active_owner(fence: kafka_client_core::GroupPositionFence) -> ClassicGroupFetchOwner {
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 0, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("activation: {:?}", error.kind()));
    owner
}

fn capture() -> crate::clock::DeadlineCapture {
    MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"))
}

const fn partition() -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0))
}
