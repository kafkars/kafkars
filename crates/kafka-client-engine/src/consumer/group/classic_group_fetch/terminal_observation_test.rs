//! Exact-once group Fetch terminal delivery and retirement-fencing scenarios.

use std::{num::NonZeroI16, sync::Arc, time::Duration};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedTopicPartition, AssignmentGeneration,
    FetchFailure, FetchRecords, GroupAssignmentPartition, LiveGroupAssignment, Moment,
    NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

use crate::{
    clock::MonotonicClock,
    consumer::{
        GroupConsumerFetchFailureKind,
        group_seek::{
            GroupConsumerSeekCompletion, GroupConsumerSeekCompletionObservation,
            GroupConsumerSeekTerminal,
        },
    },
};

use super::{
    super::session_catalog::{CurrentGroupSession, GroupSessionCatalog},
    ClassicGroupFetchFront, ClassicGroupFetchOwner,
    test_support::{committed, completed_ready, position_fence},
};

#[test]
fn retained_fetch_failures_transfer_exact_detail_once() {
    let broker = NonZeroI16::new(-47).unwrap_or_else(|| panic!("nonzero broker code"));
    for (failure, expected) in [
        (
            FetchFailure::DeadlineElapsed,
            GroupConsumerFetchFailureKind::DeadlineElapsed,
        ),
        (
            FetchFailure::DriverRejected,
            GroupConsumerFetchFailureKind::DriverRejected,
        ),
        (
            FetchFailure::Transport,
            GroupConsumerFetchFailureKind::Transport,
        ),
        (
            FetchFailure::Broker(broker),
            GroupConsumerFetchFailureKind::Broker(-47),
        ),
        (
            FetchFailure::Compatibility,
            GroupConsumerFetchFailureKind::Compatibility,
        ),
        (
            FetchFailure::InvalidResponse,
            GroupConsumerFetchFailureKind::InvalidResponse,
        ),
        (
            FetchFailure::ResponseTooLarge,
            GroupConsumerFetchFailureKind::ResponseTooLarge,
        ),
    ] {
        let (mut owner, _catalog) = owner_with_retained_fetch_failure(failure);

        assert_eq!(owner.take_fetch_failure(), Ok(Some(expected)));
        assert_eq!(owner.take_fetch_failure(), Ok(None));
        assert_eq!(owner.events.retained(), (0, 0));
    }
}

#[test]
fn throttle_deadline_overflow_transfers_once_at_delivery_observation() {
    let catalog = active_catalog();
    let mut owner = activated_owner();
    let fetch_fence = prepare_and_abandon_fetch_call(&mut owner, &catalog);
    let transition = owner
        .machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: fetch_fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: NextFetchOffset::try_from_raw(18)
                .unwrap_or_else(|| panic!("next Fetch offset")),
            now: Moment::from_tick(u64::MAX - 1),
            throttle_ticks: 2,
        })
        .unwrap_or_else(|error| panic!("Fetch throttle transition: {error}"));
    owner.effects.extend(transition.into_effects());

    retain_front_terminal(&mut owner, &catalog);
    assert_eq!(
        owner.take_fetch_failure(),
        Ok(Some(
            GroupConsumerFetchFailureKind::ThrottleDeadlineOverflow
        ))
    );
    assert_eq!(owner.take_fetch_failure(), Ok(None));
}

#[test]
fn assignment_retirement_discards_an_unobserved_fetch_failure() {
    let (mut owner, _catalog) = owner_with_retained_fetch_failure(FetchFailure::Transport);

    owner
        .retire_for_assignment_loss(&assignment())
        .unwrap_or_else(|error| panic!("retire assignment: {error:?}"));

    assert!(owner.activation().is_none());
    assert_eq!(owner.take_fetch_failure(), Ok(None));
    assert_eq!(owner.events.retained(), (0, 0));
}

#[test]
fn terminal_arriving_after_retirement_discards_without_a_stale_claim() {
    let catalog = active_catalog();
    let mut owner = activated_owner();
    let fetch_fence = prepare_and_abandon_fetch_call(&mut owner, &catalog);
    let terminal = owner
        .machine
        .apply(AssignedConsumerInput::FetchFailed {
            fence: fetch_fence,
            failure: FetchFailure::Transport,
        })
        .unwrap_or_else(|error| panic!("late Fetch terminal: {error}"))
        .into_effects()
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("late Fetch terminal effect"));
    owner
        .retire_for_assignment_loss(&assignment())
        .unwrap_or_else(|error| panic!("retire assignment: {error:?}"));
    while owner.front_effect_for_test().is_some() {
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }
    assert_eq!(owner.events.retained(), (0, 0));
    owner.effects.push_back(terminal);

    assert_eq!(
        owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(owner.events.retained(), (0, 0));
    assert!(owner.fault().is_none());
}

#[test]
fn explicit_close_can_discard_one_ready_terminal_after_assignment_retirement() {
    let (mut owner, _catalog) = owner_with_retained_fetch_failure(FetchFailure::Transport);
    owner
        .retire_for_assignment_loss(&assignment())
        .unwrap_or_else(|error| panic!("retire assignment: {error:?}"));
    assert_eq!(owner.events.retained(), (0, 1));

    assert!(owner.discard_one_retired_terminal_for_close());
    assert!(!owner.discard_one_retired_terminal_for_close());
    assert_eq!(owner.events.retained(), (0, 0));
}

#[test]
fn admitted_same_assignment_seek_discards_an_older_unobserved_fetch_failure() {
    let (mut owner, catalog) = owner_with_retained_fetch_failure(FetchFailure::Transport);
    let completion = Arc::new(GroupConsumerSeekCompletion::pending());
    owner
        .seek_partition(
            position_fence(7),
            AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(1)),
            StartPosition::Offset(
                NextFetchOffset::try_from_raw(23).unwrap_or_else(|| panic!("seek offset")),
            ),
            MonotonicClock::new()
                .capture_deadline_after(Duration::from_secs(30))
                .unwrap_or_else(|error| panic!("seek capture: {error}")),
            Arc::clone(&completion),
        )
        .unwrap_or_else(|error| panic!("same-assignment seek: {error:?}"));

    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Terminal(GroupConsumerSeekTerminal::Succeeded)
    );
    assert!(matches!(
        owner.effects.iter().collect::<Vec<_>>().as_slice(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset, .. },
        ] if next_offset.get() == 23
    ));
    assert_eq!(owner.events.retained(), (1, 1));
    assert_eq!(owner.take_fetch_failure(), Ok(None));
    assert_eq!(owner.events.retained(), (1, 0));
    while !owner.effects.is_empty() {
        assert_eq!(
            owner.interpret_front_effect(&catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted
        );
    }
    assert!(owner.fault().is_none());
}

fn owner_with_retained_fetch_failure(
    failure: FetchFailure,
) -> (ClassicGroupFetchOwner, GroupSessionCatalog) {
    let catalog = active_catalog();
    let mut owner = activated_owner();
    let fetch_fence = prepare_and_abandon_fetch_call(&mut owner, &catalog);
    let transition = owner
        .machine
        .apply(AssignedConsumerInput::FetchFailed {
            fence: fetch_fence,
            failure,
        })
        .unwrap_or_else(|error| panic!("Fetch failure transition: {error}"));
    owner.effects.extend(transition.into_effects());
    retain_front_terminal(&mut owner, &catalog);
    (owner, catalog)
}

fn prepare_and_abandon_fetch_call(
    owner: &mut ClassicGroupFetchOwner,
    catalog: &GroupSessionCatalog,
) -> kafka_client_core::FetchFence {
    let fetch_fence = match owner.effects.front().copied() {
        Some(AssignedConsumerEffect::FetchReady { fence, .. }) => fence,
        effect => panic!("initial FetchReady, got {effect:?}"),
    };
    assert_eq!(
        owner.interpret_front_effect(catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    drop(
        owner
            .pending_fetches
            .pop_front()
            .unwrap_or_else(|| panic!("prepared Fetch call")),
    );
    fetch_fence
}

fn retain_front_terminal(owner: &mut ClassicGroupFetchOwner, catalog: &GroupSessionCatalog) {
    assert!(matches!(
        owner.effects.front(),
        Some(
            AssignedConsumerEffect::FetchFailed { .. }
                | AssignedConsumerEffect::FetchThrottleFailed { .. }
        )
    ));
    assert_eq!(
        owner.interpret_front_effect(catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert!(owner.effects.is_empty());
    assert_eq!(owner.events.retained(), (0, 1));
}

fn activated_owner() -> ClassicGroupFetchOwner {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    owner
}

fn active_catalog() -> GroupSessionCatalog {
    let fence = position_fence(7);
    let assignment = assignment();
    let mut catalog = GroupSessionCatalog::try_new(
        fence.group_id(),
        Arc::from("workers"),
        &[Arc::from("orders")],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    catalog.current = Some(CurrentGroupSession {
        member_id: fence.member_id(),
        member: Arc::from("member-a"),
        installed_cycle: fence.membership_cycle(),
        classic_generation: 3,
        assignment,
    });
    catalog
}

fn assignment() -> LiveGroupAssignment {
    let fence = position_fence(7);
    LiveGroupAssignment::try_new(
        fence.group_id(),
        fence.member_id(),
        AssignmentGeneration::try_from_raw(7).unwrap_or_else(|| panic!("assignment generation")),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(1),
        )],
    )
    .unwrap_or_else(|error| panic!("live assignment: {error:?}"))
}
