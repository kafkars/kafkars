//! Shared catalog, deadline, and completed-position fixtures for group Fetch tests.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, GroupPositionBatch,
    GroupPositionFence, GroupPositionPartitionFact, MemberId, MembershipCycle, Moment,
    NextFetchOffset, PartitionIndex, TopicId,
};

use super::super::{
    classic_group_position::{
        ClassicGroupPositionCompleted, test_support::completed_ready as completed_position_ready,
    },
    session_catalog::GroupSessionCatalog,
};
use crate::{
    clock::MonotonicClock,
    consumer::fetch_execution::{FetchTerminalFixture, install_terminal_for_test},
    protocol::fetch::fixture::encoded_delivery_batches_for_test,
};

use super::{ClassicGroupFetchFront, ClassicGroupFetchOwner};

pub(super) const ATTEMPT_TIMEOUT_TICKS: u64 = 30_000_000_000;

pub(in crate::consumer::group) fn completed_ready(
    fence: GroupPositionFence,
    observed_at: Moment,
    throttle_time_ms: u32,
    facts: Vec<GroupPositionPartitionFact>,
) -> ClassicGroupPositionCompleted {
    completed_position_ready(
        fence,
        observed_at,
        GroupPositionBatch::new(throttle_time_ms, facts),
    )
}

pub(super) fn committed(topic: u64, partition: u32, offset: i64) -> GroupPositionPartitionFact {
    GroupPositionPartitionFact::committed(
        GroupAssignmentPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("next offset")),
    )
}

pub(super) fn position_fence(generation: u64) -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        MemberId::try_from_raw(5).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("assignment generation")),
    )
}

pub(super) fn catalog(topics: &[&str]) -> GroupSessionCatalog {
    let topics = topics
        .iter()
        .map(|topic| Arc::<str>::from(*topic))
        .collect::<Vec<_>>();
    GroupSessionCatalog::try_new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group identity")),
        Arc::from("workers"),
        &topics,
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

pub(super) fn assert_attempt_deadline(deadline: Deadline, before: Moment, after: Moment) {
    assert!(deadline.tick() >= before.tick() + ATTEMPT_TIMEOUT_TICKS);
    assert!(deadline.tick() <= after.tick() + ATTEMPT_TIMEOUT_TICKS);
}

pub(in crate::consumer::group) fn install_ready_delivery_for_test(
    owner: &mut ClassicGroupFetchOwner,
    catalog: &GroupSessionCatalog,
    first_offset: i64,
) {
    let front = owner.interpret_front_effect(catalog, &MonotonicClock::new());
    assert_eq!(front, ClassicGroupFetchFront::Interpreted);
    let prepared = owner
        .pending_fetches
        .pop_front()
        .unwrap_or_else(|| panic!("prepared group Fetch"));
    install_terminal_for_test(
        &mut owner.fetches,
        prepared,
        FetchTerminalFixture::Success(Some(encoded_delivery_batches_for_test(first_offset))),
    );
    let transition = owner
        .fetches
        .poll(
            &mut owner.machine,
            Moment::from_tick(first_offset.unsigned_abs().saturating_add(1)),
        )
        .unwrap_or_else(|error| panic!("group Fetch settlement: {error:?}"))
        .unwrap_or_else(|| panic!("group Fetch transition"));
    owner.effects.extend(transition.into_effects());
    while !owner.effects.is_empty() {
        assert_eq!(
            owner.interpret_front_effect(catalog, &MonotonicClock::new()),
            ClassicGroupFetchFront::Interpreted,
            "fault={:?} effect={:?}",
            owner
                .fault()
                .map(super::model::ClassicGroupFetchOwnerFault::kind),
            owner.front_effect_for_test(),
        );
    }
}

pub(in crate::consumer::group) fn install_retained_fetch_failure_for_test(
    owner: &mut ClassicGroupFetchOwner,
    catalog: &GroupSessionCatalog,
    failure: kafka_client_core::FetchFailure,
) {
    let fetch_fence = match owner.effects.front().copied() {
        Some(kafka_client_core::AssignedConsumerEffect::FetchReady { fence, .. }) => fence,
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
    let transition = owner
        .machine
        .apply(kafka_client_core::AssignedConsumerInput::FetchFailed {
            fence: fetch_fence,
            failure,
        })
        .unwrap_or_else(|error| panic!("Fetch failure transition: {error}"));
    owner.effects.extend(transition.into_effects());
    assert_eq!(
        owner.interpret_front_effect(catalog, &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    assert!(owner.effects.is_empty());
    assert_eq!(owner.events.retained(), (0, 1));
}
