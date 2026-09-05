//! Broker Fetch-session membership and route authority across replacements.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedPartition, Deadline, Moment,
    StartPosition, partitioning::TopicMetadataGeneration,
};

use crate::protocol::fetch::FetchSessionUpdate;

use super::{
    broker_session::{BrokerFetchSessions, BrokerSessionMember},
    broker_session_test::{assignment, broker, established, fetch_fence, incremental, member},
};

#[test]
fn established_member_retains_the_exact_route_for_the_next_fetch_revision() {
    let (effects, _machine) = assignment();
    let broker = broker(3);
    let member = BrokerSessionMember::with_route(
        fetch_fence(effects[0]).position(),
        Arc::from("alpha"),
        [7; 16],
        Some(11),
    );
    let position = member.position();
    let mut sessions = BrokerFetchSessions::try_new(4, 8)
        .unwrap_or_else(|error| panic!("reserve broker sessions: {error:?}"));
    let plan = sessions
        .try_begin(broker, vec![member])
        .unwrap_or_else(|(error, _active)| panic!("begin routed member: {error:?}"));
    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 1)))
        .unwrap_or_else(|error| panic!("complete routed member: {error:?}"));

    assert_eq!(
        sessions.route_for_position(position),
        Some((broker, [7; 16], Some(11), None))
    );
}

#[test]
fn replacement_assignment_requires_metadata_newer_than_retired_route() {
    let (effects, mut machine) = assignment();
    let broker = broker(3);
    let observed = TopicMetadataGeneration::from_raw(17);
    let member = BrokerSessionMember::with_route(
        fetch_fence(effects[0]).position(),
        Arc::from("alpha"),
        [7; 16],
        Some(11),
    )
    .with_metadata_generation(Some(observed));
    let mut sessions = BrokerFetchSessions::try_new(4, 8)
        .unwrap_or_else(|error| panic!("reserve broker sessions: {error:?}"));
    let plan = sessions
        .try_begin(broker, vec![member])
        .unwrap_or_else(|(error, _active)| panic!("begin routed member: {error:?}"));
    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 1)))
        .unwrap_or_else(|error| panic!("complete routed member: {error:?}"));

    let old_epoch = fetch_fence(effects[0]).position().assignment_epoch();
    let retired = machine
        .apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: Some(old_epoch),
        })
        .unwrap_or_else(|error| panic!("retire assignment: {error}"));
    for effect in retired.effects().iter().copied() {
        sessions.observe_control(effect);
    }
    assert_eq!(
        sessions.newer_route_generation(fetch_fence(effects[0]).position(), "alpha"),
        None
    );

    let partitions = effects
        .iter()
        .copied()
        .map(|effect| {
            let AssignedConsumerEffect::FetchReady { fence, next_offset } = effect else {
                panic!("FetchReady effect");
            };
            AssignedPartition::new(
                fence.position().partition(),
                StartPosition::Offset(next_offset),
            )
        })
        .collect();
    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(1_000),
        })
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));
    assert_eq!(
        sessions.newer_route_generation(fetch_fence(replacement.effects()[0]).position(), "alpha"),
        Some(observed)
    );
}

#[test]
fn carried_partition_is_active_while_only_removed_partition_is_forgotten() {
    let (effects, _machine) = assignment();
    let broker = broker(3);
    let mut sessions = established(&effects, broker);
    for effect in effects.iter().copied() {
        let position = fetch_fence(effect).position();
        sessions.observe_control(AssignedConsumerEffect::Revoke {
            assignment_epoch: position.assignment_epoch(),
            partition: position.partition(),
        });
    }

    let carried = member(effects[1], "beta");
    let plan = sessions
        .try_begin(broker, vec![carried.clone()])
        .unwrap_or_else(|(error, _active)| panic!("replacement epoch: {error:?}"));
    assert_eq!(plan.active(), &[carried]);
    assert_eq!(plan.forgotten(), &[member(effects[0], "alpha")]);
    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 2)))
        .unwrap_or_else(|error| panic!("complete replacement epoch: {error:?}"));
    assert_eq!(sessions.retained(), (1, 1));
}
