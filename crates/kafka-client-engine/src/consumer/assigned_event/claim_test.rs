//! Directional position and Fetch claim-fence scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchRecords, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};

use super::claim::EventClaim;

#[test]
fn claims_advance_only_through_directionally_newer_exact_fences() {
    let mut machine = AssignedConsumerMachine::new();
    let first = assign(&mut machine, StartPosition::Offset(offset(1)));
    let AssignedConsumerEffect::FetchReady { fence: first, .. } = first.effects()[0] else {
        panic!("first fetch");
    };
    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(2),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance fetch: {error}"));
    let AssignedConsumerEffect::FetchReady { fence: second, .. } = advanced.effects()[0] else {
        panic!("second fetch");
    };

    assert!(EventClaim::Position(first.position()).can_advance_to(EventClaim::Fetch(first)));
    assert!(EventClaim::Fetch(first).can_advance_to(EventClaim::Fetch(second)));
    assert!(!EventClaim::Fetch(second).can_advance_to(EventClaim::Fetch(first)));

    let replacement = assign(&mut machine, StartPosition::Beginning);
    let AssignedConsumerEffect::ResolvePosition { fence: newer, .. } = replacement.effects()[1]
    else {
        panic!("replacement position");
    };
    assert!(EventClaim::Fetch(second).is_older_than(newer));
    assert!(!EventClaim::Position(newer).is_older_than(second.position()));
}

fn assign(
    machine: &mut AssignedConsumerMachine,
    start: StartPosition,
) -> kafka_client_core::AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(partition(), start)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assign: {error}"))
}

fn partition() -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0))
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
