//! Exact cycle, topic order, and deadline retention scenarios.

use std::time::Instant;

use kafka_client_core::{Deadline, MembershipCycle, TopicId};

use crate::clock::OperationDeadline;

use super::classic_group_partition_counts::PreparedClassicGroupPartitionCounts;

#[test]
fn prepared_count_read_retains_the_exact_core_effect_facts() {
    let cycle = MembershipCycle::initial();
    let topics = vec![TopicId::from_raw(3), TopicId::from_raw(9)];
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(91), Instant::now());

    let prepared = PreparedClassicGroupPartitionCounts::try_new(cycle, topics, deadline)
        .unwrap_or_else(|error| panic!("count owner failed: {error:?}"));

    assert_eq!(prepared.cycle(), cycle);
    assert_eq!(
        prepared.topics(),
        &[TopicId::from_raw(3), TopicId::from_raw(9)]
    );
    assert_eq!(prepared.deadline(), deadline);
}

#[test]
fn progress_accepts_only_the_next_ordered_topic_once() {
    let mut prepared = PreparedClassicGroupPartitionCounts::try_new(
        MembershipCycle::initial(),
        vec![TopicId::from_raw(3), TopicId::from_raw(9)],
        OperationDeadline::from_parts_for_test(Deadline::from_tick(91), Instant::now()),
    )
    .unwrap_or_else(|error| panic!("count owner failed: {error:?}"));

    assert!(prepared.append(TopicId::from_raw(9), 2, 5).is_err());
    prepared
        .append(TopicId::from_raw(3), 4, 5)
        .unwrap_or_else(|error| panic!("first count failed: {error:?}"));
    prepared
        .append(TopicId::from_raw(9), 2, 5)
        .unwrap_or_else(|error| panic!("second count failed: {error:?}"));

    assert!(prepared.is_complete());
    assert_eq!(prepared.counts()[0].count(), 4);
    assert_eq!(prepared.counts()[1].count(), 2);
}

#[test]
fn changed_generation_discards_mixed_counts_and_restarts_from_first_topic() {
    let mut prepared = PreparedClassicGroupPartitionCounts::try_new(
        MembershipCycle::initial(),
        vec![TopicId::from_raw(3), TopicId::from_raw(9)],
        OperationDeadline::from_parts_for_test(Deadline::from_tick(91), Instant::now()),
    )
    .unwrap_or_else(|error| panic!("count owner failed: {error:?}"));
    prepared
        .append(TopicId::from_raw(3), 4, 5)
        .unwrap_or_else(|error| panic!("first count failed: {error:?}"));

    assert_eq!(
        prepared.append(TopicId::from_raw(9), 2, 6),
        Ok(super::classic_group_partition_counts::ClassicGroupPartitionCountProgress::Restarted)
    );
    assert_eq!(prepared.next_topic(), Some(TopicId::from_raw(3)));
    assert!(prepared.counts().is_empty());
    assert!(!prepared.is_complete());
}
