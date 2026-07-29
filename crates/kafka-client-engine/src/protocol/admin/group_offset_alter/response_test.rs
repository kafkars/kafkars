//! Caller-order, exact-code, hostile-shape, and retained-capacity scenarios.

use core::num::NonZeroI16;

use kafka_wire::{
    OffsetCommitResponse,
    offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
};

use super::{
    GroupOffsetAlterProtocolFailure, OffsetCommitPartitionResult, OffsetCommitTargetRef,
    response::validate_group_offset_alter_response,
};

#[test]
fn response_is_restored_to_caller_order_with_exact_signed_codes() {
    let targets = targets();
    let response = response(
        17,
        vec![
            topic("audit", vec![partition(1, -31_000)]),
            topic("orders", vec![partition(7, 0), partition(2, 9)]),
        ],
    );
    let validated = validate_group_offset_alter_response(&targets, None, &response, 9, usize::MAX)
        .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));
    assert_eq!(validated.throttle_time_ms(), 17);
    let entries = validated.entries();
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.topic(), entry.partition()))
            .collect::<Vec<_>>(),
        [("orders", 2), ("audit", 1), ("orders", 7)]
    );
    assert_eq!(
        entries[0].result(),
        OffsetCommitPartitionResult::Rejected {
            code: NonZeroI16::new(9).unwrap_or_else(|| panic!("nonzero code"))
        }
    );
    assert_eq!(
        entries[1].result(),
        OffsetCommitPartitionResult::Rejected {
            code: NonZeroI16::new(-31_000).unwrap_or_else(|| panic!("nonzero code"))
        }
    );
    assert_eq!(entries[2].result(), OffsetCommitPartitionResult::Altered);
}

#[test]
fn selected_version_respects_v2_floor_v9_ceiling_and_epoch_floor() {
    let ordinary = [target("orders", 2, None)];
    let response = response(0, vec![topic("orders", vec![partition(2, 0)])]);
    for (actual, minimum) in [(1, 2), (10, 2)] {
        assert_eq!(
            validate_group_offset_alter_response(&ordinary, None, &response, actual, usize::MAX,)
                .err(),
            Some(GroupOffsetAlterProtocolFailure::UnsupportedApiVersion {
                minimum,
                maximum: 9,
                actual,
            })
        );
    }

    let epoch = [target("orders", 2, Some(7))];
    assert_eq!(
        validate_group_offset_alter_response(&epoch, None, &response, 5, usize::MAX).err(),
        Some(GroupOffsetAlterProtocolFailure::UnsupportedApiVersion {
            minimum: 6,
            maximum: 9,
            actual: 5,
        })
    );
}

#[test]
fn explicit_retention_rejects_a_response_selected_above_v4() {
    let targets = [target("orders", 2, None)];
    let response = response(0, vec![topic("orders", vec![partition(2, 0)])]);

    assert_eq!(
        validate_group_offset_alter_response(&targets, Some(86_400_000), &response, 5, usize::MAX,)
            .err(),
        Some(GroupOffsetAlterProtocolFailure::UnsupportedApiVersion {
            minimum: 2,
            maximum: 4,
            actual: 5,
        })
    );
}

#[test]
fn throttle_is_absent_in_v2_and_nonnegative_after_v2() {
    let targets = [target("orders", 2, None)];
    let negative = response(-1, vec![topic("orders", vec![partition(2, 0)])]);
    let v2 = validate_group_offset_alter_response(&targets, None, &negative, 2, usize::MAX)
        .unwrap_or_else(|error| panic!("v2 has no throttle field: {error:?}"));
    assert_eq!(v2.throttle_time_ms(), 0);
    assert_eq!(
        validate_group_offset_alter_response(&targets, None, &negative, 3, usize::MAX).err(),
        Some(GroupOffsetAlterProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn ambiguous_or_unexpected_response_targets_never_correlate() {
    let duplicate_topic = response(
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("orders", vec![partition(2, 0)]),
        ],
    );
    assert_eq!(
        validate_group_offset_alter_response(&targets(), None, &duplicate_topic, 9, usize::MAX,)
            .err(),
        Some(GroupOffsetAlterProtocolFailure::DuplicateTopic)
    );

    let duplicate_partition = response(
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(2, 0)]),
            topic("audit", vec![partition(1, 0)]),
        ],
    );
    assert_eq!(
        validate_group_offset_alter_response(
            &targets(),
            None,
            &duplicate_partition,
            9,
            usize::MAX,
        )
        .err(),
        Some(GroupOffsetAlterProtocolFailure::DuplicatePartition { actual: 2 })
    );

    let unexpected = response(
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("audit", vec![partition(99, 0)]),
        ],
    );
    assert_eq!(
        validate_group_offset_alter_response(&targets(), None, &unexpected, 9, usize::MAX).err(),
        Some(GroupOffsetAlterProtocolFailure::MissingPartition { actual: 1 })
    );
}

#[test]
fn hostile_excess_partitions_are_rejected_before_correlation_allocation() {
    let response = response(
        0,
        vec![topic(
            "orders",
            vec![
                partition(2, 0),
                partition(7, 0),
                partition(8, 0),
                partition(9, 0),
            ],
        )],
    );
    assert_eq!(
        validate_group_offset_alter_response(&targets(), None, &response, 9, usize::MAX).err(),
        Some(GroupOffsetAlterProtocolFailure::PartitionCount {
            expected: 3,
            actual: 4,
        })
    );
}

#[test]
fn complete_future_allocation_charge_must_fit_before_correlation() {
    let response = response(
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("audit", vec![partition(1, 0)]),
        ],
    );
    let validated =
        validate_group_offset_alter_response(&targets(), None, &response, 9, usize::MAX)
            .unwrap_or_else(|error| panic!("charge can be measured: {error:?}"));
    assert_eq!(
        validate_group_offset_alter_response(
            &targets(),
            None,
            &response,
            9,
            validated.retained_charge() - 1,
        )
        .err(),
        Some(GroupOffsetAlterProtocolFailure::RetainedBytes)
    );
}

fn targets() -> [OffsetCommitTargetRef<'static>; 3] {
    [
        target("orders", 2, Some(7)),
        target("audit", 1, None),
        target("orders", 7, None),
    ]
}

fn target(
    topic: &'static str,
    partition: i32,
    leader_epoch: Option<i32>,
) -> OffsetCommitTargetRef<'static> {
    OffsetCommitTargetRef::new(topic, partition, 1, leader_epoch, None)
}

fn partition(partition_index: i32, error_code: i16) -> OffsetCommitResponsePartition {
    let mut partition = OffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}

fn topic(name: &str, partitions: Vec<OffsetCommitResponsePartition>) -> OffsetCommitResponseTopic {
    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn response(throttle_time_ms: i32, topics: Vec<OffsetCommitResponseTopic>) -> OffsetCommitResponse {
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = topics;
    response
}
