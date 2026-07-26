//! Caller-order, exact-code, response-shape, and retained-capacity scenarios.

use core::num::NonZeroI16;

use kafka_wire::{
    OffsetDeleteResponse,
    offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
};

use super::{
    GroupOffsetDeleteProtocolFailure, OffsetDeletePartitionResult, OffsetDeleteTargetRef,
    validate_group_offset_delete_response,
};

#[test]
fn response_is_restored_to_caller_order_with_exact_signed_codes() {
    let targets = targets();
    let response = response(
        0,
        17,
        vec![
            topic("audit", vec![partition(1, -31_000)]),
            topic("orders", vec![partition(7, 0), partition(2, 9)]),
        ],
    );
    let validated = validate_group_offset_delete_response(&targets, &response, 0, usize::MAX)
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
        OffsetDeletePartitionResult::Rejected {
            code: NonZeroI16::new(9).unwrap_or_else(|| panic!("nonzero code"))
        }
    );
    assert_eq!(
        entries[1].result(),
        OffsetDeletePartitionResult::Rejected {
            code: NonZeroI16::new(-31_000).unwrap_or_else(|| panic!("nonzero code"))
        }
    );
    assert_eq!(entries[2].result(), OffsetDeletePartitionResult::Deleted);
}

#[test]
fn exact_top_level_error_wins_without_materializing_hostile_topics() {
    let response = response(-719, 11, vec![topic("", vec![partition(-4, -31_000)])]);
    let validated = validate_group_offset_delete_response(&targets(), &response, 0, 128)
        .unwrap_or_else(|error| panic!("top-level broker error: {error:?}"));
    assert_eq!(validated.top_level_error().map(NonZeroI16::get), Some(-719));
    assert!(validated.entries().is_empty());
}

#[test]
fn response_requires_exact_v0_and_nonnegative_throttle() {
    let version_response = response(0, 0, Vec::new());
    assert_eq!(
        validate_group_offset_delete_response(&[], &version_response, 1, usize::MAX).err(),
        Some(GroupOffsetDeleteProtocolFailure::UnsupportedApiVersion { actual: 1 })
    );
    let throttle_response = response(0, -1, Vec::new());
    assert_eq!(
        validate_group_offset_delete_response(&[], &throttle_response, 0, usize::MAX).err(),
        Some(GroupOffsetDeleteProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn ambiguous_or_unexpected_response_targets_never_correlate() {
    let duplicate_topic = response(
        0,
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("orders", vec![partition(2, 0)]),
        ],
    );
    assert_eq!(
        validate_group_offset_delete_response(&targets(), &duplicate_topic, 0, usize::MAX).err(),
        Some(GroupOffsetDeleteProtocolFailure::DuplicateTopic)
    );

    let duplicate_partition = response(
        0,
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(2, 0)]),
            topic("audit", vec![partition(1, 0)]),
        ],
    );
    assert_eq!(
        validate_group_offset_delete_response(&targets(), &duplicate_partition, 0, usize::MAX)
            .err(),
        Some(GroupOffsetDeleteProtocolFailure::DuplicatePartition { actual: 2 })
    );

    let unexpected = response(
        0,
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("audit", vec![partition(99, 0)]),
        ],
    );
    assert_eq!(
        validate_group_offset_delete_response(&targets(), &unexpected, 0, usize::MAX).err(),
        Some(GroupOffsetDeleteProtocolFailure::MissingPartition { actual: 1 })
    );
}

#[test]
fn complete_future_allocation_charge_must_fit_before_correlation() {
    let response = response(
        0,
        0,
        vec![
            topic("orders", vec![partition(2, 0), partition(7, 0)]),
            topic("audit", vec![partition(1, 0)]),
        ],
    );
    let validated = validate_group_offset_delete_response(&targets(), &response, 0, usize::MAX)
        .unwrap_or_else(|error| panic!("charge can be measured: {error:?}"));
    assert_eq!(
        validate_group_offset_delete_response(
            &targets(),
            &response,
            0,
            validated.retained_charge() - 1,
        )
        .err(),
        Some(GroupOffsetDeleteProtocolFailure::RetainedBytes)
    );
}

fn targets() -> [OffsetDeleteTargetRef<'static>; 3] {
    [
        OffsetDeleteTargetRef::new("orders", 2),
        OffsetDeleteTargetRef::new("audit", 1),
        OffsetDeleteTargetRef::new("orders", 7),
    ]
}

fn partition(partition_index: i32, error_code: i16) -> OffsetDeleteResponsePartition {
    let mut partition = OffsetDeleteResponsePartition::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}

fn topic(name: &str, partitions: Vec<OffsetDeleteResponsePartition>) -> OffsetDeleteResponseTopic {
    let mut topic = OffsetDeleteResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn response(
    error_code: i16,
    throttle_time_ms: i32,
    topics: Vec<OffsetDeleteResponseTopic>,
) -> OffsetDeleteResponse {
    let mut response = OffsetDeleteResponse::default();
    response.error_code = error_code;
    response.throttle_time_ms = throttle_time_ms;
    response.topics = topics;
    response
}
