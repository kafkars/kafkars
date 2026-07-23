//! Compatibility scenarios for deterministic keyed partition selection.

use super::partitioner::java_key_length;
use super::topic_partitions_test::TestTopicSource;
use super::{
    AvailablePartition, KeyedPartitionError, LeaderEpoch, PartitionCount, TopicMetadataGeneration,
    select_java_keyed_partition, select_java_keyed_topic_partition,
};
use crate::PartitionIndex;

#[test]
fn apache_java_golden_vectors_select_the_same_partitions() {
    let seven = count(7);
    let twelve = count(12);
    let java_max = count(i32::MAX.unsigned_abs());
    let vectors: &[(&[u8], u32, u32, u32)] = &[
        (b"", 275_646_681, 2, 9),
        (b"kafka", 1_348_980_580, 3, 4),
        ("café".as_bytes(), 789_476_274, 6, 6),
        ("😀".as_bytes(), 131_881_617, 0, 9),
        (&[0x00, 0xff, 0x80, 0x7f], 578_742_699, 3, 3),
        (&[0, 1, 2, 3], 1_916_244_640, 2, 4),
        (&[0, 1, 2, 3, 4], 230_989_574, 4, 2),
        (&[0, 1, 2, 3, 4, 5], 1_749_926_848, 5, 4),
        (&[0, 1, 2, 3, 4, 5, 6], 659_314_828, 4, 4),
    ];

    for &(key, expected_positive_hash, expected_seven, expected_twelve) in vectors {
        assert_eq!(
            select_java_keyed_partition(key, java_max),
            Ok(PartitionIndex::from_raw(expected_positive_hash))
        );
        assert_eq!(
            select_java_keyed_partition(key, seven),
            Ok(PartitionIndex::from_raw(expected_seven))
        );
        assert_eq!(
            select_java_keyed_partition(key, twelve),
            Ok(PartitionIndex::from_raw(expected_twelve))
        );
    }
}

#[test]
fn negative_java_hash_uses_sign_bit_masking_instead_of_absolute_value() {
    let java_signed_hash = -798_503_068_i32;
    let expected_positive = 1_348_980_580_u32;
    assert_eq!(
        u32::from_ne_bytes(java_signed_hash.to_ne_bytes()) & i32::MAX.unsigned_abs(),
        expected_positive
    );
    assert_eq!(
        select_java_keyed_partition(b"kafka", count(i32::MAX.unsigned_abs())),
        Ok(PartitionIndex::from_raw(expected_positive))
    );
}

#[test]
fn empty_present_key_is_hashed_and_single_partition_is_zero() {
    assert_eq!(
        select_java_keyed_partition(b"", count(12)),
        Ok(PartitionIndex::from_raw(9))
    );
    assert_eq!(
        select_java_keyed_partition(b"any serialized key", count(1)),
        Ok(PartitionIndex::from_raw(0))
    );
}

#[test]
fn partition_count_rejects_zero_and_values_outside_java_domain() {
    assert_eq!(PartitionCount::try_from_raw(0), None);
    assert_eq!(
        PartitionCount::try_from_raw(i32::MAX.unsigned_abs()).map(PartitionCount::get),
        Some(i32::MAX.unsigned_abs())
    );
    assert_eq!(
        PartitionCount::try_from_raw(i32::MAX.unsigned_abs() + 1),
        None
    );
}

#[test]
fn serialized_key_length_rejects_values_outside_java_array_domain() {
    let java_max = usize::try_from(i32::MAX.unsigned_abs())
        .unwrap_or_else(|_| panic!("the supported Rust targets represent Java array lengths"));
    assert_eq!(java_key_length(java_max), Ok(i32::MAX.unsigned_abs()));
    assert_eq!(
        java_key_length(java_max + 1),
        Err(KeyedPartitionError::KeyLengthUnrepresentable)
    );
}

#[test]
fn keyed_selection_uses_logical_count_instead_of_available_count() {
    let available = [
        AvailablePartition::new(PartitionIndex::from_raw(1), None),
        AvailablePartition::new(PartitionIndex::from_raw(7), None),
    ];
    let source = TestTopicSource::new(TopicMetadataGeneration::from_raw(21), count(12), &available);

    let selected = select_java_keyed_topic_partition(b"kafka", source.facts())
        .unwrap_or_else(|error| panic!("Java-compatible keyed selection: {error}"));
    assert_eq!(selected.partition(), PartitionIndex::from_raw(4));
    assert_eq!(selected.generation(), TopicMetadataGeneration::from_raw(21));
    assert!(!selected.is_available());
    assert_eq!(selected.leader_epoch(), None);
}

#[test]
fn keyed_selection_retains_available_leader_facts() {
    let available = [AvailablePartition::new(
        PartitionIndex::from_raw(9),
        epoch(14),
    )];
    let source = TestTopicSource::new(TopicMetadataGeneration::from_raw(22), count(12), &available);

    let selected = select_java_keyed_topic_partition(b"", source.facts())
        .unwrap_or_else(|error| panic!("empty present key remains keyed: {error}"));
    assert_eq!(selected.partition(), PartitionIndex::from_raw(9));
    assert!(selected.is_available());
    assert_eq!(selected.leader_epoch().map(LeaderEpoch::get), Some(14));
}

fn count(value: u32) -> PartitionCount {
    PartitionCount::try_from_raw(value)
        .unwrap_or_else(|| panic!("test partition count must be Java-representable"))
}

fn epoch(value: i32) -> Option<LeaderEpoch> {
    LeaderEpoch::try_from_raw(value)
        .unwrap_or_else(|error| panic!("test leader epoch must be valid: {error}"))
}
