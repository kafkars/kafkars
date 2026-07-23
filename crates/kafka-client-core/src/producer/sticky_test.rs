//! Scenarios for deterministic availability-aware sticky partition selection.

use core::cell::Cell;

use super::topic_partitions_test::TestTopicSource;
use super::{
    AvailablePartition, LeaderEpoch, PartitionCount, PartitionSelection, StickyPartitionError,
    StickyPartitioner, TopicMetadataGeneration, TopicPartitionSource,
};
use crate::PartitionIndex;

#[test]
fn repeated_unkeyed_selection_sticks_while_partition_remains_available() {
    let available = [partition(0, None), partition(2, epoch(3))];
    let first = source(4, &available);
    let later = TestTopicSource::new(generation(5), count(3), &available);
    let mut sticky = StickyPartitioner::new(1);

    assert_eq!(
        sticky
            .select(first.facts())
            .map(PartitionSelection::partition),
        Ok(index(2))
    );
    let selected = sticky
        .select(later.facts())
        .unwrap_or_else(|error| panic!("sticky partition remains available: {error}"));
    assert_eq!(selected.partition(), index(2));
    assert_eq!(selected.generation(), generation(5));
    assert_eq!(selected.leader_epoch(), epoch(3));
}

#[test]
fn sealed_batches_advance_over_available_partitions_in_stable_order() {
    let available = [partition(0, None), partition(2, None), partition(5, None)];
    let source = source(1, &available);
    let facts = source.facts();
    let mut sticky = StickyPartitioner::new(1);

    let first = sticky
        .select(facts)
        .unwrap_or_else(|error| panic!("first sticky selection: {error}"));
    sticky.batch_sealed();
    let second = sticky
        .select(facts)
        .unwrap_or_else(|error| panic!("second sticky selection: {error}"));
    sticky.batch_sealed();
    let third = sticky
        .select(facts)
        .unwrap_or_else(|error| panic!("third sticky selection: {error}"));

    assert_eq!(
        [first.partition(), second.partition(), third.partition()],
        [index(2), index(5), index(0)]
    );
}

#[test]
fn metadata_loss_reselects_without_waiting_for_the_old_batch_to_seal() {
    let initial_available = [partition(0, None), partition(1, None)];
    let replacement_available = [partition(0, None), partition(2, epoch(9))];
    let initial = source(1, &initial_available);
    let replacement = source(2, &replacement_available);
    let mut sticky = StickyPartitioner::new(1);
    let first = sticky
        .select(initial.facts())
        .unwrap_or_else(|error| panic!("initial sticky selection: {error}"));
    let selected_replacement = sticky
        .select(replacement.facts())
        .unwrap_or_else(|error| panic!("replacement sticky selection: {error}"));

    assert_eq!(first.partition(), index(1));
    assert_eq!(selected_replacement.partition(), index(2));
    assert_eq!(selected_replacement.generation(), generation(2));
    assert_eq!(selected_replacement.leader_epoch(), epoch(9));
}

#[test]
fn unavailable_topic_rejects_without_mutating_sticky_state() {
    let available = [partition(1, None), partition(3, None)];
    let empty = source(1, &[]);
    let recovered = source(2, &available);
    let mut sticky = StickyPartitioner::new(3);

    assert_eq!(
        sticky.select(empty.facts()),
        Err(StickyPartitionError::NoAvailablePartition)
    );
    assert_eq!(
        sticky
            .select(recovered.facts())
            .map(PartitionSelection::partition),
        Ok(index(3))
    );
}

#[test]
fn available_selection_uses_the_exact_indexed_fact_without_a_second_lookup() {
    let first_read = Cell::new(true);
    let source = OneShotAvailableSource {
        first_read: &first_read,
    };
    let mut sticky = StickyPartitioner::new(0);

    let selected = sticky
        .select(super::TopicPartitionFacts::new(&source))
        .unwrap_or_else(|error| panic!("first available fact must be sufficient: {error}"));
    assert_eq!(selected.partition(), index(1));
    assert!(selected.is_available());
    assert!(!first_read.get());
}

fn source(generation_value: u64, available: &[AvailablePartition]) -> TestTopicSource<'_> {
    TestTopicSource::new(generation(generation_value), count(6), available)
}

const fn generation(value: u64) -> TopicMetadataGeneration {
    TopicMetadataGeneration::from_raw(value)
}

fn count(value: u32) -> PartitionCount {
    PartitionCount::try_from_raw(value)
        .unwrap_or_else(|| panic!("test partition count must be Java-representable"))
}

const fn index(value: u32) -> PartitionIndex {
    PartitionIndex::from_raw(value)
}

fn epoch(value: i32) -> Option<LeaderEpoch> {
    LeaderEpoch::try_from_raw(value)
        .unwrap_or_else(|error| panic!("test leader epoch must be valid: {error}"))
}

const fn partition(value: u32, epoch: Option<LeaderEpoch>) -> AvailablePartition {
    AvailablePartition::new(index(value), epoch)
}

struct OneShotAvailableSource<'a> {
    first_read: &'a Cell<bool>,
}

impl TopicPartitionSource for OneShotAvailableSource<'_> {
    fn generation(&self) -> TopicMetadataGeneration {
        generation(1)
    }

    fn logical_count(&self) -> PartitionCount {
        count(2)
    }

    fn available_len(&self) -> usize {
        1
    }

    fn available_at(&self, index: usize) -> Option<AvailablePartition> {
        if index == 0 && self.first_read.replace(false) {
            Some(partition(1, None))
        } else {
            None
        }
    }
}
