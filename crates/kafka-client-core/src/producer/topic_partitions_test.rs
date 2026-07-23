//! Scenarios for allocation-free access to lazy normalized topic facts.

use core::cell::Cell;

use super::{
    AvailablePartition, PartitionCount, TopicMetadataGeneration, TopicPartitionFacts,
    TopicPartitionFactsError, TopicPartitionSource,
};
use crate::PartitionIndex;

#[test]
fn facts_borrow_a_lazy_source_without_copying_a_topic_cache() {
    let available = [partition(0), partition(2)];
    let generation = Cell::new(7);
    let logical_count = Cell::new(count(4));
    let available_len = Cell::new(2);
    let source = MutableScalarSource {
        generation: &generation,
        logical_count: &logical_count,
        available_len: &available_len,
        available: &available,
    };
    let facts = TopicPartitionFacts::new(&source);

    assert_eq!(facts.generation(), generation_value(7));
    generation.set(8);
    logical_count.set(count(1));
    available_len.set(0);
    assert_eq!(facts.generation(), generation_value(7));
    assert_eq!(facts.logical_count(), count(4));
    assert_eq!(facts.available_len(), 2);
    assert_eq!(facts.available_at(1), Ok(partition(2)));
    assert_eq!(facts.find_available(index(1)), Ok(None));
}

#[test]
fn missing_index_below_captured_source_length_is_rejected_at_use() {
    let missing = MissingIndexSource;
    let facts = TopicPartitionFacts::new(&missing);
    assert_eq!(
        facts.available_at(0),
        Err(TopicPartitionFactsError::AvailableIndexMissing {
            index: 0,
            declared_len: 1,
        })
    );
}

#[test]
fn source_partitions_must_fit_the_total_logical_count() {
    let available = [partition(3)];
    let source = TestTopicSource::new(generation_value(1), count(3), &available);
    let facts = source.facts();

    assert_eq!(
        facts.available_at(0),
        Err(TopicPartitionFactsError::PartitionOutsideTopic {
            partition: index(3),
            logical_count: count(3),
        })
    );
}

pub(super) struct TestTopicSource<'a> {
    generation: TopicMetadataGeneration,
    logical_count: PartitionCount,
    available: &'a [AvailablePartition],
}

impl<'a> TestTopicSource<'a> {
    pub(super) const fn new(
        generation: TopicMetadataGeneration,
        logical_count: PartitionCount,
        available: &'a [AvailablePartition],
    ) -> Self {
        Self {
            generation,
            logical_count,
            available,
        }
    }

    pub(super) fn facts(&self) -> TopicPartitionFacts<'_> {
        TopicPartitionFacts::new(self)
    }
}

impl TopicPartitionSource for TestTopicSource<'_> {
    fn generation(&self) -> TopicMetadataGeneration {
        self.generation
    }

    fn logical_count(&self) -> PartitionCount {
        self.logical_count
    }

    fn available_len(&self) -> usize {
        self.available.len()
    }

    fn available_at(&self, index: usize) -> Option<AvailablePartition> {
        self.available.get(index).copied()
    }
}

struct MutableScalarSource<'a> {
    generation: &'a Cell<u64>,
    logical_count: &'a Cell<PartitionCount>,
    available_len: &'a Cell<usize>,
    available: &'a [AvailablePartition],
}

impl TopicPartitionSource for MutableScalarSource<'_> {
    fn generation(&self) -> TopicMetadataGeneration {
        generation_value(self.generation.get())
    }

    fn logical_count(&self) -> PartitionCount {
        self.logical_count.get()
    }

    fn available_len(&self) -> usize {
        self.available_len.get()
    }

    fn available_at(&self, index: usize) -> Option<AvailablePartition> {
        self.available.get(index).copied()
    }
}

struct MissingIndexSource;

impl TopicPartitionSource for MissingIndexSource {
    fn generation(&self) -> TopicMetadataGeneration {
        generation_value(1)
    }

    fn logical_count(&self) -> PartitionCount {
        count(1)
    }

    fn available_len(&self) -> usize {
        1
    }

    fn available_at(&self, _index: usize) -> Option<AvailablePartition> {
        None
    }
}

pub(super) const fn generation_value(value: u64) -> TopicMetadataGeneration {
    TopicMetadataGeneration::from_raw(value)
}

pub(super) fn count(value: u32) -> PartitionCount {
    PartitionCount::try_from_raw(value)
        .unwrap_or_else(|| panic!("test partition count must be Java-representable"))
}

pub(super) const fn index(value: u32) -> PartitionIndex {
    PartitionIndex::from_raw(value)
}

pub(super) const fn partition(value: u32) -> AvailablePartition {
    AvailablePartition::new(index(value), None)
}
