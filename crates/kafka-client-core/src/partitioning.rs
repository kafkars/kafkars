//! Curated deterministic producer partition-selection vocabulary.

pub use crate::producer::{
    AvailablePartition, KeyedPartitionError, LeaderEpoch, LeaderEpochError, PartitionCount,
    PartitionSelection, StickyPartitionError, StickyPartitioner, TopicMetadataGeneration,
    TopicPartitionFacts, TopicPartitionFactsError, TopicPartitionSource,
    select_java_keyed_partition, select_java_keyed_topic_partition,
};
