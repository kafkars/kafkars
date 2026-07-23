//! Atomic producer admission, retained capacity, and terminal settlement.

mod batch;
mod batch_retry;
#[cfg(test)]
mod batch_retry_test;
mod batch_revision;
mod batch_timer;
mod batch_transition;
mod batching;
mod cancellation;
mod close_transition;
mod execution_stop;
#[cfg(test)]
mod execution_stop_capacity_test;
mod flush;
mod flush_transition;
mod input_batch;
mod input_outcome;
mod lifecycle;
mod machine;
mod partitioner;
mod retry;
#[cfg(test)]
mod retry_cancellation_test;
#[cfg(test)]
mod retry_safety_test;
#[cfg(test)]
mod retry_test;
#[cfg(test)]
mod retry_test_support;
mod settlement;
mod sticky;
mod topic_partition;
mod topic_partitions;

pub(crate) use batch::{
    BatchAccumulation, BatchMember, BatchRemoval, BatchRevision, BatchRoute, BatchSeal, BatchState,
    BatchTimerObservation, ProducerBatch,
};
pub(crate) use flush::FlushLedger;
pub use flush::{AdmissionSequence, FlushId, FlushLedgerError};
pub use machine::ProducerMachine;
pub use partitioner::select_java_keyed_topic_partition;
pub use partitioner::{KeyedPartitionError, PartitionCount, select_java_keyed_partition};
pub use sticky::{StickyPartitionError, StickyPartitioner};
pub use topic_partition::{
    AvailablePartition, LeaderEpoch, LeaderEpochError, PartitionSelection, TopicMetadataGeneration,
};
pub use topic_partitions::{TopicPartitionFacts, TopicPartitionFactsError, TopicPartitionSource};

#[cfg(test)]
mod cancellation_order_test;
#[cfg(test)]
mod cancellation_revision_test;
#[cfg(test)]
mod cancellation_test;
#[cfg(test)]
mod close_transition_test;
#[cfg(test)]
mod execution_stop_test;
#[cfg(test)]
mod flush_test;
#[cfg(test)]
mod flush_transition_test;
#[cfg(test)]
mod input_outcome_test;
#[cfg(test)]
mod partitioner_test;
#[cfg(test)]
mod sticky_test;
#[cfg(test)]
mod topic_partition_test;
#[cfg(test)]
mod topic_partitions_test;
