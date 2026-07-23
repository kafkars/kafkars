//! Atomic producer admission, retained capacity, and terminal settlement.

mod batch;
mod batch_timer;
mod batch_transition;
mod batching;
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
mod settlement;

pub(crate) use batch::{
    BatchAccumulation, BatchMember, BatchRemoval, BatchRoute, BatchSeal, BatchState,
    BatchTimerObservation, ProducerBatch,
};
pub(crate) use flush::FlushLedger;
pub use flush::{AdmissionSequence, FlushId, FlushLedgerError};
pub use machine::ProducerMachine;
pub use partitioner::{KeyedPartitionError, PartitionCount, select_java_keyed_partition};

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
