//! Declarative facade for exact driver-owned immutable topic-view adapters.

mod partition_count;
#[cfg(test)]
mod partition_count_test;

pub(crate) use partition_count::{
    TopicPartitionCountAdmissionFailure, TopicPartitionCountAdmissionFailureKind,
    TopicPartitionCountCall, TopicPartitionCountFact, TopicPartitionCountFailure,
};
