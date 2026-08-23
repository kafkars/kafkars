//! Declarative facade for exact driver-owned immutable topic-view adapters.

mod partition_count;
#[cfg(test)]
mod partition_count_test;
mod producer;

pub(crate) use partition_count::{
    TopicPartitionCountAdmissionFailure, TopicPartitionCountAdmissionFailureKind,
    TopicPartitionCountCall, TopicPartitionCountFact, TopicPartitionCountFailure,
};
pub(crate) use producer::{TopicRouteView, TopicRouteViewCall};
