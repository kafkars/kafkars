//! Borrowed reassignment intent and bounded generated-free response facts.

use kafka_client_core::{AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentsBatch};

/// One caller-owned change borrowed during protocol adaptation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AlterPartitionReassignmentRef<'a> {
    topic: &'a str,
    partition: i32,
    replicas: Option<&'a [i32]>,
}

impl<'a> AlterPartitionReassignmentRef<'a> {
    pub(crate) const fn new(topic: &'a str, partition: i32, replicas: Option<&'a [i32]>) -> Self {
        Self {
            topic,
            partition,
            replicas,
        }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn replicas(self) -> Option<&'a [i32]> {
        self.replicas
    }
}

/// Validated response facts safe to apply to deterministic policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValidatedAlterPartitionReassignmentsResponse {
    /// Exact top-level broker rejection and bounded diagnostic.
    BrokerRejected(AlterPartitionReassignmentBrokerError),
    /// Caller-ordered per-partition results plus throttle.
    Batch(AlterPartitionReassignmentsBatch),
}
