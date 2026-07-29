//! Borrowed leader-election intent and bounded generated-free response facts.

use kafka_client_core::{ElectLeadersBatch, LeaderElectionBrokerError, LeaderElectionType};

/// One caller-owned target borrowed during protocol adaptation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LeaderElectionRef<'a> {
    topic: &'a str,
    partition: i32,
}

impl<'a> LeaderElectionRef<'a> {
    pub(crate) const fn new(topic: &'a str, partition: i32) -> Self {
        Self { topic, partition }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }
}

/// Kafka's nullable partition selection without conflating empty with all partitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersSelectionRef<'a> {
    AllPartitions,
    Selected(&'a [LeaderElectionRef<'a>]),
}

pub(crate) const fn election_type_code(election_type: LeaderElectionType) -> i8 {
    match election_type {
        LeaderElectionType::Preferred => 0,
        LeaderElectionType::Unclean => 1,
    }
}

/// Validated response facts safe to apply to deterministic policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValidatedElectLeadersResponse {
    /// Exact top-level broker rejection.
    BrokerRejected(LeaderElectionBrokerError),
    /// Caller-ordered per-partition results plus throttle.
    Batch(ElectLeadersBatch),
}
