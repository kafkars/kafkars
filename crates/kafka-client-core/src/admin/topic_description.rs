//! Protocol-normalized topic and partition description facts.

use core::num::NonZeroI16;

/// Stable description of one topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionDescription {
    partition_index: i32,
    error: Option<NonZeroI16>,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    in_sync_replicas: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl TopicPartitionDescription {
    /// Creates one protocol-normalized partition description.
    pub const fn new(
        partition_index: i32,
        error: Option<NonZeroI16>,
        leader_id: Option<i32>,
        leader_epoch: Option<i32>,
        replicas: Vec<i32>,
        in_sync_replicas: Vec<i32>,
        offline_replicas: Vec<i32>,
    ) -> Self {
        Self {
            partition_index,
            error,
            leader_id,
            leader_epoch,
            replicas,
            in_sync_replicas,
            offline_replicas,
        }
    }

    /// Returns the nonnegative Kafka partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns Kafka's exact signed partition error code, when nonzero.
    pub const fn error_code(&self) -> Option<i16> {
        match self.error {
            Some(code) => Some(code.get()),
            None => None,
        }
    }

    /// Returns the current leader broker, when Kafka knows one.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the current leader epoch, when present in the negotiated version.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns replica broker identities in Kafka order.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Returns in-sync replica broker identities in Kafka order.
    pub fn in_sync_replicas(&self) -> &[i32] {
        &self.in_sync_replicas
    }

    /// Returns offline replica broker identities in Kafka order.
    pub fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }

    /// Consumes the description into adapter-owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        Option<i16>,
        Option<i32>,
        Option<i32>,
        Vec<i32>,
        Vec<i32>,
        Vec<i32>,
    ) {
        (
            self.partition_index,
            self.error.map(NonZeroI16::get),
            self.leader_id,
            self.leader_epoch,
            self.replicas,
            self.in_sync_replicas,
            self.offline_replicas,
        )
    }
}

/// Stable description of one requested topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    name: String,
    topic_id: Option<[u8; 16]>,
    internal: bool,
    partitions: Vec<TopicPartitionDescription>,
}

impl TopicDescription {
    /// Creates one protocol-normalized topic description.
    pub const fn new(
        name: String,
        topic_id: Option<[u8; 16]>,
        internal: bool,
        partitions: Vec<TopicPartitionDescription>,
    ) -> Self {
        Self {
            name,
            topic_id,
            internal,
            partitions,
        }
    }

    /// Returns the requested topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns Kafka's topic UUID bytes when provided by the negotiated version.
    pub const fn topic_id(&self) -> Option<[u8; 16]> {
        self.topic_id
    }

    /// Returns whether Kafka marks this as an internal topic.
    pub const fn is_internal(&self) -> bool {
        self.internal
    }

    /// Returns partitions in ascending partition-index order.
    pub fn partitions(&self) -> &[TopicPartitionDescription] {
        &self.partitions
    }

    /// Consumes the description into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<[u8; 16]>,
        bool,
        Vec<TopicPartitionDescription>,
    ) {
        (self.name, self.topic_id, self.internal, self.partitions)
    }
}
