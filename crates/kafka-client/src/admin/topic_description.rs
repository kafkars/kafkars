//! Stable Rust topic and partition descriptions.

use crate::KafkaError;

/// One bounded partition description returned by Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionDescription {
    partition_index: i32,
    error: Option<KafkaError>,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    in_sync_replicas: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl TopicPartitionDescription {
    pub(crate) const fn new(
        partition_index: i32,
        error: Option<KafkaError>,
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

    /// Returns the partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns Kafka's exact partition error, when present.
    pub const fn error(&self) -> Option<&KafkaError> {
        self.error.as_ref()
    }

    /// Returns the current leader broker, when known.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the current leader epoch, when the negotiated version provides it.
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
}

/// One bounded topic description returned by Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    name: String,
    topic_id: Option<[u8; 16]>,
    internal: bool,
    partitions: Vec<TopicPartitionDescription>,
}

impl TopicDescription {
    pub(crate) const fn new(
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

    /// Returns the topic name.
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
}
