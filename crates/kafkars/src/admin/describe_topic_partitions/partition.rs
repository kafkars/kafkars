//! Stable API-key 75 partition entry with nullable leader and ELR facts.

use crate::KafkaError;

/// One partition description returned within an explicit response page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartition {
    error: Option<KafkaError>,
    partition_index: i32,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    in_sync_replicas: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_eligible_leader_replicas: Option<Vec<i32>>,
    offline_replicas: Vec<i32>,
}

impl DescribeTopicPartition {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        error: Option<KafkaError>,
        partition_index: i32,
        leader_id: Option<i32>,
        leader_epoch: Option<i32>,
        replicas: Vec<i32>,
        in_sync_replicas: Vec<i32>,
        eligible_leader_replicas: Option<Vec<i32>>,
        last_known_eligible_leader_replicas: Option<Vec<i32>>,
        offline_replicas: Vec<i32>,
    ) -> Self {
        Self {
            error,
            partition_index,
            leader_id,
            leader_epoch,
            replicas,
            in_sync_replicas,
            eligible_leader_replicas,
            last_known_eligible_leader_replicas,
            offline_replicas,
        }
    }

    /// Returns Kafka's partition-scoped error with its exact signed code.
    pub const fn error(&self) -> Option<&KafkaError> {
        self.error.as_ref()
    }

    /// Returns the nonnegative partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the current leader broker after sentinel normalization.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the current leader epoch after sentinel normalization.
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

    /// Returns the nullable eligible-leader replica list in Kafka order.
    pub fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    /// Returns the nullable last-known eligible-leader list in Kafka order.
    pub fn last_known_eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.last_known_eligible_leader_replicas.as_deref()
    }

    /// Returns offline replica broker identities in Kafka order.
    pub fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }
}
