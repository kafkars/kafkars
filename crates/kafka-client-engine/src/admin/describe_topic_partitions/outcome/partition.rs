//! Stable engine-owned partition facts for one explicit page.

/// One exact partition entry from the explicit broker page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartition {
    pub(super) error_code: i16,
    pub(super) partition_index: i32,
    pub(super) leader_id: Option<i32>,
    pub(super) leader_epoch: Option<i32>,
    pub(super) replicas: Vec<i32>,
    pub(super) isr: Vec<i32>,
    pub(super) eligible_leader_replicas: Option<Vec<i32>>,
    pub(super) last_known_elr: Option<Vec<i32>>,
    pub(super) offline_replicas: Vec<i32>,
}

impl AdminDescribeTopicPartition {
    /// Returns Kafka's exact signed partition error.
    pub const fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the nonnegative partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the leader after sentinel normalization.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the leader epoch after sentinel normalization.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns ordered replica broker IDs.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Returns ordered in-sync replica broker IDs.
    pub fn isr(&self) -> &[i32] {
        &self.isr
    }

    /// Returns nullable ordered eligible-leader replica IDs.
    pub fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    /// Returns nullable ordered last-known eligible-leader replica IDs.
    pub fn last_known_elr(&self) -> Option<&[i32]> {
        self.last_known_elr.as_deref()
    }

    /// Returns ordered offline replica broker IDs.
    pub fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }

    /// Consumes every exact partition fact into stable owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i16,
        i32,
        Option<i32>,
        Option<i32>,
        Vec<i32>,
        Vec<i32>,
        Option<Vec<i32>>,
        Option<Vec<i32>>,
        Vec<i32>,
    ) {
        (
            self.error_code,
            self.partition_index,
            self.leader_id,
            self.leader_epoch,
            self.replicas,
            self.isr,
            self.eligible_leader_replicas,
            self.last_known_elr,
            self.offline_replicas,
        )
    }
}
