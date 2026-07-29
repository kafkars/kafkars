//! One exact protocol-normalized API-key 75 partition description.

use std::collections::BTreeSet;

use super::DescribeTopicPartitionsValueError;

/// One partition with exact errors, leader facts, and ordered broker lists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartition {
    error_code: i16,
    partition_index: i32,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
    offline_replicas: Vec<i32>,
}

impl DescribeTopicPartition {
    /// Validates nonnegative scalars and duplicate-free ordered broker lists.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        error_code: i16,
        partition_index: i32,
        leader_id: Option<i32>,
        leader_epoch: Option<i32>,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        eligible_leader_replicas: Option<Vec<i32>>,
        last_known_elr: Option<Vec<i32>>,
        offline_replicas: Vec<i32>,
    ) -> Result<Self, DescribeTopicPartitionsValueError> {
        if partition_index < 0 {
            return Err(DescribeTopicPartitionsValueError::NegativePartition);
        }
        if leader_id.is_some_and(|value| value < 0) {
            return Err(DescribeTopicPartitionsValueError::NegativeLeaderId);
        }
        if leader_epoch.is_some_and(|value| value < 0) {
            return Err(DescribeTopicPartitionsValueError::NegativeLeaderEpoch);
        }
        for brokers in [
            Some(replicas.as_slice()),
            Some(isr.as_slice()),
            eligible_leader_replicas.as_deref(),
            last_known_elr.as_deref(),
            Some(offline_replicas.as_slice()),
        ]
        .into_iter()
        .flatten()
        {
            validate_brokers(brokers)?;
        }
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replicas,
            isr,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
        })
    }

    /// Returns Kafka's exact signed partition error code.
    pub const fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the nonnegative partition index.
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the leader after protocol sentinel normalization.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the leader epoch after protocol sentinel normalization.
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

    /// Returns the total broker references retained by this partition.
    pub(crate) fn broker_reference_count(&self) -> Option<usize> {
        self.replicas
            .len()
            .checked_add(self.isr.len())?
            .checked_add(self.eligible_leader_replicas.as_ref().map_or(0, Vec::len))?
            .checked_add(self.last_known_elr.as_ref().map_or(0, Vec::len))?
            .checked_add(self.offline_replicas.len())
    }

    /// Consumes all exact partition facts into adapter-owned parts.
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

fn validate_brokers(brokers: &[i32]) -> Result<(), DescribeTopicPartitionsValueError> {
    let mut identities = BTreeSet::new();
    for broker in brokers.iter().copied() {
        if broker < 0 {
            return Err(DescribeTopicPartitionsValueError::NegativeBrokerId);
        }
        if !identities.insert(broker) {
            return Err(DescribeTopicPartitionsValueError::DuplicateBrokerId);
        }
    }
    Ok(())
}
