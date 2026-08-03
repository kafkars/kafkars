//! Pre-core capacity and uniqueness checks for each claim-bearing transition.

use kafka_client_core::AssignedTopicPartition;

use super::{
    super::{AssignedConsumerEventStore, AssignedConsumerEventStoreError},
    model::{PreparedEventClaims, PreparedKind},
};

impl AssignedConsumerEventStore {
    pub(crate) fn prepare_replacement(
        &mut self,
        partition_count: usize,
    ) -> Result<PreparedEventClaims<'_, 'static>, AssignedConsumerEventStoreError> {
        if self.ready.len().saturating_add(partition_count) > self.capacity {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Replacement(partition_count),
        })
    }

    pub(crate) fn prepare_reconciliation(
        &mut self,
        partition_count: usize,
    ) -> Result<PreparedEventClaims<'_, 'static>, AssignedConsumerEventStoreError> {
        if self.ready.len().saturating_add(partition_count) > self.capacity {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Reconciliation(partition_count),
        })
    }

    pub(crate) fn prepare_partition(
        &mut self,
        partition: AssignedTopicPartition,
    ) -> Result<PreparedEventClaims<'_, 'static>, AssignedConsumerEventStoreError> {
        let extra = usize::from(
            !self
                .claims
                .iter()
                .any(|claim| claim.partition() == partition),
        );
        if self
            .claims
            .len()
            .saturating_add(self.ready.len())
            .saturating_add(extra)
            > self.capacity
        {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Partition(partition),
        })
    }

    pub(crate) fn prepare_pause_partitions<'store, 'input>(
        &'store mut self,
        partitions: &'input [AssignedTopicPartition],
    ) -> Result<PreparedEventClaims<'store, 'input>, AssignedConsumerEventStoreError> {
        ensure_unique(partitions)?;
        if self.claims.len().saturating_add(self.ready.len()) > self.capacity {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Pause(partitions),
        })
    }

    pub(crate) fn prepare_resume_partitions<'store, 'input>(
        &'store mut self,
        partitions: &'input [AssignedTopicPartition],
    ) -> Result<PreparedEventClaims<'store, 'input>, AssignedConsumerEventStoreError> {
        ensure_unique(partitions)?;
        let extra = partitions
            .iter()
            .filter(|partition| {
                !self
                    .claims
                    .iter()
                    .any(|claim| claim.partition() == **partition)
            })
            .count();
        if self
            .claims
            .len()
            .saturating_add(self.ready.len())
            .saturating_add(extra)
            > self.capacity
        {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Resume(partitions),
        })
    }
}

fn ensure_unique(
    partitions: &[AssignedTopicPartition],
) -> Result<(), AssignedConsumerEventStoreError> {
    if partitions
        .iter()
        .enumerate()
        .any(|(index, partition)| partitions[..index].contains(partition))
    {
        Err(AssignedConsumerEventStoreError::TransitionMismatch)
    } else {
        Ok(())
    }
}
