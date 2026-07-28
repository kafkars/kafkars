//! Engine-owned canonical request intent for partition reassignments.

use core::mem::size_of;

use kafka_client_core::{
    AlterPartitionReassignment as CoreChange, AlterPartitionReassignmentsPlan,
    AlterPartitionReassignmentsPlanError, PartitionReassignmentTarget,
};

/// One raw caller-ordered replacement or explicit cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionReassignmentChange {
    topic: String,
    partition: i32,
    replicas: Option<Vec<i32>>,
}

impl PartitionReassignmentChange {
    /// Creates one ordered replacement replica placement.
    pub fn replace(topic: String, partition: i32, replicas: Vec<i32>) -> Self {
        Self {
            topic,
            partition,
            replicas: Some(replicas),
        }
    }

    /// Creates one explicit cancellation.
    pub const fn cancel(topic: String, partition: i32) -> Self {
        Self {
            topic,
            partition,
            replicas: None,
        }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = canonical_string(self.topic);
        self.replicas = self.replicas.map(canonical_vec);
        self
    }

    fn into_core(self) -> CoreChange {
        let target = match self.replicas {
            Some(replicas) => PartitionReassignmentTarget::Replicas(replicas),
            None => PartitionReassignmentTarget::Cancel,
        };
        CoreChange::new(self.topic, self.partition, target)
    }
}

/// One nonempty caller-ordered reassignment alteration batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterPartitionReassignmentsRequest {
    changes: Vec<PartitionReassignmentChange>,
    allow_replication_factor_change: bool,
}

impl AlterPartitionReassignmentsRequest {
    /// Creates one inert request for validation at the public call boundary.
    pub const fn new(changes: Vec<PartitionReassignmentChange>) -> Self {
        Self {
            changes,
            allow_replication_factor_change: true,
        }
    }

    /// Replaces whether Kafka may change a partition's replication factor.
    pub const fn with_allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.changes = canonical_vec(
            self.changes
                .into_iter()
                .map(PartitionReassignmentChange::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn preparation_charge(&self) -> Option<usize> {
        self.changes.iter().try_fold(
            size_of::<Self>().checked_add(
                self.changes
                    .len()
                    .checked_mul(size_of::<PartitionReassignmentChange>())?,
            )?,
            |bytes, change| {
                bytes.checked_add(change.topic.len())?.checked_add(
                    change
                        .replicas
                        .as_ref()
                        .map_or(0, Vec::len)
                        .checked_mul(size_of::<i32>())?,
                )
            },
        )
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AlterPartitionReassignmentsPlan, AlterPartitionReassignmentsPlanError> {
        AlterPartitionReassignmentsPlan::new(
            self.changes
                .into_iter()
                .map(PartitionReassignmentChange::into_core)
                .collect(),
        )
        .map(|plan| plan.with_allow_replication_factor_change(self.allow_replication_factor_change))
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
