//! Engine-owned request values for automatic or explicit `CreatePartitions`.

use kafka_client_core::{
    CreatePartitionsPlan, CreatePartitionsPlanError,
    CreatePartitionsSpecification as CoreSpecification,
};

/// One topic and its requested new total partition count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionIncrease {
    topic: String,
    total_count: i32,
    replica_assignments: Option<Vec<Vec<i32>>>,
}

impl PartitionIncrease {
    /// Creates one broker-assigned partition increase.
    pub fn new(topic: impl Into<String>, total_count: i32) -> Self {
        Self {
            topic: topic.into(),
            total_count,
            replica_assignments: None,
        }
    }

    /// Creates one increase with assignments for every new partition.
    pub fn with_replica_assignments(
        topic: impl Into<String>,
        total_count: i32,
        replica_assignments: Vec<Vec<i32>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            total_count,
            replica_assignments: Some(replica_assignments),
        }
    }
}

/// One ordered batch-native `CreatePartitions` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePartitionsRequest {
    topics: Vec<PartitionIncrease>,
    validate_only: bool,
}

impl CreatePartitionsRequest {
    /// Creates one ordered partition-increase batch.
    pub const fn new(topics: Vec<PartitionIncrease>) -> Self {
        Self {
            topics,
            validate_only: false,
        }
    }

    /// Selects validation without broker mutation.
    #[must_use]
    pub const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(crate) fn into_plan(self) -> Result<CreatePartitionsPlan, CreatePartitionsPlanError> {
        CreatePartitionsPlan::new(
            self.topics
                .into_iter()
                .map(|topic| match topic.replica_assignments {
                    Some(assignments) => CoreSpecification::with_replica_assignments(
                        topic.topic,
                        topic.total_count,
                        assignments,
                    ),
                    None => CoreSpecification::new(topic.topic, topic.total_count),
                })
                .collect(),
            self.validate_only,
        )
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for topic in &mut self.topics {
            topic.topic = canonical_string(std::mem::take(&mut topic.topic));
            if let Some(assignments) = &mut topic.replica_assignments {
                for broker_ids in assignments.iter_mut() {
                    *broker_ids = canonical_vec(std::mem::take(broker_ids));
                }
                *assignments = canonical_vec(std::mem::take(assignments));
            }
        }
        self.topics = canonical_vec(self.topics);
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        let mut text_bytes = 0usize;
        let mut assignment_count = 0usize;
        let mut broker_id_count = 0usize;
        for topic in &self.topics {
            text_bytes = text_bytes.checked_add(topic.topic.len())?;
            if let Some(assignments) = &topic.replica_assignments {
                assignment_count = assignment_count.checked_add(assignments.len())?;
                for broker_ids in assignments {
                    broker_id_count = broker_id_count.checked_add(broker_ids.len())?;
                }
            }
        }
        crate::admin::retention::request_with_assignments_charge(
            self.topics.len(),
            0,
            assignment_count,
            broker_id_count,
            text_bytes,
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topics.capacity() == self.topics.len()
            && self.topics.iter().all(|topic| {
                topic.topic.capacity() == topic.topic.len()
                    && topic
                        .replica_assignments
                        .as_ref()
                        .is_none_or(|assignments| {
                            assignments.capacity() == assignments.len()
                                && assignments
                                    .iter()
                                    .all(|broker_ids| broker_ids.capacity() == broker_ids.len())
                        })
            })
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
