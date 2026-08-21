//! Prepared request ownership for the public Admin bridge.

use kafka_client_engine::{
    CreatePartitionsRequest as EnginePartitionsRequest, CreateTopic as EngineTopic,
    CreateTopicConfig as EngineTopicConfig, CreateTopicReplicaAssignment as EngineAssignment,
    CreateTopicsRequest as EngineRequest, DeleteTopicsRequest as EngineDeleteRequest,
    PartitionIncrease as EnginePartitionIncrease,
};

use crate::admin::{NewPartitions, NewTopic, NewTopicPlacement};

/// Prepared engine request retained by an inert partition builder.
pub(crate) struct PartitionsAdminRequest {
    pub(super) inner: EnginePartitionsRequest,
}

impl PartitionsAdminRequest {
    pub(crate) fn from_topics<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = NewPartitions>,
    {
        Self {
            inner: EnginePartitionsRequest::new(
                topics.into_iter().map(into_engine_partitions).collect(),
            ),
        }
    }

    pub(crate) fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.inner = self.inner.with_validate_only(validate_only);
        self
    }
}

impl std::fmt::Debug for PartitionsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartitionsAdminRequest")
            .finish_non_exhaustive()
    }
}

/// Prepared engine request retained by an inert deletion builder.
pub(crate) struct DeleteAdminRequest {
    pub(super) inner: EngineDeleteRequest,
}

impl DeleteAdminRequest {
    pub(crate) fn from_topics<I, T>(topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            inner: EngineDeleteRequest::new(topics.into_iter().map(Into::into).collect()),
        }
    }

    pub(crate) fn from_topic_ids<I>(topic_ids: I) -> Self
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        Self {
            inner: EngineDeleteRequest::by_ids(topic_ids.into_iter().collect()),
        }
    }
}

impl std::fmt::Debug for DeleteAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteAdminRequest")
            .finish_non_exhaustive()
    }
}

/// Prepared engine request retained by an otherwise inert public builder.
pub(crate) struct AdminRequest {
    pub(super) inner: EngineRequest,
}

impl AdminRequest {
    pub(crate) fn from_topics<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = NewTopic>,
    {
        Self {
            inner: EngineRequest::new(topics.into_iter().map(into_engine_topic).collect()),
        }
    }

    pub(crate) fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.inner = self.inner.with_validate_only(validate_only);
        self
    }
}

impl std::fmt::Debug for AdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminRequest")
            .finish_non_exhaustive()
    }
}

fn into_engine_topic(topic: NewTopic) -> EngineTopic {
    let (name, placement, mixed_replication_factor, configs) = topic.into_parts();
    let topic = match placement {
        NewTopicPlacement::Automatic {
            partitions,
            replication_factor,
        } => EngineTopic::new(name, partitions).with_replication_factor(replication_factor),
        NewTopicPlacement::Manual { assignments } => EngineTopic::with_replica_assignments(
            name,
            assignments
                .into_iter()
                .map(|assignment| {
                    let (partition_index, broker_ids) = assignment.into_parts();
                    EngineAssignment::new(partition_index, broker_ids)
                })
                .collect(),
            mixed_replication_factor,
        ),
    };
    configs.into_iter().fold(topic, |topic, (name, value)| {
        topic.with_config(EngineTopicConfig::new(name, Some(value)))
    })
}

fn into_engine_partitions(topic: NewPartitions) -> EnginePartitionIncrease {
    let (name, total_count, replica_assignments) = topic.into_parts();
    match replica_assignments {
        Some(assignments) => {
            EnginePartitionIncrease::with_replica_assignments(name, total_count, assignments)
        }
        None => EnginePartitionIncrease::new(name, total_count),
    }
}
