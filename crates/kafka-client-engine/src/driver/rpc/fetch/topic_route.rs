//! Broker-issued topic identity, metadata generation, and partition leader for Fetch v16.

use kafka_client_core::partitioning::TopicMetadataGeneration;

/// Immutable topic metadata retained by one prepared broker-routed Fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchTopicRoute {
    topic_id: [u8; 16],
    leader_epoch: Option<i32>,
    metadata_generation: Option<TopicMetadataGeneration>,
}

impl FetchTopicRoute {
    pub(crate) const fn new(topic_id: [u8; 16], leader_epoch: Option<i32>) -> Self {
        Self {
            topic_id,
            leader_epoch,
            metadata_generation: None,
        }
    }

    pub(crate) const fn observed(
        topic_id: [u8; 16],
        leader_epoch: Option<i32>,
        metadata_generation: TopicMetadataGeneration,
    ) -> Self {
        Self {
            topic_id,
            leader_epoch,
            metadata_generation: Some(metadata_generation),
        }
    }

    pub(crate) const fn topic_id(self) -> [u8; 16] {
        self.topic_id
    }

    pub(crate) const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }

    pub(crate) const fn metadata_generation(self) -> Option<TopicMetadataGeneration> {
        self.metadata_generation
    }

    pub(crate) const fn with_leader_epoch(self, leader_epoch: i32) -> Self {
        Self {
            leader_epoch: Some(leader_epoch),
            ..self
        }
    }

    pub(crate) const fn without_leader_epoch(self) -> Self {
        Self {
            leader_epoch: None,
            ..self
        }
    }
}
