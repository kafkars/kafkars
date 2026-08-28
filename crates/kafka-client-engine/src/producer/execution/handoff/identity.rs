//! Immutable route-planning facts and topic-identity proof for one handoff.

use kafka_client_core::partitioning::TopicMetadataGeneration;

use super::PreparedProduceSubmission;
use crate::protocol::produce::MaterializedProduce;

impl PreparedProduceSubmission {
    /// Returns the core operation whose original deadline governs this handoff.
    pub(crate) const fn operation_id(&self) -> kafka_client_core::OperationId {
        self.operation_id
    }

    /// Returns the exact partition retained by this materialized batch.
    pub(crate) const fn partition(&self) -> i32 {
        self.materialized.partition()
    }

    /// Borrows the still-linear materialized owner for route and request planning.
    pub(crate) const fn materialized(&self) -> &MaterializedProduce {
        &self.materialized
    }

    /// Returns the expected broker-issued topic identity, when configured.
    pub(crate) const fn expected_topic_uuid(&self) -> Option<[u8; 16]> {
        self.materialized.expected_topic_uuid()
    }

    /// Returns the topic-view generation that validated the expected identity.
    pub(crate) const fn validated_topic_generation(&self) -> Option<TopicMetadataGeneration> {
        self.materialized.validated_topic_generation()
    }

    /// Returns identity proof required before a replacement Produce attempt.
    pub(crate) const fn retry_topic_identity(&self) -> Option<([u8; 16], TopicMetadataGeneration)> {
        if self.execution.generation().get() <= 1 {
            return None;
        }
        match (
            self.materialized.expected_topic_uuid(),
            self.materialized.validated_topic_generation(),
        ) {
            (Some(topic_uuid), Some(generation)) => Some((topic_uuid, generation)),
            _ => None,
        }
    }

    /// Replaces the retained proof after one exact newer topic view succeeds.
    pub(crate) fn record_retry_topic_identity(
        &mut self,
        expected_topic_uuid: [u8; 16],
        generation: TopicMetadataGeneration,
    ) -> bool {
        self.materialized
            .record_topic_identity_revalidation(expected_topic_uuid, generation)
    }
}
