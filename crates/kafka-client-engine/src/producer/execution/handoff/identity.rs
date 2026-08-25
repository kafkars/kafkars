//! Topic-identity proof retained across one prepared Produce replacement.

use kafka_client_core::partitioning::TopicMetadataGeneration;

use super::PreparedProduceSubmission;

impl PreparedProduceSubmission {
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

    /// Borrows the logical topic required for retry identity validation.
    pub(crate) fn topic(&self) -> &str {
        self.materialized.topic_name_for_identity()
    }
}
