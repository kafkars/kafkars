//! Topic-identity proof carried with one materialized Produce request.

use kafka_client_core::partitioning::TopicMetadataGeneration;

use super::MaterializedProduce;

impl MaterializedProduce {
    pub(crate) const fn with_expected_topic_identity(
        mut self,
        expected_topic_uuid: Option<[u8; 16]>,
        validated_topic_generation: Option<TopicMetadataGeneration>,
    ) -> Self {
        self.expected_topic_uuid = expected_topic_uuid;
        self.validated_topic_generation = validated_topic_generation;
        self
    }

    pub(crate) const fn expected_topic_uuid(&self) -> Option<[u8; 16]> {
        self.expected_topic_uuid
    }

    pub(crate) const fn validated_topic_generation(&self) -> Option<TopicMetadataGeneration> {
        self.validated_topic_generation
    }

    pub(crate) fn record_topic_identity_revalidation(
        &mut self,
        expected_topic_uuid: [u8; 16],
        generation: TopicMetadataGeneration,
    ) -> bool {
        if self.expected_topic_uuid != Some(expected_topic_uuid)
            || self
                .validated_topic_generation
                .is_some_and(|current| generation <= current)
        {
            return false;
        }
        self.validated_topic_generation = Some(generation);
        true
    }

    pub(crate) fn topic_name_for_identity(&self) -> &str {
        self.topic.as_ref()
    }
}
