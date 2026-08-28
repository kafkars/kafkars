//! Atomic persistence of exact retry topic-identity validation.

use kafka_client_core::{BatchExecutionId, partitioning::TopicMetadataGeneration};

use super::ProducerStore;
use crate::producer::ProducerStoreError;

impl ProducerStore {
    /// Checks one newer UUID validation across every record in exact executions.
    pub(in crate::producer) fn can_record_retry_topic_identity<I>(
        &self,
        executions: I,
        expected: [u8; 16],
        generation: TopicMetadataGeneration,
    ) -> Result<bool, ProducerStoreError>
    where
        I: Iterator<Item = BatchExecutionId>,
    {
        for execution in executions {
            let members = self.batches.execution_members(execution)?;
            if members.is_empty() {
                return Err(ProducerStoreError::EmptyBatch);
            }
            for member in members {
                if !self
                    .records
                    .record(member.payload_id)?
                    .can_record_topic_identity_revalidation(expected, generation)
                {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Commits a previously checked UUID validation to canonical record owners.
    pub(in crate::producer) fn record_retry_topic_identity<I>(
        &mut self,
        executions: I,
        expected: [u8; 16],
        generation: TopicMetadataGeneration,
    ) -> Result<bool, ProducerStoreError>
    where
        I: Clone + Iterator<Item = BatchExecutionId>,
    {
        if !self.can_record_retry_topic_identity(executions.clone(), expected, generation)? {
            return Ok(false);
        }
        let (batches, records) = (&self.batches, &mut self.records);
        for execution in executions {
            for member in batches.execution_members(execution)? {
                let recorded = records
                    .record_mut(member.payload_id)?
                    .record_topic_identity_revalidation(expected, generation);
                debug_assert!(recorded);
            }
        }
        Ok(true)
    }
}
