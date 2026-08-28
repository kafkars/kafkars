//! Borrow-only facts for correlating prepared Produce work with one topic view.

use std::sync::Arc;

use kafka_client_core::{BatchExecutionId, OperationId, partitioning::TopicMetadataGeneration};

use crate::clock::OperationDeadline;

/// Stable facts shared by every candidate that may use one immutable topic view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedProduceRouteKey {
    topic: Arc<str>,
    deadline: OperationDeadline,
    expected_topic_uuid: Option<[u8; 16]>,
    replacement: bool,
    validated_generation: Option<TopicMetadataGeneration>,
}

/// Exact prepared entry observed without moving its materialized byte owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedProduceRouteCandidate {
    execution: BatchExecutionId,
    operation_id: OperationId,
    partition: i32,
}

/// One freshly observed stable prefix under the prepared owner.
pub(crate) struct PreparedProduceRouteWindow {
    key: PreparedProduceRouteKey,
    candidates: Vec<PreparedProduceRouteCandidate>,
}

impl PreparedProduceRouteKey {
    pub(super) const fn new(
        topic: Arc<str>,
        deadline: OperationDeadline,
        expected_topic_uuid: Option<[u8; 16]>,
        replacement: bool,
        validated_generation: Option<TopicMetadataGeneration>,
    ) -> Self {
        Self {
            topic,
            deadline,
            expected_topic_uuid,
            replacement,
            validated_generation,
        }
    }

    /// Borrows the exact topic used to acquire and correlate a topic view.
    pub(crate) fn topic(&self) -> &str {
        self.topic.as_ref()
    }

    /// Returns the unchanged public-boundary deadline shared by the cohort.
    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }

    /// Returns the expected broker-issued topic identity, when configured.
    pub(crate) const fn expected_topic_uuid(&self) -> Option<[u8; 16]> {
        self.expected_topic_uuid
    }

    /// Returns the exact retry identity proof required by this cohort.
    pub(crate) const fn retry_topic_identity(&self) -> Option<([u8; 16], TopicMetadataGeneration)> {
        match (
            self.replacement,
            self.expected_topic_uuid,
            self.validated_generation,
        ) {
            (true, Some(expected), Some(generation)) => Some((expected, generation)),
            _ => None,
        }
    }

    /// Returns the expected UUID whose newer validation must persist for a retry.
    pub(crate) const fn replacement_topic_uuid(&self) -> Option<[u8; 16]> {
        if self.replacement {
            self.expected_topic_uuid
        } else {
            None
        }
    }

    pub(super) const fn replacement(&self) -> bool {
        self.replacement
    }

    pub(super) const fn validated_generation(&self) -> Option<TopicMetadataGeneration> {
        self.validated_generation
    }
}

impl PreparedProduceRouteCandidate {
    pub(super) const fn new(
        execution: BatchExecutionId,
        operation_id: OperationId,
        partition: i32,
    ) -> Self {
        Self {
            execution,
            operation_id,
            partition,
        }
    }

    /// Returns the exact core execution observed under prepared ownership.
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Returns the explicit partition requiring one exact leader broker.
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }
}

impl PreparedProduceRouteWindow {
    pub(super) const fn new(
        key: PreparedProduceRouteKey,
        candidates: Vec<PreparedProduceRouteCandidate>,
    ) -> Self {
        Self { key, candidates }
    }

    /// Borrows the stable route key for comparison with a retained view.
    pub(crate) const fn key(&self) -> &PreparedProduceRouteKey {
        &self.key
    }

    /// Transfers the borrowed snapshot facts into immediate route planning.
    pub(crate) fn into_parts(
        self,
    ) -> (PreparedProduceRouteKey, Vec<PreparedProduceRouteCandidate>) {
        (self.key, self.candidates)
    }
}
