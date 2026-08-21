//! Inert group-offset alteration intent translated only at the engine boundary.

use std::time::Duration;

use kafka_client_engine::{
    AlterConsumerGroupOffsetTarget as EngineTarget,
    AlterConsumerGroupOffsetsRequest as EngineRequest,
};

use crate::ConsumerGroupOffsetAlteration;

/// Linear request retained by the public builder before submission.
pub(crate) struct AlterConsumerGroupOffsetsAdminRequest {
    inner: EngineRequest,
}

impl AlterConsumerGroupOffsetsAdminRequest {
    pub(crate) fn new(group_id: String, alterations: Vec<ConsumerGroupOffsetAlteration>) -> Self {
        Self {
            inner: EngineRequest::new(
                group_id,
                alterations
                    .into_iter()
                    .map(into_engine_alteration)
                    .collect(),
            ),
        }
    }

    pub(crate) fn with_retention_time(mut self, retention_time: Duration) -> Self {
        self.inner = self.inner.with_retention_time(retention_time);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for AlterConsumerGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterConsumerGroupOffsetsAdminRequest")
            .finish_non_exhaustive()
    }
}

fn into_engine_alteration(alteration: ConsumerGroupOffsetAlteration) -> EngineTarget {
    let (topic, partition, next_offset, leader_epoch, metadata) = alteration.into_parts();
    EngineTarget::new(topic, partition, next_offset, leader_epoch, metadata)
}
