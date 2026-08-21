//! Inert group-offset deletion intent translated only at the engine boundary.

use kafka_client_engine::{
    DeleteConsumerGroupOffsetTarget as EngineTarget,
    DeleteConsumerGroupOffsetsRequest as EngineRequest,
};

use crate::TopicPartition;

// Kafka partitions are nonnegative. Preparing this sentinel before `submit`
// preserves the assignment-only misuse until engine validation, after the
// public absolute deadline has been captured.
const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

/// Linear request retained by the public builder before submission.
pub(crate) struct DeleteConsumerGroupOffsetsAdminRequest {
    inner: EngineRequest,
}

impl DeleteConsumerGroupOffsetsAdminRequest {
    pub(crate) fn new(group_id: String, targets: Vec<TopicPartition>) -> Self {
        Self {
            inner: EngineRequest::new(
                group_id,
                targets.into_iter().map(into_engine_target).collect(),
            ),
        }
    }

    pub(super) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for DeleteConsumerGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupOffsetsAdminRequest")
            .finish_non_exhaustive()
    }
}

fn into_engine_target(target: TopicPartition) -> EngineTarget {
    let (topic, partition, start) = target.into_parts();
    let partition = if start.is_some() {
        INVALID_ASSIGNMENT_POSITION_PARTITION
    } else {
        partition
    };
    EngineTarget::new(topic, partition)
}
