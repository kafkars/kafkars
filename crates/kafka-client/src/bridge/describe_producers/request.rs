//! Inert public `DescribeProducers` intent translated at the engine boundary.

use crate::TopicPartition;

use super::engine::{Request as EngineRequest, RequestTarget as EngineTarget};

// Kafka partitions are nonnegative. Preparing this sentinel before `submit`
// preserves assignment-only misuse until engine validation, after the public
// absolute deadline has been captured.
const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

/// Linear request retained by the public builder before submission.
pub(crate) struct DescribeProducersAdminRequest {
    targets: Vec<TopicPartition>,
    broker_id: Option<i32>,
}

impl DescribeProducersAdminRequest {
    pub(crate) const fn new(targets: Vec<TopicPartition>) -> Self {
        Self {
            targets,
            broker_id: None,
        }
    }

    pub(crate) const fn with_broker_id(mut self, broker_id: i32) -> Self {
        self.broker_id = Some(broker_id);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.targets.into_iter().map(into_engine_target).collect(),
            self.broker_id,
        )
    }
}

impl std::fmt::Debug for DescribeProducersAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeProducersAdminRequest")
            .field("targets", &self.targets)
            .field("broker_id", &self.broker_id)
            .finish()
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
