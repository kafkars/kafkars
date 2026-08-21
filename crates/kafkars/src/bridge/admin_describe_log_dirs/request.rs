//! Inert broker and topic-partition selection translated at the engine boundary.

use crate::TopicPartition;

use super::engine::{
    Request as EngineRequest, all_request as engine_all_request,
    selected_request as engine_selected_request, target as engine_target,
};

// Kafka partitions are nonnegative. Preparing this sentinel before `submit`
// preserves assignment-only misuse until engine validation, after the public
// absolute deadline has been captured.
const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

enum Selection {
    All,
    Selected(Vec<TopicPartition>),
}

/// Linear caller-ordered selection retained by the public builder.
pub(crate) struct DescribeLogDirsAdminRequest {
    broker_ids: Vec<i32>,
    selection: Selection,
}

impl DescribeLogDirsAdminRequest {
    pub(crate) const fn new(broker_ids: Vec<i32>) -> Self {
        Self {
            broker_ids,
            selection: Selection::All,
        }
    }

    pub(crate) fn with_partitions(mut self, partitions: Vec<TopicPartition>) -> Self {
        self.selection = Selection::Selected(partitions);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        match self.selection {
            Selection::All => engine_all_request(self.broker_ids),
            Selection::Selected(partitions) => engine_selected_request(
                self.broker_ids,
                partitions.into_iter().map(into_engine_target).collect(),
            ),
        }
    }
}

impl std::fmt::Debug for DescribeLogDirsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeLogDirsAdminRequest")
            .field("broker_ids", &self.broker_ids)
            .field(
                "selection",
                &match &self.selection {
                    Selection::All => "All",
                    Selection::Selected(_) => "Selected",
                },
            )
            .finish_non_exhaustive()
    }
}

fn into_engine_target(target: TopicPartition) -> super::engine::Target {
    let (topic, partition, start) = target.into_parts();
    let partition = if start.is_some() {
        INVALID_ASSIGNMENT_POSITION_PARTITION
    } else {
        partition
    };
    engine_target(topic, partition)
}
