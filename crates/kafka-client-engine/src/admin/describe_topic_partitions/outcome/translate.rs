//! Exhaustive core-to-engine topic-partition page translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeTopicPartition as CorePartition,
    DescribeTopicPartitionsFailureKind as CoreFailureKind,
    DescribeTopicPartitionsTerminal as CoreTerminal, DescribeTopicPartitionsTopic as CoreTopic,
};

use super::{
    AdminDescribeTopicPartition, AdminDescribeTopicPartitionsDeliveryStatus,
    AdminDescribeTopicPartitionsFailure, AdminDescribeTopicPartitionsFailureKind,
    AdminDescribeTopicPartitionsOutcome, AdminDescribeTopicPartitionsPage,
    AdminDescribeTopicPartitionsTopic,
};
use crate::admin::describe_topic_partitions::AdminDescribeTopicPartitionsCursor;

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AdminDescribeTopicPartitionsOutcome {
    match terminal {
        CoreTerminal::Page(page) => {
            let (throttle_time_ms, topics, next_cursor) = page.into_parts();
            AdminDescribeTopicPartitionsOutcome::Page(AdminDescribeTopicPartitionsPage {
                throttle_time_ms,
                topics: topics.into_iter().map(translate_topic).collect(),
                next_cursor: next_cursor.map(|cursor| {
                    let (topic_name, partition_index) = cursor.into_parts();
                    AdminDescribeTopicPartitionsCursor::new(topic_name, partition_index)
                }),
            })
        }
        CoreTerminal::Failed(failure) => {
            AdminDescribeTopicPartitionsOutcome::Failed(AdminDescribeTopicPartitionsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn translate_topic(topic: CoreTopic) -> AdminDescribeTopicPartitionsTopic {
    let (error_code, name, topic_id, internal, partitions, authorized_operations) =
        topic.into_parts();
    AdminDescribeTopicPartitionsTopic {
        error_code,
        name,
        topic_id,
        internal,
        partitions: partitions.into_iter().map(translate_partition).collect(),
        authorized_operations,
    }
}

fn translate_partition(partition: CorePartition) -> AdminDescribeTopicPartition {
    let (
        error_code,
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    ) = partition.into_parts();
    AdminDescribeTopicPartition {
        error_code,
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AdminDescribeTopicPartitionsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => {
            AdminDescribeTopicPartitionsFailureKind::DeadlineElapsed
        }
        CoreFailureKind::DriverRejected => AdminDescribeTopicPartitionsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AdminDescribeTopicPartitionsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => {
            AdminDescribeTopicPartitionsFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => AdminDescribeTopicPartitionsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => {
            AdminDescribeTopicPartitionsFailureKind::InvalidResponse
        }
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AdminDescribeTopicPartitionsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AdminDescribeTopicPartitionsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => {
            AdminDescribeTopicPartitionsDeliveryStatus::PossiblySent
        }
    }
}
