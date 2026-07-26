//! Exhaustive translation from deterministic core topic terminals.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeTopicResult as CoreTopicResult,
    DescribeTopicsFailureKind as CoreFailureKind, DescribeTopicsTerminal,
};

use super::{
    DescribeTopicError, DescribeTopicResult, DescribeTopicsDeliveryStatus, DescribeTopicsFailure,
    DescribeTopicsFailureKind, DescribeTopicsOutcome, TopicDescription, TopicPartitionDescription,
};

pub(crate) fn translate_terminal(terminal: DescribeTopicsTerminal) -> DescribeTopicsOutcome {
    match terminal {
        DescribeTopicsTerminal::Topics(outcomes) => DescribeTopicsOutcome::Topics(
            outcomes
                .into_iter()
                .map(|outcome| {
                    let (topic, internal, result) = outcome.into_parts();
                    let result = match result {
                        CoreTopicResult::Described(description) => {
                            Ok(translate_description(description))
                        }
                        CoreTopicResult::Failed(error) => {
                            Err(DescribeTopicError { code: error.code() })
                        }
                    };
                    DescribeTopicResult {
                        topic,
                        internal,
                        result,
                    }
                })
                .collect(),
        ),
        DescribeTopicsTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => DescribeTopicsFailureKind::DeadlineElapsed,
                CoreFailureKind::DriverRejected => DescribeTopicsFailureKind::DriverRejected,
                CoreFailureKind::Transport => DescribeTopicsFailureKind::Transport,
                CoreFailureKind::Broker(code) => DescribeTopicsFailureKind::Broker(code.get()),
                CoreFailureKind::ResponseTooLarge => DescribeTopicsFailureKind::ResponseTooLarge,
                CoreFailureKind::Compatibility => DescribeTopicsFailureKind::Compatibility,
                CoreFailureKind::InvalidResponse => DescribeTopicsFailureKind::InvalidResponse,
            };
            DescribeTopicsOutcome::Failed(DescribeTopicsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => DescribeTopicsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => DescribeTopicsDeliveryStatus::PossiblySent,
                },
            })
        }
    }
}

fn translate_description(description: kafka_client_core::TopicDescription) -> TopicDescription {
    let (name, topic_id, internal, partitions) = description.into_parts();
    TopicDescription {
        name,
        topic_id,
        internal,
        partitions: partitions
            .into_iter()
            .map(|partition| {
                let (partition_index, error_code, leader_id, leader_epoch, replicas, isr, offline) =
                    partition.into_parts();
                TopicPartitionDescription {
                    partition_index,
                    error_code,
                    leader_id,
                    leader_epoch,
                    replicas,
                    in_sync_replicas: isr,
                    offline_replicas: offline,
                }
            })
            .collect(),
    }
}
