//! Exhaustive core-to-engine share-group offset terminal translation.

use kafka_client_core::{
    AlterShareGroupOffsetsFailureKind as CoreFailureKind,
    AlterShareGroupOffsetsPartitionResult as CorePartitionResult,
    AlterShareGroupOffsetsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsDeliveryStatus,
    AlterShareGroupOffsetsFailure, AlterShareGroupOffsetsFailureKind,
    AlterShareGroupOffsetsOutcome,
};
use crate::admin::alter_share_group_offsets::{
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsPartitionError,
    AlterShareGroupOffsetsPartitionResult,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AlterShareGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Altered(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AlterShareGroupOffsetsOutcome::Altered(AlterShareGroupOffsetsBatch {
                throttle_time_ms,
                partitions: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, topic_id, partition, result) = outcome.into_parts();
                        let result = match result {
                            CorePartitionResult::Altered => Ok(topic_id),
                            CorePartitionResult::Failed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(AlterShareGroupOffsetsPartitionError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        AlterShareGroupOffsetsPartitionResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
            AlterShareGroupOffsetsOutcome::BrokerRejected(AlterShareGroupOffsetsBrokerError {
                throttle_time_ms,
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => {
            AlterShareGroupOffsetsOutcome::Failed(AlterShareGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AlterShareGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AlterShareGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AlterShareGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AlterShareGroupOffsetsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AlterShareGroupOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AlterShareGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AlterShareGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AlterShareGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AlterShareGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AlterShareGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
