//! Exhaustive core-to-engine share-group offset terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ListShareGroupOffsetResult as CorePartitionResult,
    ListShareGroupOffsetsFailureKind as CoreFailureKind,
    ListShareGroupOffsetsTerminal as CoreTerminal,
};

use super::{
    ListShareGroupOffsetsBatchOutcome, ListShareGroupOffsetsBrokerError,
    ListShareGroupOffsetsDeliveryStatus, ListShareGroupOffsetsFailure,
    ListShareGroupOffsetsFailureKind, ListShareGroupOffsetsOutcome, ListShareGroupsOffsetsBatch,
};
use crate::admin::list_share_group_offsets::{
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsPartitionDescription,
    ListShareGroupOffsetsPartitionError, ListShareGroupOffsetsPartitionResult,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListShareGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Offsets(batch) => {
            ListShareGroupOffsetsOutcome::Offsets(translate_offsets_batch(batch))
        }
        CoreTerminal::BrokerRejected(error) => {
            ListShareGroupOffsetsOutcome::BrokerRejected(translate_broker_error(error))
        }
        CoreTerminal::Batch(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            ListShareGroupOffsetsOutcome::Batch(ListShareGroupsOffsetsBatch {
                throttle_time_ms,
                outcomes: outcomes.into_iter().map(translate_batch_outcome).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            ListShareGroupOffsetsOutcome::Failed(ListShareGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn translate_batch_outcome(
    outcome: kafka_client_core::ListShareGroupOffsetsBatchOutcome,
) -> ListShareGroupOffsetsBatchOutcome {
    match outcome {
        kafka_client_core::ListShareGroupOffsetsBatchOutcome::Offsets { group_id, offsets } => {
            ListShareGroupOffsetsBatchOutcome::Offsets {
                group_id,
                offsets: translate_offsets_batch(offsets),
            }
        }
        kafka_client_core::ListShareGroupOffsetsBatchOutcome::BrokerRejected {
            group_id,
            error,
        } => ListShareGroupOffsetsBatchOutcome::BrokerRejected {
            group_id,
            error: translate_broker_error(error),
        },
    }
}

fn translate_offsets_batch(
    batch: kafka_client_core::ListShareGroupOffsetsBatch,
) -> ListShareGroupOffsetsBatch {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    ListShareGroupOffsetsBatch {
        throttle_time_ms,
        offsets: outcomes
            .into_iter()
            .map(|outcome| {
                let (topic, topic_id, partition, result) = outcome.into_parts();
                let result = match result {
                    CorePartitionResult::Described(description) => {
                        let (start_offset, leader_epoch, lag) = description.into_parts();
                        Ok(ListShareGroupOffsetsPartitionDescription {
                            start_offset,
                            leader_epoch,
                            lag,
                        })
                    }
                    CorePartitionResult::Failed(error) => {
                        let (code, message, message_truncated) = error.into_parts();
                        Err(ListShareGroupOffsetsPartitionError {
                            code,
                            message,
                            message_truncated,
                        })
                    }
                };
                ListShareGroupOffsetsPartitionResult {
                    topic,
                    topic_id,
                    partition,
                    result,
                }
            })
            .collect(),
    }
}

fn translate_broker_error(
    error: kafka_client_core::ListShareGroupOffsetsBrokerError,
) -> ListShareGroupOffsetsBrokerError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    ListShareGroupOffsetsBrokerError {
        throttle_time_ms,
        code,
        message,
        message_truncated,
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ListShareGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListShareGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListShareGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListShareGroupOffsetsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => ListShareGroupOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ListShareGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListShareGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListShareGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListShareGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListShareGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
