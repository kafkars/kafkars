//! Exhaustive core-to-engine share-group offset terminal translation.

use kafka_client_core::{
    DeleteShareGroupOffsetsFailureKind as CoreFailureKind,
    DeleteShareGroupOffsetsTerminal as CoreTerminal,
    DeleteShareGroupOffsetsTopicResult as CoreTopicResult, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    DeleteShareGroupOffsetsBrokerError, DeleteShareGroupOffsetsDeliveryStatus,
    DeleteShareGroupOffsetsFailure, DeleteShareGroupOffsetsFailureKind,
    DeleteShareGroupOffsetsOutcome,
};
use crate::admin::delete_share_group_offsets::{
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsTopicError,
    DeleteShareGroupOffsetsTopicResult,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DeleteShareGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Deleted(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DeleteShareGroupOffsetsOutcome::Deleted(DeleteShareGroupOffsetsBatch {
                throttle_time_ms,
                topics: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, result) = outcome.into_parts();
                        let result = match result {
                            CoreTopicResult::Deleted(topic_id) => Ok(topic_id),
                            CoreTopicResult::Failed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(DeleteShareGroupOffsetsTopicError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        DeleteShareGroupOffsetsTopicResult { topic, result }
                    })
                    .collect(),
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
            DeleteShareGroupOffsetsOutcome::BrokerRejected(DeleteShareGroupOffsetsBrokerError {
                throttle_time_ms,
                code,
                message,
                message_truncated,
            })
        }
        CoreTerminal::Failed(failure) => {
            DeleteShareGroupOffsetsOutcome::Failed(DeleteShareGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> DeleteShareGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DeleteShareGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DeleteShareGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DeleteShareGroupOffsetsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DeleteShareGroupOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DeleteShareGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DeleteShareGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DeleteShareGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DeleteShareGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DeleteShareGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
