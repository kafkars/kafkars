//! Lossless core-to-engine group-offset terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, GroupOffsetResult as CoreOffsetResult,
    ListConsumerGroupOffsetsFailureKind as CoreFailureKind,
    ListConsumerGroupOffsetsTerminal as CoreTerminal,
};

use super::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetResult,
    ListConsumerGroupBatchOutcome, ListConsumerGroupOffsetsBatch,
    ListConsumerGroupOffsetsDeliveryStatus, ListConsumerGroupOffsetsFailure,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsOutcome,
    ListConsumerGroupsOffsetsBatch,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListConsumerGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Offsets(batch) => {
            ListConsumerGroupOffsetsOutcome::Offsets(translate_offsets_batch(batch))
        }
        CoreTerminal::Batch(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            ListConsumerGroupOffsetsOutcome::Batch(ListConsumerGroupsOffsetsBatch {
                throttle_time_ms,
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (group_id, result) = outcome.into_parts();
                        match result {
                            Ok(offsets) => ListConsumerGroupBatchOutcome::Offsets {
                                group_id,
                                offsets: translate_offsets_batch(offsets),
                            },
                            Err(code) => ListConsumerGroupBatchOutcome::BrokerRejected {
                                group_id,
                                code: code.get(),
                            },
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            ListConsumerGroupOffsetsOutcome::Failed(ListConsumerGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn translate_offsets_batch(
    batch: kafka_client_core::ListConsumerGroupOffsetsBatch,
) -> ListConsumerGroupOffsetsBatch {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    ListConsumerGroupOffsetsBatch {
        throttle_time_ms,
        offsets: outcomes
            .into_iter()
            .map(|outcome| {
                let (topic, partition, result) = outcome.into_parts();
                let result = match result {
                    CoreOffsetResult::Described(description) => {
                        let (offset, leader_epoch, metadata) = description.into_parts();
                        Ok(GroupOffsetDescription {
                            offset,
                            leader_epoch,
                            metadata,
                        })
                    }
                    CoreOffsetResult::Failed(error) => {
                        Err(GroupOffsetBrokerError { code: error.code() })
                    }
                };
                GroupOffsetResult {
                    topic,
                    partition,
                    result,
                }
            })
            .collect(),
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ListConsumerGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListConsumerGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListConsumerGroupOffsetsFailureKind::Transport,
        CoreFailureKind::Broker(code) => ListConsumerGroupOffsetsFailureKind::Broker(code.get()),
        CoreFailureKind::ResponseTooLarge => ListConsumerGroupOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ListConsumerGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListConsumerGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListConsumerGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListConsumerGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
