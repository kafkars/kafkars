//! Exhaustive translation of multi-consumer-group offset outcomes.

use std::time::Duration;

use kafka_client_engine::{ListConsumerGroupOffsetsObserverError, ListConsumerGroupOffsetsOutcome};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, ListConsumerGroupsOffsetsResult},
};

use super::{
    list_groups_operation::AdminListConsumerGroupsOffsetsResult,
    list_result::{
        group_error, translate_failure, translate_observer_error, translate_offsets_batch,
    },
};

pub(super) fn translate_observation(
    result: Result<ListConsumerGroupOffsetsOutcome, ListConsumerGroupOffsetsObserverError>,
) -> AdminListConsumerGroupsOffsetsResult {
    match result {
        Ok(ListConsumerGroupOffsetsOutcome::Batch(batch)) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            let groups = outcomes
                .into_iter()
                .map(|outcome| {
                    let (group_id, result) = outcome.into_parts();
                    (
                        group_id,
                        result.map(translate_offsets_batch).map_err(group_error),
                    )
                })
                .collect();
            Ok(ListConsumerGroupsOffsetsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(groups),
            ))
        }
        Ok(ListConsumerGroupOffsetsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(ListConsumerGroupOffsetsOutcome::Offsets(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "plural ListConsumerGroupsOffsets received a singular terminal",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}
