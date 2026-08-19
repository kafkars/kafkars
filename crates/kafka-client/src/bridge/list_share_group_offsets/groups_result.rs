//! Exhaustive translation of multi-ShareGroup offset outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, ListShareGroupsOffsetsResult},
};

use super::{
    engine::{BatchOutcome, ObserverError, Outcome},
    groups_operation::AdminListShareGroupsOffsetsResult,
    result::{
        translate_broker_error, translate_failure, translate_observer_error,
        translate_offsets_batch,
    },
};

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminListShareGroupsOffsetsResult {
    match result {
        Ok(Outcome::Batch(batch)) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            let groups = outcomes
                .into_iter()
                .map(|outcome| match outcome {
                    BatchOutcome::Offsets { group_id, offsets } => {
                        (group_id, Ok(translate_offsets_batch(offsets)))
                    }
                    BatchOutcome::BrokerRejected { group_id, error } => {
                        (group_id, Err(translate_broker_error(error)))
                    }
                })
                .collect();
            Ok(ListShareGroupsOffsetsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(groups),
            ))
        }
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(Outcome::Offsets(_) | Outcome::BrokerRejected(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "plural ListShareGroupsOffsets received a singular terminal",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}
