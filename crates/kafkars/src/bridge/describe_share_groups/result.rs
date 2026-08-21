//! Exhaustive stable translation of batched `ShareGroup` engine outcomes.

use std::time::Duration;

use crate::{
    BatchResult, DeliveryStatus, ErrorKind, KafkaError,
    admin::DescribeShareGroupsResult,
    bridge::describe_share_group::result::{
        translate_broker_error, translate_description, translate_failure, translate_observer_error,
    },
};

use super::{
    engine::{AdmissionError, BatchOutcome, ObserverError, Outcome},
    operation::AdminDescribeShareGroupsResult,
};

pub(super) fn translate_admission(error: AdmissionError) -> KafkaError {
    crate::bridge::describe_share_group::result::translate_admission_error(error)
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeShareGroupsResult {
    match result {
        Ok(Outcome::Batch(batch)) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            let entries = outcomes
                .into_iter()
                .map(|outcome| match outcome {
                    BatchOutcome::Described(result) => {
                        let (_group_throttle_time_ms, description) = result.into_parts();
                        let group_id = description.group_id().to_owned();
                        (group_id, Ok(translate_description(description)))
                    }
                    BatchOutcome::BrokerRejected { group_id, error } => {
                        (group_id, Err(translate_broker_error(error)))
                    }
                })
                .collect();
            Ok(DescribeShareGroupsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(Outcome::Described(_) | Outcome::BrokerRejected(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "DescribeShareGroups received a singular terminal from its batch plan",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}
