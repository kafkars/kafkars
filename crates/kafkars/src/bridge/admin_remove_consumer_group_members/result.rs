//! Exhaustive translation of concrete static-member removal outcomes.

use std::time::Duration;

use kafka_client_engine::{
    ConsumerGroupMemberRemovalBrokerError as EngineBrokerError,
    RemoveConsumerGroupMembersAcceptedFaultKind, RemoveConsumerGroupMembersAdmissionError,
    RemoveConsumerGroupMembersAdmissionErrorKind, RemoveConsumerGroupMembersDeliveryStatus,
    RemoveConsumerGroupMembersFailure, RemoveConsumerGroupMembersFailureKind,
    RemoveConsumerGroupMembersObserverError, RemoveConsumerGroupMembersOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, RemoveConsumerGroupMembersResult},
};

use super::operation::AdminRemoveConsumerGroupMembersResult;

pub(super) fn translate_admission_error(
    error: &RemoveConsumerGroupMembersAdmissionError,
) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        RemoveConsumerGroupMembersAdmissionErrorKind::InvalidRequest
        | RemoveConsumerGroupMembersAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        RemoveConsumerGroupMembersAdmissionErrorKind::Contended
        | RemoveConsumerGroupMembersAdmissionErrorKind::Capacity
        | RemoveConsumerGroupMembersAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        RemoveConsumerGroupMembersAdmissionErrorKind::Closed => ErrorKind::State,
        RemoveConsumerGroupMembersAdmissionErrorKind::IdentityExhausted
        | RemoveConsumerGroupMembersAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("RemoveConsumerGroupMembers admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: RemoveConsumerGroupMembersAcceptedFaultKind,
) -> KafkaError {
    match fault {
        RemoveConsumerGroupMembersAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "RemoveConsumerGroupMembers was accepted but its host wake failed",
        ),
        RemoveConsumerGroupMembersAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "RemoveConsumerGroupMembers host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<RemoveConsumerGroupMembersOutcome, RemoveConsumerGroupMembersObserverError>,
) -> AdminRemoveConsumerGroupMembersResult {
    match result {
        Ok(RemoveConsumerGroupMembersOutcome::Removed(batch)) => {
            let (throttle_time_ms, members) = batch.into_parts();
            let entries = members
                .into_iter()
                .map(|member| {
                    let (group_instance_id, result) = member.into_parts();
                    (group_instance_id, result.map_err(member_error))
                })
                .collect();
            Ok(RemoveConsumerGroupMembersResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(RemoveConsumerGroupMembersOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn member_error(error: EngineBrokerError) -> KafkaError {
    broker_error("member", error, DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: RemoveConsumerGroupMembersFailure) -> KafkaError {
    let (kind, delivery) = failure.into_parts();
    let delivery = translate_delivery(delivery);
    match kind {
        RemoveConsumerGroupMembersFailureKind::Broker(error) => {
            broker_error("request", error, delivery)
        }
        kind => {
            let public = match kind {
                RemoveConsumerGroupMembersFailureKind::DeadlineElapsed => ErrorKind::Timeout,
                RemoveConsumerGroupMembersFailureKind::DriverRejected
                | RemoveConsumerGroupMembersFailureKind::ResponseTooLarge => {
                    ErrorKind::Backpressure
                }
                RemoveConsumerGroupMembersFailureKind::Transport => ErrorKind::Transport,
                RemoveConsumerGroupMembersFailureKind::Compatibility => ErrorKind::Compatibility,
                RemoveConsumerGroupMembersFailureKind::InvalidResponse => ErrorKind::Broker,
                RemoveConsumerGroupMembersFailureKind::Broker(_) => unreachable!(),
            };
            KafkaError::new(
                public,
                format!("RemoveConsumerGroupMembers failed: {kind:?}"),
            )
            .with_delivery_status(delivery)
        }
    }
}

fn broker_error(scope: &str, error: EngineBrokerError, delivery: DeliveryStatus) -> KafkaError {
    let code = error.code();
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka rejected consumer-group {scope} removal with broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(delivery)
}

const fn translate_delivery(delivery: RemoveConsumerGroupMembersDeliveryStatus) -> DeliveryStatus {
    match delivery {
        RemoveConsumerGroupMembersDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        RemoveConsumerGroupMembersDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: RemoveConsumerGroupMembersObserverError) -> KafkaError {
    let public = match error {
        RemoveConsumerGroupMembersObserverError::AlreadyObserved => ErrorKind::State,
        RemoveConsumerGroupMembersObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
