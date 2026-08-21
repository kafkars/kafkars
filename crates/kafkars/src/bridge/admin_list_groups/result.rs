//! Exhaustive stable translation of unfiltered cluster group-listing outcomes.

use std::time::Duration;

use kafka_client_engine::{
    ListConsumerGroupsAcceptedFaultKind, ListConsumerGroupsAdmissionError,
    ListConsumerGroupsAdmissionErrorKind, ListConsumerGroupsDeliveryStatus,
    ListConsumerGroupsDiscoveryError as EngineDiscoveryError, ListConsumerGroupsFailure,
    ListConsumerGroupsFailureKind, ListConsumerGroupsObserverError, ListConsumerGroupsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{GroupListing, ListGroupsBrokerError, ListGroupsResult},
};

use super::operation::AdminListGroupsResult;

pub(super) fn translate_admission_error(error: ListConsumerGroupsAdmissionError) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        ListConsumerGroupsAdmissionErrorKind::InvalidDeadline
        | ListConsumerGroupsAdmissionErrorKind::InvalidRequest => ErrorKind::Configuration,
        ListConsumerGroupsAdmissionErrorKind::Contended
        | ListConsumerGroupsAdmissionErrorKind::Capacity
        | ListConsumerGroupsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        ListConsumerGroupsAdmissionErrorKind::Closed => ErrorKind::State,
        ListConsumerGroupsAdmissionErrorKind::IdentityExhausted
        | ListConsumerGroupsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(public, format!("ListGroups admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: ListConsumerGroupsAcceptedFaultKind) -> KafkaError {
    match fault {
        ListConsumerGroupsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ListGroups was accepted but its host wake failed",
        ),
        ListConsumerGroupsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ListGroups was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<ListConsumerGroupsOutcome, ListConsumerGroupsObserverError>,
) -> AdminListGroupsResult {
    match result {
        Ok(ListConsumerGroupsOutcome::Groups(batch)) => {
            let (throttle_time_ms, groups, errors) = batch.into_parts();
            Ok(ListGroupsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                groups
                    .into_iter()
                    .map(|group| {
                        let (group_id, protocol_type, group_state, group_type) = group.into_parts();
                        GroupListing::new(group_id, protocol_type, group_state, group_type)
                    })
                    .collect(),
                errors
                    .into_iter()
                    .map(|error| {
                        let (broker_id, code) = error.into_parts();
                        ListGroupsBrokerError::new(broker_id, code)
                    })
                    .collect(),
            ))
        }
        Ok(ListConsumerGroupsOutcome::DiscoveryRejected(error)) => {
            Err(translate_discovery_error(error))
        }
        Ok(ListConsumerGroupsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_discovery_error(error: EngineDiscoveryError) -> KafkaError {
    let (code, message, truncated) = error.into_parts();
    let detail = message.map_or_else(
        || format!("Kafka rejected ListGroups broker discovery with code {code}"),
        |message| format!("Kafka rejected ListGroups broker discovery with code {code}: {message}"),
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(truncated)
}

fn translate_failure(failure: ListConsumerGroupsFailure) -> KafkaError {
    let kind = failure.kind();
    let public = match kind {
        ListConsumerGroupsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        ListConsumerGroupsFailureKind::DriverRejected
        | ListConsumerGroupsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        ListConsumerGroupsFailureKind::Transport => ErrorKind::Transport,
        ListConsumerGroupsFailureKind::Compatibility => ErrorKind::Compatibility,
        ListConsumerGroupsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("ListGroups failed: {kind:?}"))
        .with_delivery_status(translate_delivery(failure.delivery()))
}

const fn translate_delivery(delivery: ListConsumerGroupsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        ListConsumerGroupsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        ListConsumerGroupsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: ListConsumerGroupsObserverError) -> KafkaError {
    let public = match error {
        ListConsumerGroupsObserverError::AlreadyObserved => ErrorKind::State,
        ListConsumerGroupsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
