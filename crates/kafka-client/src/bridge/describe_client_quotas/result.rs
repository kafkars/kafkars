//! Exhaustive stable translation of engine-owned DescribeClientQuotas outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        ClientQuotaEntityComponent, ClientQuotaEntry, ClientQuotaValue, DescribeClientQuotasResult,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, DeliveryStatus,
        Entity, EntityComponent, Failure, FailureKind, ObserverError, Outcome, Value,
    },
    operation::AdminDescribeClientQuotasResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("DescribeClientQuotas admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeClientQuotas was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeClientQuotas was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeClientQuotasResult {
    match result {
        Ok(Outcome::Described(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> DescribeClientQuotasResult {
    let (throttle_time_ms, entries) = batch.into_parts();
    DescribeClientQuotasResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        entries.into_iter().map(translate_entity).collect(),
    )
}

fn translate_entity(entity: Entity) -> ClientQuotaEntry {
    let (components, values) = entity.into_parts();
    translate_entity_parts(
        components.into_iter().map(translate_component).collect(),
        values.into_iter().map(translate_value).collect(),
    )
}

fn translate_component(component: EntityComponent) -> ClientQuotaEntityComponent {
    let (entity_type, entity_name) = component.into_parts();
    ClientQuotaEntityComponent::new(entity_type, entity_name)
}

fn translate_value(value: Value) -> ClientQuotaValue {
    let (key, value) = value.into_parts();
    ClientQuotaValue::new(key, value)
}

pub(super) const fn translate_entity_parts(
    components: Vec<ClientQuotaEntityComponent>,
    values: Vec<ClientQuotaValue>,
) -> ClientQuotaEntry {
    ClientQuotaEntry::new(components, values)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind().clone(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let delivery = translate_delivery(delivery);
    match kind {
        FailureKind::Broker(error) => translate_broker_error(error, delivery),
        kind => {
            let public = match kind {
                FailureKind::DeadlineElapsed => ErrorKind::Timeout,
                FailureKind::DriverRejected | FailureKind::ResponseTooLarge => {
                    ErrorKind::Backpressure
                }
                FailureKind::Transport => ErrorKind::Transport,
                FailureKind::Compatibility => ErrorKind::Compatibility,
                FailureKind::InvalidResponse => ErrorKind::Broker,
                FailureKind::Broker(_) => unreachable!(),
            };
            KafkaError::new(public, format!("DescribeClientQuotas failed: {kind:?}"))
                .with_delivery_status(delivery)
        }
    }
}

fn translate_broker_error(error: BrokerError, delivery: PublicDeliveryStatus) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_parts(code, message.as_deref(), message_truncated, delivery)
}

pub(super) fn translate_broker_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
    delivery: PublicDeliveryStatus,
) -> KafkaError {
    let diagnostic = match (message, message_truncated) {
        (Some(message), true) => format!(
            "Kafka rejected DescribeClientQuotas with broker code {code}: {message} [truncated]"
        ),
        (Some(message), false) => {
            format!("Kafka rejected DescribeClientQuotas with broker code {code}: {message}")
        }
        (None, _) => {
            format!("Kafka rejected DescribeClientQuotas with broker code {code}")
        }
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(delivery)
        .with_diagnostic_truncated(message_truncated)
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
