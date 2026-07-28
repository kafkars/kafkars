//! Exhaustive stable translation of engine-owned DescribeAcls outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        AccessControlEntry, AclBinding, AclOperation, AclPatternType, AclPermissionType,
        AclResourceType, DescribeAclsResult, ResourcePattern,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, Binding, BrokerError,
        DeliveryStatus, Failure, FailureKind, ObserverError, Outcome,
    },
    operation::AdminDescribeAclsResult,
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
    KafkaError::new(public, format!("DescribeAcls admission failed: {kind:?}"))
        .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeAcls was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeAcls was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeAclsResult {
    match result {
        Ok(Outcome::Described(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> DescribeAclsResult {
    let (throttle_time_ms, bindings) = batch.into_parts();
    DescribeAclsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        bindings.into_iter().map(translate_binding).collect(),
    )
}

fn translate_binding(binding: Binding) -> AclBinding {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        binding.into_parts();
    translate_binding_parts(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn translate_binding_parts(
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(
            AclResourceType::from_code(resource_type),
            resource_name,
            AclPatternType::from_code(pattern_type),
        ),
        AccessControlEntry::new(
            principal,
            host,
            AclOperation::from_code(operation),
            AclPermissionType::from_code(permission_type),
        ),
    )
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
            KafkaError::new(public, format!("DescribeAcls failed: {kind:?}"))
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
        (Some(message), true) => {
            format!("Kafka rejected DescribeAcls with broker code {code}: {message} [truncated]")
        }
        (Some(message), false) => {
            format!("Kafka rejected DescribeAcls with broker code {code}: {message}")
        }
        (None, _) => format!("Kafka rejected DescribeAcls with broker code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(delivery)
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
