//! Exhaustive stable translation of engine-owned CreateAcls outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        AccessControlEntry, AclBinding, AclOperation, AclPatternType, AclPermissionType,
        AclResourceType, CreateAclBrokerError, CreateAclOutcome, CreateAclResult, CreateAclsResult,
        ResourcePattern,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AclOutcome, AclResult, AdmissionError, AdmissionErrorKind, Batch,
        Binding, BrokerError, DeliveryStatus, Failure, FailureKind, ObserverError, Outcome,
    },
    operation::AdminCreateAclsResult,
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
    KafkaError::new(public, format!("CreateAcls admission failed: {kind:?}"))
        .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "CreateAcls was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "CreateAcls was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
    prepared: Option<PreparedCreateAclsOutcomes>,
) -> AdminCreateAclsResult {
    match result {
        Ok(Outcome::Created(batch)) => translate_batch(
            batch,
            prepared.ok_or_else(missing_prepared_result_capacity)?,
        ),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) struct PreparedCreateAclsOutcomes {
    expected: usize,
    outcomes: Vec<CreateAclOutcome>,
}

impl PreparedCreateAclsOutcomes {
    pub(super) fn try_new(expected: usize) -> Result<Self, ()> {
        let mut outcomes = Vec::new();
        outcomes.try_reserve_exact(expected).map_err(|_error| ())?;
        Ok(Self { expected, outcomes })
    }
}

fn translate_batch(
    batch: Batch,
    mut prepared: PreparedCreateAclsOutcomes,
) -> AdminCreateAclsResult {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    if outcomes.len() != prepared.expected
        || prepared
            .outcomes
            .capacity()
            .saturating_sub(prepared.outcomes.len())
            < outcomes.len()
    {
        return Err(missing_prepared_result_capacity());
    }
    for outcome in outcomes {
        prepared.outcomes.push(translate_acl_outcome(outcome));
    }
    Ok(CreateAclsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        prepared.outcomes,
    ))
}

fn translate_acl_outcome(outcome: AclOutcome) -> CreateAclOutcome {
    let (binding, result) = outcome.into_parts();
    CreateAclOutcome::new(translate_binding(binding), translate_acl_result(result))
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

fn translate_acl_result(result: AclResult) -> CreateAclResult {
    match result {
        AclResult::Created => CreateAclResult::Created,
        AclResult::BrokerFailed(error) => {
            CreateAclResult::BrokerFailed(translate_broker_error(error))
        }
    }
}

fn translate_broker_error(error: BrokerError) -> CreateAclBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    translate_broker_parts(code, message, message_truncated)
}

pub(super) fn translate_broker_parts(
    code: i16,
    message: Option<String>,
    message_truncated: bool,
) -> CreateAclBrokerError {
    CreateAclBrokerError::new(code, message, message_truncated)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("CreateAcls failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
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

fn missing_prepared_result_capacity() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "CreateAcls terminal did not match its prepared public result capacity",
    )
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
}
