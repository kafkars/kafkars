//! Exhaustive stable translation of engine-owned DeleteAcls outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{DeleteAclFilterOutcome, DeleteAclsResult},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, DeliveryStatus, Failure,
        FailureKind, ObserverError, Outcome,
    },
    operation::AdminDeleteAclsResult,
    value::translate_filter_outcome,
};

pub(super) struct PreparedDeleteAclsOutcomes {
    expected: usize,
    outcomes: Vec<DeleteAclFilterOutcome>,
}

impl PreparedDeleteAclsOutcomes {
    pub(super) fn try_new(expected: usize) -> Result<Self, ()> {
        let mut outcomes = Vec::new();
        outcomes.try_reserve_exact(expected).map_err(|_error| ())?;
        Ok(Self { expected, outcomes })
    }
}

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
    KafkaError::new(public, format!("DeleteAcls admission failed: {kind:?}"))
        .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DeleteAcls was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DeleteAcls was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
    prepared: Option<PreparedDeleteAclsOutcomes>,
) -> AdminDeleteAclsResult {
    match result {
        Ok(Outcome::Deleted(batch)) => {
            translate_batch(batch, prepared.ok_or_else(missing_prepared_outer)?)
        }
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(
    batch: Batch,
    mut prepared: PreparedDeleteAclsOutcomes,
) -> AdminDeleteAclsResult {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    if outcomes.len() != prepared.expected
        || prepared
            .outcomes
            .capacity()
            .saturating_sub(prepared.outcomes.len())
            < outcomes.len()
    {
        return Err(missing_prepared_outer());
    }
    for outcome in outcomes {
        prepared.outcomes.push(translate_filter_outcome(outcome)?);
    }
    Ok(DeleteAclsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        prepared.outcomes,
    ))
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
    KafkaError::new(public, format!("DeleteAcls failed: {kind:?}"))
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

fn missing_prepared_outer() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "DeleteAcls terminal did not match prepared public filter storage",
    )
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
}
