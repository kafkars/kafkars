//! Exhaustive stable translation of secret-bearing token descriptions.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        DelegationToken as PublicToken, DelegationTokenHmac as PublicHmac,
        DelegationTokenPrincipal as PublicPrincipal,
        DescribeDelegationTokensResult as PublicResult,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, Principal as EnginePrincipal,
        Result as EngineResult, Token as EngineToken,
    },
    operation::AdminDescribeDelegationTokensResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::DeadlineElapsed => ErrorKind::Timeout,
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::HostInvariant
        | AdmissionErrorKind::IdentityExhausted
        | AdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("DescribeDelegationTokens admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeDelegationTokens was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeDelegationTokens was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeDelegationTokensResult {
    match result {
        Ok(Outcome::Described(result)) => Ok(translate_success(result)),
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_success(result: EngineResult) -> PublicResult {
    let (throttle_time_ms, tokens) = result.into_parts();
    PublicResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        tokens.into_iter().map(translate_token).collect(),
    )
}

fn translate_token(token: EngineToken) -> PublicToken {
    let (
        owner,
        requester,
        renewers,
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        hmac,
    ) = token.into_parts();
    PublicToken::new(
        translate_principal(owner),
        requester.map(translate_principal),
        renewers.into_iter().map(translate_principal).collect(),
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        PublicHmac::new(hmac.into_bytes()),
    )
}

fn translate_principal(principal: EnginePrincipal) -> PublicPrincipal {
    let (principal_type, principal_name) = principal.into_parts();
    PublicPrincipal::new(principal_type, principal_name)
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code) = error.into_parts();
    translate_broker_error_parts(throttle_time_ms, code)
}

pub(super) fn translate_broker_error_parts(throttle_time_ms: u32, code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!(
            "Kafka rejected DescribeDelegationTokens with broker code {code} after \
             {throttle_time_ms} ms throttle"
        ),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
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
    KafkaError::new(public, format!("DescribeDelegationTokens failed: {kind:?}"))
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
