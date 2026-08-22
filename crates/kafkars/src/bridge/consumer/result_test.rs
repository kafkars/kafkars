//! Exhaustive category tests for assigned-consumer engine translation.

use kafka_client_engine::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerClaimError,
    AssignedConsumerCloseObserverError, AssignedConsumerTryCloseErrorKind,
};

use super::result::{
    translate_assigned_close_admission_kind, translate_assigned_close_fault,
    translate_assigned_close_observation, translate_assigned_consumer_claim,
};
use crate::{ErrorKind, RetryAdvice};

#[test]
fn one_shot_claim_failures_have_stable_facade_categories() {
    assert_eq!(
        translate_assigned_consumer_claim(AssignedConsumerClaimError::AlreadyClaimed).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_assigned_consumer_claim(AssignedConsumerClaimError::Poisoned).kind(),
        ErrorKind::Internal
    );
}

#[test]
fn every_close_admission_kind_is_translated() {
    for (kind, expected, retry) in [
        (
            AssignedConsumerTryCloseErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            AssignedConsumerTryCloseErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            AssignedConsumerTryCloseErrorKind::CompletionCapacity,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            AssignedConsumerTryCloseErrorKind::Pending,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            AssignedConsumerTryCloseErrorKind::HostUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        (
            AssignedConsumerTryCloseErrorKind::InternalInvariant,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_assigned_close_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.retry_advice(), retry);
    }
}

#[test]
fn accepted_fault_and_observation_failures_are_exhaustive() {
    assert_eq!(
        translate_assigned_close_fault(AssignedConsumerAcceptedFaultKind::Wake).kind(),
        ErrorKind::Internal
    );
    for (error, expected) in [
        (
            AssignedConsumerCloseObserverError::ExecutionUnavailable,
            ErrorKind::Internal,
        ),
        (
            AssignedConsumerCloseObserverError::AlreadyObserved,
            ErrorKind::State,
        ),
        (
            AssignedConsumerCloseObserverError::Stale,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(translate_assigned_close_observation(error).kind(), expected);
    }
}
