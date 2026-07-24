//! Exhaustive category tests for assigned-consumer engine translation.

use kafka_client_engine::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerClaimError,
    AssignedConsumerCloseObserverError, AssignedConsumerTryCloseErrorKind,
};

use super::consumer_result::{
    translate_assigned_close_admission_kind, translate_assigned_close_fault,
    translate_assigned_close_observation, translate_assigned_consumer_claim,
};
use crate::ErrorKind;

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
    for (kind, expected) in [
        (
            AssignedConsumerTryCloseErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (AssignedConsumerTryCloseErrorKind::Closed, ErrorKind::State),
        (
            AssignedConsumerTryCloseErrorKind::CompletionCapacity,
            ErrorKind::Backpressure,
        ),
        (
            AssignedConsumerTryCloseErrorKind::Pending,
            ErrorKind::Backpressure,
        ),
        (
            AssignedConsumerTryCloseErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
        (
            AssignedConsumerTryCloseErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(
            translate_assigned_close_admission_kind(kind).kind(),
            expected
        );
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
