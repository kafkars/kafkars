//! Hosted group registration result translation shape contract.

use kafka_client_engine::{
    GroupConsumerMissingOffsetPolicy, GroupConsumerRegistrationErrorKind, GroupConsumerStartError,
    GroupConsumerStartErrorKind,
};

use super::group_consumer_registration::engine_missing_offset_policy;
use super::group_consumer_registration_result::{
    translate_group_registration_kind, translate_group_start, translate_group_start_kind,
};
use crate::{ErrorKind, KafkaError, OffsetReset, RetryAdvice};

#[test]
fn every_engine_start_error_crosses_one_translation_function() {
    fn contract(error: GroupConsumerStartError) -> KafkaError {
        translate_group_start(error)
    }
    let _ = contract as fn(GroupConsumerStartError) -> KafkaError;
}

#[test]
fn only_transient_pre_admission_registration_categories_are_safe_to_retry() {
    for (kind, expected, retry) in [
        (
            GroupConsumerRegistrationErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerRegistrationErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            GroupConsumerRegistrationErrorKind::Backpressure,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            GroupConsumerRegistrationErrorKind::InvalidInput,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerRegistrationErrorKind::Internal,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_group_registration_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.retry_advice(), retry);
    }

    let contended =
        translate_group_registration_kind(GroupConsumerRegistrationErrorKind::Contended);
    let capacity =
        translate_group_registration_kind(GroupConsumerRegistrationErrorKind::Backpressure);
    assert!(contended.to_string().contains("temporarily contended"));
    assert!(capacity.to_string().contains("capacity is full"));
}

#[test]
fn only_transient_pre_core_start_contention_is_safe_to_retry() {
    for (kind, expected, retry) in [
        (
            GroupConsumerStartErrorKind::Closed,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerStartErrorKind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        (
            GroupConsumerStartErrorKind::AlreadyStarted,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerStartErrorKind::GroupUnavailable,
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerStartErrorKind::InvalidTimeout,
            ErrorKind::Configuration,
            RetryAdvice::DoNotRetry,
        ),
        (
            GroupConsumerStartErrorKind::Internal,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let error = translate_group_start_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.retry_advice(), retry);
    }
}

#[test]
fn every_missing_offset_choice_crosses_the_registration_boundary_exactly() {
    for (policy, expected) in [
        (OffsetReset::Error, GroupConsumerMissingOffsetPolicy::Error),
        (
            OffsetReset::Earliest,
            GroupConsumerMissingOffsetPolicy::Earliest,
        ),
        (
            OffsetReset::Latest,
            GroupConsumerMissingOffsetPolicy::Latest,
        ),
    ] {
        assert_eq!(engine_missing_offset_policy(policy), expected);
    }
}
