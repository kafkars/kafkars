//! Exhaustive stable failure-category translation scenarios.

use kafka_client_engine::{
    AssignedConsumerFetchFailureKind as EngineFetchFailure,
    AssignedConsumerFetchThrottleFailureKind as EngineFetchThrottleFailure,
    AssignedConsumerPositionResolutionFailureKind as EnginePositionFailure,
};

use super::event::{
    translate_fetch_failure, translate_fetch_throttle_failure, translate_position_failure,
};
use crate::consumer::{
    AssignedConsumerFetchFailureKind, AssignedConsumerFetchThrottleFailureKind,
    AssignedConsumerPositionResolutionFailureKind,
};

#[test]
fn every_position_category_and_signed_broker_code_translate_exactly() {
    for (engine, facade) in [
        (
            EnginePositionFailure::DeadlineElapsed,
            AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed,
        ),
        (
            EnginePositionFailure::DriverRejected,
            AssignedConsumerPositionResolutionFailureKind::DriverRejected,
        ),
        (
            EnginePositionFailure::Transport,
            AssignedConsumerPositionResolutionFailureKind::Transport,
        ),
        (
            EnginePositionFailure::Broker(-42),
            AssignedConsumerPositionResolutionFailureKind::Broker(-42),
        ),
        (
            EnginePositionFailure::Compatibility,
            AssignedConsumerPositionResolutionFailureKind::Compatibility,
        ),
        (
            EnginePositionFailure::InvalidResponse,
            AssignedConsumerPositionResolutionFailureKind::InvalidResponse,
        ),
        (
            EnginePositionFailure::ResponseTooLarge,
            AssignedConsumerPositionResolutionFailureKind::ResponseTooLarge,
        ),
        (
            EnginePositionFailure::ThrottleDeadlineOverflow,
            AssignedConsumerPositionResolutionFailureKind::ThrottleDeadlineOverflow,
        ),
    ] {
        assert_eq!(translate_position_failure(engine), facade);
    }
}

#[test]
fn throttle_category_translates_without_fallbacks() {
    assert_eq!(
        translate_fetch_throttle_failure(EngineFetchThrottleFailure::DeadlineOverflow),
        AssignedConsumerFetchThrottleFailureKind::DeadlineOverflow
    );
}

#[test]
fn every_fetch_category_and_signed_broker_code_translate_exactly() {
    for (engine, facade) in [
        (
            EngineFetchFailure::DeadlineElapsed,
            AssignedConsumerFetchFailureKind::DeadlineElapsed,
        ),
        (
            EngineFetchFailure::DriverRejected,
            AssignedConsumerFetchFailureKind::DriverRejected,
        ),
        (
            EngineFetchFailure::Transport,
            AssignedConsumerFetchFailureKind::Transport,
        ),
        (
            EngineFetchFailure::Broker(-42),
            AssignedConsumerFetchFailureKind::Broker(-42),
        ),
        (
            EngineFetchFailure::Compatibility,
            AssignedConsumerFetchFailureKind::Compatibility,
        ),
        (
            EngineFetchFailure::InvalidResponse,
            AssignedConsumerFetchFailureKind::InvalidResponse,
        ),
        (
            EngineFetchFailure::ResponseTooLarge,
            AssignedConsumerFetchFailureKind::ResponseTooLarge,
        ),
    ] {
        assert_eq!(translate_fetch_failure(engine), facade);
    }
}
