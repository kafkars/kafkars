//! Only definitely-unsent controller facts may reach the bounded core retry policy.

use std::time::{Duration, Instant};

use kafka_client_core::{CreateTopicsInput, Deadline, DeliveryStatus, Moment, OperationId};
use kafka_driver::{CallFailure, Delivery, RequestError};
use kafka_wire::CreateTopicsResponse;

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{
    create_topics_calls::TrackedCreateTopicsCalls,
    create_topics_controller_refresh::{CreateTopicsControllerRefresh, retryable},
    create_topics_visibility::SettledCreateTopicsCall,
};

#[test]
fn replay_requires_an_exact_definitely_unsent_route_failure() {
    assert!(retryable(&Err(RequestError::RouteUnavailable)));
    for (failure, delivery, expected) in [
        (CallFailure::NotReady, Delivery::NotSent, true),
        (CallFailure::NotReady, Delivery::PossiblySent, false),
        (CallFailure::DeadlineExceeded, Delivery::NotSent, false),
    ] {
        assert_eq!(
            retryable(&Err(RequestError::Rejected { failure, delivery })),
            expected
        );
    }
    assert!(!retryable(&Ok(CreateTopicsResponse::default())));
}

#[test]
fn observed_route_failure_without_its_capability_cannot_authorize_refresh() {
    assert!(
        CreateTopicsControllerRefresh::from_terminal(
            &Err(RequestError::Rejected {
                failure: CallFailure::NotReady,
                delivery: Delivery::NotSent
            }),
            &mut None,
            deadline(),
        )
        .is_none()
    );
}

#[test]
fn controller_refresh_retains_one_completion_slot_and_original_deadline() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let refresh = CreateTopicsControllerRefresh::from_terminal(
        &Err(RequestError::RouteUnavailable),
        &mut None,
        deadline(),
    )
    .unwrap_or_else(|| panic!("unrouted failure must reach core retry policy"));
    let mut calls = TrackedCreateTopicsCalls::new(1);
    calls.retain_settled_for_test(SettledCreateTopicsCall::new(
        OperationId::from_raw(7),
        CreateTopicsInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        },
        None,
        Vec::new(),
        Some(refresh),
    ));
    assert_eq!(calls.retained_count(), 1);
    assert!(calls.try_reserve().is_none());
    assert!(
        calls
            .poll_next_ready()
            .unwrap_or_else(|error| panic!("poll: {error}"))
            .is_none()
    );
    assert_eq!(calls.next_deadline(), Some(Deadline::from_tick(10)));
    assert!(calls.advance_one_settlement(&driver, Moment::from_tick(10)));
    assert_eq!(
        calls.take_ready_input_for_test(),
        Some((
            OperationId::from_raw(7),
            CreateTopicsInput::ControllerRouteUnavailable {
                now: Moment::from_tick(10)
            },
        ))
    );
    assert!(!calls.advance_one_settlement(&driver, Moment::from_tick(11)));
    assert_eq!(calls.retained_count(), 1);
    assert!(calls.discard_settled(OperationId::from_raw(7)));
    assert_eq!(calls.retained_count(), 0);
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(5),
    )
}
