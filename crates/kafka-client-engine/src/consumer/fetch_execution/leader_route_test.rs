//! KIP-951 route-refresh, metadata retry, and closed-call recovery scenarios.

use std::sync::Arc;

use crate::driver::FetchRouteRefresh;
use kafka_client_core::{FetchOwnership, Moment};

use super::{
    broker_execution::ActiveBrokerSession,
    broker_session::BrokerSessionMember,
    executor::DirectFetchExecutor,
    leader_retry_test::{broker, claims, driver},
    settlement_test::{OUTPUT_BYTES, assignment, fetch_fence, prepared},
};

#[test]
fn failed_cache_invalidation_preserves_the_exact_newer_leader_hint() {
    let (effect, mut machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("session capacity"));
    executor.leader_recovery.begin(
        Some(FetchRouteRefresh::Unavailable),
        Some(prepared(effect)),
        Some(broker(4)),
    );
    let driver = driver();
    let clock = crate::clock::MonotonicClock::new();

    let (_transition, invalidation_progressed) = executor
        .drive_broker_fetches(&driver, &mut machine, &clock, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("failed invalidation: {error:?}"));
    assert!(invalidation_progressed);
    let (_transition, route_progressed) = executor
        .drive_broker_fetches(&driver, &mut machine, &clock, Moment::from_tick(9))
        .unwrap_or_else(|error| panic!("hint route: {error:?}"));
    assert!(route_progressed);
    let routed = executor
        .routed
        .pop()
        .unwrap_or_else(|| panic!("retained hinted route"));
    assert_eq!(routed.broker_id, broker(4));
}

#[test]
fn temporarily_leaderless_topic_view_retains_the_same_fetch_attempt() {
    let (effect, machine) = assignment();
    let fence = fetch_fence(effect);
    let prepared = prepared(effect);
    let deadline = prepared.deadline();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("session capacity"));

    executor
        .retain_topic_route_retry(prepared)
        .unwrap_or_else(|_prepared| panic!("retain metadata retry"));
    let waiting = executor
        .leader_recovery
        .take_waiting()
        .unwrap_or_else(|| panic!("retained metadata retry"));
    let super::route_refresh::WaitingLeaderRoute::Ready {
        prepared,
        hinted_broker: None,
    } = waiting
    else {
        panic!("metadata retry must not invent a leader hint");
    };
    assert_eq!(prepared.fence(), fence);
    assert_eq!(prepared.deadline(), deadline);
    assert_eq!(machine.fetch_ownership(fence), Ok(FetchOwnership::Active));
}

#[test]
fn closed_driver_completion_retries_the_same_offset_through_topic_metadata() {
    let (effect, machine) = assignment();
    let fence = fetch_fence(effect);
    let mut events = claims(effect);
    let source_broker = broker(3);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("session capacity"));
    let member = BrokerSessionMember::new(fence.position(), Arc::from("events"), [7; 16]);
    let plan = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .try_begin(source_broker, vec![member])
        .unwrap_or_else(|(error, _)| panic!("broker plan: {error:?}"));
    let mut prepared = prepared(effect);
    prepared.request.bind_topic_route_for_test([7; 16], Some(9));
    prepared.request.bind_session(plan.session());
    let (request, bytes) = prepared.into_parts_for_test();
    let original_deadline = request.operation_deadline();
    executor
        .reserve_output_for_test(fence, bytes)
        .unwrap_or_else(|error| panic!("output: {error:?}"));
    executor
        .broker_calls
        .install_closed_completion_for_test(request);
    executor.active_broker_sessions.push(ActiveBrokerSession {
        fences: vec![fence],
        plan,
        update: None,
        reset: false,
    });
    let driver = driver();
    let mut machine = machine;

    assert!(
        executor
            .poll_with_driver(&driver, &mut machine, &mut events, Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("closed completion retry: {error:?}"))
            .is_none()
    );
    assert_eq!(
        machine.fetch_ownership(fence),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(executor.leader_recovery.retained(), 1);
    let (_transition, progressed) = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &crate::clock::MonotonicClock::new(),
            Moment::from_tick(9),
        )
        .unwrap_or_else(|error| panic!("closed completion metadata route: {error:?}"));
    assert!(progressed);
    let recovered = executor
        .route_calls
        .pop()
        .unwrap_or_else(|| panic!("metadata route call"));
    let request = recovered.call.recover_after_driver_shutdown();
    assert_eq!(request.operation_deadline(), original_deadline);
    assert_eq!(request.next_offset().get(), 10);
    assert_ne!(request.fence(), fence);
}
