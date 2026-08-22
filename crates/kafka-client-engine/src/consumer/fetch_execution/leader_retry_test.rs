//! KIP-951 leader-hint and metadata-fallback replacement ownership scenarios.

use std::sync::Arc;

use crate::{
    EngineConfig,
    consumer::assigned_event::{AssignedConsumerEventStore, test_support::event_store},
    driver::{BrokerId, DriverOwner},
};
use kafka_client_core::{AssignedConsumerEffect, FetchFailure, FetchOwnership, Moment};

use super::{
    broker_execution::ActiveBrokerSession,
    broker_session::BrokerSessionMember,
    executor::DirectFetchExecutor,
    settlement_test::{OUTPUT_BYTES, assignment, fetch_fence, prepared},
};

#[test]
fn exact_leader_movement_codes_replace_same_offset_with_a_fresh_fence() {
    for code in [6, 74] {
        let (mut executor, mut machine, mut events, old, original_deadline) =
            terminal(code, Some((4, 10)));
        let driver = driver();

        assert_eq!(
            executor
                .poll_with_driver(&driver, &mut machine, &mut events, Moment::from_tick(8))
                .unwrap_or_else(|error| panic!("leader retry: {error:?}")),
            None
        );
        assert_eq!(machine.fetch_ownership(old), Ok(FetchOwnership::Superseded));
        assert!(executor.active_broker_sessions.is_empty());
        assert_eq!(executor.retained().1, 0);
        assert_eq!(executor.leader_recovery.retained(), 1);
        let (_transition, progressed) = executor
            .drive_broker_fetches(
                &driver,
                &mut machine,
                &crate::clock::MonotonicClock::new(),
                Moment::from_tick(9),
            )
            .unwrap_or_else(|error| panic!("hinted replacement: {error:?}"));
        assert!(progressed);
        let routed = executor
            .routed
            .pop()
            .unwrap_or_else(|| panic!("hinted broker route"));
        assert_eq!(routed.broker_id, broker(4));
        assert_eq!(routed.request.next_offset().get(), 10);
        assert_eq!(routed.request.operation_deadline(), original_deadline);
        assert_eq!(routed.request.leader_epoch(), Some(10));
        assert_ne!(routed.request.fence(), old);
        events
            .observe_effect(AssignedConsumerEffect::AuthorizeFetchDelivery {
                fence: routed.request.fence(),
                next_offset: routed.request.next_offset(),
            })
            .unwrap_or_else(|error| panic!("replacement event claim: {error:?}"));
        assert_eq!(
            machine.fetch_ownership(routed.request.fence()),
            Ok(FetchOwnership::Active)
        );
    }
}

#[test]
fn absent_or_stale_hint_falls_back_to_topic_metadata_under_the_original_deadline() {
    for leader in [None, Some((4, 8))] {
        let (mut executor, mut machine, mut events, old, original_deadline) = terminal(6, leader);
        let driver = driver();
        assert!(
            executor
                .poll_with_driver(&driver, &mut machine, &mut events, Moment::from_tick(8))
                .unwrap_or_else(|error| panic!("fallback retry: {error:?}"))
                .is_none()
        );
        assert_eq!(machine.fetch_ownership(old), Ok(FetchOwnership::Superseded));
        assert!(executor.routed.is_empty());
        assert_eq!(executor.leader_recovery.retained(), 1);

        let (_transition, progressed) = executor
            .drive_broker_fetches(
                &driver,
                &mut machine,
                &crate::clock::MonotonicClock::new(),
                Moment::from_tick(9),
            )
            .unwrap_or_else(|error| panic!("metadata fallback: {error:?}"));
        assert!(progressed);
        assert_eq!(executor.route_calls.len(), 1);
        let recovered = executor
            .route_calls
            .pop()
            .unwrap_or_else(|| panic!("route call"));
        let request = recovered.call.recover_after_driver_shutdown();
        assert_eq!(request.operation_deadline(), original_deadline);
        assert_eq!(request.next_offset().get(), 10);
        assert_ne!(request.fence(), old);
    }
}

#[test]
fn unknown_leader_epoch_retries_the_same_broker_without_the_transient_epoch() {
    let (mut executor, mut machine, mut events, old, original_deadline) = terminal(75, None);
    let driver = driver();

    assert!(
        executor
            .poll_with_driver(&driver, &mut machine, &mut events, Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("unknown leader epoch retry: {error:?}"))
            .is_none()
    );
    let (_transition, progressed) = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &crate::clock::MonotonicClock::new(),
            Moment::from_tick(9),
        )
        .unwrap_or_else(|error| panic!("same-broker retry: {error:?}"));
    assert!(progressed);
    let routed = executor
        .routed
        .pop()
        .unwrap_or_else(|| panic!("same-broker retry route"));
    assert_eq!(routed.broker_id, broker(3));
    assert_eq!(routed.request.next_offset().get(), 10);
    assert_eq!(routed.request.operation_deadline(), original_deadline);
    assert_eq!(routed.request.leader_epoch(), None);
    assert_ne!(routed.request.fence(), old);
}

#[test]
fn other_partition_codes_remain_terminal_broker_failures() {
    let (mut executor, mut machine, mut events, old, _deadline) = terminal(5, Some((4, 10)));
    let transition = executor
        .poll_with_driver(&driver(), &mut machine, &mut events, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("terminal broker code: {error:?}"))
        .unwrap_or_else(|| panic!("terminal broker transition"));
    assert!(matches!(
        transition.effects(),
        [kafka_client_core::AssignedConsumerEffect::FetchFailed {
            fence,
            failure: FetchFailure::Broker(code),
        }] if *fence == old && code.get() == 5
    ));
}

fn terminal(
    error_code: i16,
    leader: Option<(i32, i32)>,
) -> (
    DirectFetchExecutor,
    kafka_client_core::AssignedConsumerMachine,
    AssignedConsumerEventStore,
    kafka_client_core::FetchFence,
    crate::clock::OperationDeadline,
) {
    let (effect, machine) = assignment();
    let fence = fetch_fence(effect);
    let events = claims(effect);
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
    executor.broker_calls.install_leader_movement_for_test(
        request,
        Moment::from_tick(7),
        error_code,
        leader,
    );
    executor.active_broker_sessions.push(ActiveBrokerSession {
        fences: vec![fence],
        plan,
        update: None,
        reset: false,
    });
    (executor, machine, events, fence, original_deadline)
}

pub(super) fn claims(effect: AssignedConsumerEffect) -> AssignedConsumerEventStore {
    let mut events = event_store(1);
    let prepared = events
        .prepare_partition(fetch_fence(effect).position().partition())
        .unwrap_or_else(|error| panic!("prepare event claim: {error:?}"));
    prepared
        .commit_event_claims(&[effect])
        .unwrap_or_else(|error| panic!("commit event claim: {error:?}"));
    events
}

pub(super) fn broker(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("broker: {error}"))
}

pub(super) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"))
}
