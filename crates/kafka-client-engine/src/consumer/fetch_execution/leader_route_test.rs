//! KIP-951 route-refresh, metadata retry, and closed-call recovery scenarios.

use std::sync::Arc;

use crate::driver::FetchRouteRefresh;
use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchOwnership, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};

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
        None,
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
        failure_token: None,
    } = waiting
    else {
        panic!("metadata retry must not invent a leader hint");
    };
    assert_eq!(prepared.fence(), fence);
    assert_eq!(prepared.deadline(), deadline);
    assert_eq!(machine.fetch_ownership(fence), Ok(FetchOwnership::Active));
}

#[test]
fn incremental_revoke_retires_only_the_matching_leader_recovery() {
    let (effects, mut machine) = two_partition_assignment();
    let removed = fetch_fence(effects[0]);
    let survivor = fetch_fence(effects[1]);
    let mut executor = DirectFetchExecutor::create_unbound(2, 2, 8_192);
    executor
        .try_enable_sessions(2)
        .unwrap_or_else(|()| panic!("reserve leader recovery"));
    for effect in effects {
        executor.leader_recovery.begin(
            Some(FetchRouteRefresh::Unavailable),
            Some(prepared(effect)),
            None,
            None,
        );
    }
    assert_eq!(executor.leader_recovery.retained(), 4);

    let transition = machine
        .apply(AssignedConsumerInput::RemoveAssignments {
            partitions: vec![removed.position().partition()],
        })
        .unwrap_or_else(|error| panic!("incremental removal: {error}"));
    executor
        .observe_control(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe incremental revoke: {error:?}"));
    assert_eq!(executor.leader_recovery.retained(), 2);

    let driver = driver();
    assert!(executor.leader_recovery.poll(&driver));
    let waiting = executor
        .leader_recovery
        .take_waiting()
        .unwrap_or_else(|| panic!("surviving leader recovery"));
    let prepared = match waiting {
        super::route_refresh::WaitingLeaderRoute::Ready { prepared, .. }
        | super::route_refresh::WaitingLeaderRoute::Failed { prepared, .. } => prepared,
    };
    assert_eq!(prepared.fence(), survivor);
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
    assert_eq!(request.topic_id(), Some([7; 16]));
}

fn two_partition_assignment() -> (Vec<AssignedConsumerEffect>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![entry(3, 10), entry(4, 20)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("two-partition assignment: {error}"));
    (transition.into_effects(), machine)
}

fn entry(partition: u32, offset: i64) -> AssignedPartition {
    AssignedPartition::new(
        AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(partition)),
        StartPosition::Offset(
            NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("nonnegative offset")),
        ),
    )
}
