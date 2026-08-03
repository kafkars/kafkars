//! Exact stale-control release scenarios for accepted Fetch output reservations.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition,
    TopicId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{BrokerId, DriverOwner},
    protocol::fetch::{
        FetchDecodeLimits, FetchRequestSettings, FetchSessionRequest, FetchSessionUpdate,
    },
};

use super::{DirectFetchExecutor, FetchAttemptDeadline, FetchSubmission, PreparedFetchExecution};

#[test]
fn suspend_returns_request_storage_before_the_driver_call_finishes_draining() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let prepared = prepared(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 4_096);
    let mut driver = owner();

    assert!(matches!(
        executor
            .submit(&driver, &mut machine, prepared, Moment::from_tick(0))
            .unwrap_or_else(|error| panic!("accept Fetch: {error:?}")),
        FetchSubmission::Accepted
    ));
    assert_eq!(executor.retained(), (1, 1, 4_096));

    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause Fetch: {error}"));
    let [effect] = transition.effects() else {
        panic!("one suspend effect");
    };
    assert!(matches!(effect, AssignedConsumerEffect::Suspend { .. }));
    executor
        .observe_control(*effect)
        .unwrap_or_else(|error| panic!("observe stale control: {error:?}"));

    assert_eq!(executor.retained(), (1, 0, 0));
    shutdown(&mut driver);
    let recovery = executor.release_fetch_executor_after_driver_shutdown();
    assert!(!recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert_eq!(requests.len(), 0);
    assert_eq!(completion, None);
}

#[test]
fn revoke_retires_a_pending_broker_route_before_unassigned_dispatch() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 4_096);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve broker-routed Fetch state"));
    let mut driver = owner();
    assert!(matches!(
        executor
            .submit(
                &driver,
                &mut machine,
                prepared(effect),
                Moment::from_tick(0),
            )
            .unwrap_or_else(|error| panic!("admit route projection: {error:?}")),
        FetchSubmission::Accepted
    ));
    assert_eq!(executor.route_calls.len(), 1);

    let retirement = machine
        .apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: Some(fence.position().assignment_epoch()),
        })
        .unwrap_or_else(|error| panic!("retire assignment: {error}"));
    let [revoke] = retirement.effects() else {
        panic!("one exact revoke");
    };
    executor
        .observe_control(*revoke)
        .unwrap_or_else(|error| panic!("retire pending route: {error:?}"));

    assert!(executor.route_calls.is_empty());
    assert!(executor.routed.is_empty());
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
    let recovery = executor.release_fetch_executor_after_driver_shutdown();
    assert!(!recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert!(requests.is_empty());
    assert_eq!(completion, None);
}

#[test]
fn suspend_retires_only_the_exact_already_routed_request() {
    let (effects, mut machine) = two_partition_assignment();
    let first_fence = fetch_fence(effects[0]);
    let second_fence = fetch_fence(effects[1]);
    let broker = BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"));
    let mut executor = DirectFetchExecutor::create_unbound(2, 2, 8_192);
    executor
        .try_enable_sessions(2)
        .unwrap_or_else(|()| panic!("reserve broker-routed Fetch state"));
    executor.restore_routed(broker, prepared(effects[0]));
    executor.restore_routed(broker, prepared(effects[1]));
    assert_eq!(executor.routed.len(), 2);

    let suspension = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: first_fence.position().assignment_epoch(),
            partition: first_fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause first partition: {error}"));
    let [suspend] = suspension.effects() else {
        panic!("one exact suspend");
    };
    executor
        .observe_control(*suspend)
        .unwrap_or_else(|error| panic!("retire exact routed request: {error:?}"));

    assert_eq!(executor.routed.len(), 1);
    assert_eq!(executor.routed[0].request.fence(), second_fence);
    assert_eq!(executor.retained(), (1, 0, 0));
    let mut driver = owner();
    shutdown(&mut driver);
    let recovery = executor.release_fetch_executor_after_driver_shutdown();
    assert!(!recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].fence(), second_fence);
    assert_eq!(completion, None);
}

#[test]
fn position_control_fences_the_partition_session_before_new_fetch_work() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 4_096);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve session state"));
    let metadata =
        FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("valid session state"));
    executor.commit_fetch_session(fence, FetchSessionUpdate::Continue(metadata));

    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause session Fetch: {error}"));
    let [control] = transition.effects() else {
        panic!("one session control effect");
    };
    executor
        .observe_control(*control)
        .unwrap_or_else(|error| panic!("fence session: {error:?}"));

    let (mut request, _bytes) = prepared(effect).into_parts_for_test();
    executor.bind_fetch_session(&mut request);
    assert_eq!(request.session(), FetchSessionRequest::INITIAL);
}

fn assignment() -> (AssignedConsumerEffect, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                StartPosition::Offset(offset(10)),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    (transition.effects()[0], machine)
}

fn two_partition_assignment() -> (Vec<AssignedConsumerEffect>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![
                AssignedPartition::new(
                    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                    StartPosition::Offset(offset(10)),
                ),
                AssignedPartition::new(
                    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(4)),
                    StartPosition::Offset(offset(20)),
                ),
            ],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("two-partition assignment: {error}"));
    (transition.into_effects(), machine)
}

fn prepared(effect: AssignedConsumerEffect) -> PreparedFetchExecution {
    let fence = fetch_fence(effect);
    PreparedFetchExecution::new(
        effect,
        "events".to_owned(),
        FetchRequestSettings::new(500, 1, 1_048_576, 1_048_576, 0),
        FetchDecodeLimits::default(),
        FetchAttemptDeadline::from_parts_for_test(
            fence,
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(1_000_000_000),
                Instant::now() + Duration::from_secs(60),
            ),
        ),
        4_096,
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"))
}

fn fetch_fence(effect: AssignedConsumerEffect) -> kafka_client_core::FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver: {error}"))
}

fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}
