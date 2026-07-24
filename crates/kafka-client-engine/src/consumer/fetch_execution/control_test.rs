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
    driver::DriverOwner,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
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
