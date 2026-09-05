//! Route-phase backpressure, deadline, and shutdown ownership scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFailure, Moment, PartitionIndex, StartPosition, TopicId,
    partitioning::TopicMetadataGeneration,
};

use super::{
    DirectFetchExecutor, FetchSubmission,
    admission_test::{fetch_fence, offset, owner, prepared, shutdown},
    broker_execution::RoutedBrokerFetch,
};
use crate::driver::BrokerId;

#[test]
fn routed_retry_requires_metadata_newer_than_its_observed_generation() {
    let (effects, _machine) = assignment();
    let observed = TopicMetadataGeneration::from_raw(23);
    let mut retry = prepared(effects[0], 100, 4_096);
    retry
        .request
        .bind_cached_topic_route([7; 16], Some(9), Some(observed));
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 4_096);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve broker-routed Fetch state"));

    assert_eq!(executor.required_route_generation(&retry), Some(observed));
}

#[test]
fn later_fetch_is_retained_and_its_original_deadline_wins_retry() {
    let (effects, mut machine) = assignment();
    let first_fence = fetch_fence(effects[0]);
    let second_fence = fetch_fence(effects[1]);
    let mut executor = DirectFetchExecutor::create_unbound(2, 2, 8_192);
    executor
        .try_enable_sessions(2)
        .unwrap_or_else(|()| panic!("reserve broker-routed Fetch state"));
    let mut driver = owner();

    assert!(matches!(
        executor
            .submit(
                &driver,
                &mut machine,
                prepared(effects[0], 100, 4_096),
                Moment::from_tick(0),
            )
            .unwrap_or_else(|error| panic!("admit first route: {error:?}")),
        FetchSubmission::Accepted
    ));
    let second = executor
        .submit(
            &driver,
            &mut machine,
            prepared(effects[1], 100, 4_096),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("retain second route: {error:?}"));
    let FetchSubmission::Backpressured(second) = second else {
        panic!("one active route projection must retain the later Fetch");
    };
    assert_eq!(second.fence(), second_fence);
    assert_eq!(second.deadline(), Deadline::from_tick(100));
    assert_eq!(executor.retained(), (1, 0, 0));

    let elapsed = executor
        .submit(&driver, &mut machine, second, Moment::from_tick(100))
        .unwrap_or_else(|error| panic!("settle retained deadline: {error:?}"));
    let FetchSubmission::Settled(Some(transition)) = elapsed else {
        panic!("elapsed retained Fetch must settle before route backpressure");
    };
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::FetchFailed {
            fence: second_fence,
            failure: FetchFailure::DeadlineElapsed,
        }]
    );

    shutdown(&mut driver);
    let recovery = executor.release_fetch_executor_after_driver_shutdown();
    assert!(!recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].fence(), first_fence);
    assert_eq!(completion, None);
}

#[test]
fn routed_fetch_retired_before_submission_releases_without_fault() {
    let (effects, mut machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(2, 2, 8_192);
    executor
        .try_enable_sessions(2)
        .unwrap_or_else(|()| panic!("reserve broker-routed Fetch state"));
    let (request, hard_output_bytes) = prepared(effects[0], 100, 4_096).into_parts();
    executor.routed.push(RoutedBrokerFetch {
        broker_id: BrokerId::from_raw(1).unwrap_or_else(|error| panic!("broker: {error:?}")),
        request,
        hard_output_bytes,
    });
    machine
        .apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: machine.assignment_epoch(),
        })
        .unwrap_or_else(|error| panic!("retire assignment: {error}"));

    let (_transition, progressed) = executor
        .drive_broker_fetches(
            &owner(),
            &mut machine,
            &crate::clock::MonotonicClock::new(),
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("discard retired route: {error:?}"));
    assert!(progressed);
    assert_eq!(executor.retained(), (0, 0, 0));
}

#[test]
fn older_routed_fetch_deadline_is_not_hidden_by_later_broker_work() {
    let (effects, mut machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(2, 2, 8_192);
    executor
        .try_enable_sessions(2)
        .unwrap_or_else(|()| panic!("reserve broker-routed Fetch state"));
    for (broker_id, effect) in [1, 2].into_iter().zip(effects.iter().copied()) {
        let broker =
            BrokerId::from_raw(broker_id).unwrap_or_else(|error| panic!("broker: {error:?}"));
        executor.restore_routed(broker, prepared(effect, 100, 4_096));
    }
    let mut driver = owner();
    let clock = crate::clock::MonotonicClock::new();

    for effect in effects {
        let (transition, progressed) = executor
            .drive_broker_fetches(&driver, &mut machine, &clock, Moment::from_tick(100))
            .unwrap_or_else(|error| panic!("settle oldest routed deadline: {error:?}"));
        let transition = transition.unwrap_or_else(|| panic!("elapsed Fetch must settle"));
        assert!(progressed);
        assert_eq!(
            transition.effects(),
            &[AssignedConsumerEffect::FetchFailed {
                fence: fetch_fence(effect),
                failure: FetchFailure::DeadlineElapsed,
            }]
        );
    }
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
}

fn assignment() -> (Vec<AssignedConsumerEffect>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(3, 10), assigned(4, 20)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("batch direct assignment: {error}"));
    (transition.into_effects(), machine)
}

fn assigned(partition: u32, next_offset: i64) -> AssignedPartition {
    AssignedPartition::new(
        AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(partition)),
        StartPosition::Offset(offset(next_offset)),
    )
}
