//! Route-phase backpressure, deadline, and shutdown ownership scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFailure, Moment, PartitionIndex, StartPosition, TopicId,
};

use super::{
    DirectFetchExecutor, FetchSubmission,
    admission_test::{fetch_fence, offset, owner, prepared, shutdown},
};

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
