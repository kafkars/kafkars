//! Deadline, isolation, and bounded-capacity Fetch admission scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFailure, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{
    DirectFetchExecutor, FetchAttemptDeadline, FetchSubmission, PrepareFetchError,
    PreparedFetchExecution,
};

#[test]
fn non_fetch_effect_is_rejected_as_caller_invariant_misuse() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause Fetch: {error}"));
    let [effect] = transition.effects() else {
        panic!("one suspend effect");
    };
    let error = PreparedFetchExecution::new(
        *effect,
        "events".to_owned(),
        settings(0),
        FetchDecodeLimits::default(),
        FetchAttemptDeadline::from_parts_for_test(fence, operation_deadline(100)),
        4_096,
    )
    .err()
    .unwrap_or_else(|| panic!("non-Fetch effect must fail preparation"));

    assert_eq!(error, PrepareFetchError::UnexpectedEffect);
}

#[test]
fn read_committed_reaches_bounded_output_admission() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let prepared = PreparedFetchExecution::new(
        effect,
        "events".to_owned(),
        settings(1),
        FetchDecodeLimits::default(),
        FetchAttemptDeadline::from_parts_for_test(fence, operation_deadline(100)),
        4_096,
    )
    .unwrap_or_else(|error| panic!("prepare read-committed Fetch: {error:?}"));
    let mut executor = DirectFetchExecutor::create_unbound(0, 0, 0);
    let mut driver = owner();

    let submission = executor
        .submit(&driver, &mut machine, prepared, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("admit read-committed Fetch: {error:?}"));
    let FetchSubmission::Backpressured(prepared) = submission else {
        panic!("read-committed Fetch must reach bounded output admission");
    };
    assert_eq!(prepared.fence(), fence);
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
}

#[test]
fn elapsed_deadline_settles_before_zero_store_and_call_capacity() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(10));
    let fence = fetch_fence(effect);
    let prepared = prepared(effect, 10, 4_096);
    let mut executor = DirectFetchExecutor::create_unbound(0, 0, 0);
    let mut driver = owner();

    let submission = executor
        .submit(&driver, &mut machine, prepared, Moment::from_tick(10))
        .unwrap_or_else(|error| panic!("deadline settlement: {error:?}"));
    let FetchSubmission::Settled(Some(transition)) = submission else {
        panic!("elapsed deadline must settle through core");
    };
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::FetchFailed {
            fence,
            failure: FetchFailure::DeadlineElapsed,
        }]
    );
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
}

#[test]
fn backpressured_fetch_superseded_before_retry_settles_without_driver_admission() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(10));
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 0, 0);
    let mut driver = owner();
    let submission = executor
        .submit(
            &driver,
            &mut machine,
            prepared(effect, 10, 4_096),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("initial backpressure: {error:?}"));
    let FetchSubmission::Backpressured(backpressured) = submission else {
        panic!("zero store capacity must return exact prepared ownership");
    };

    machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
            position: StartPosition::Offset(offset(20)),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("supersede backpressured Fetch: {error}"));
    assert!(matches!(
        prepared(effect, 10, 4_096).reconcile_ownership(&machine),
        Ok(None)
    ));
    assert!(matches!(
        executor
            .submit(&driver, &mut machine, backpressured, Moment::from_tick(10),)
            .unwrap_or_else(|error| panic!("stale retry: {error:?}")),
        FetchSubmission::Settled(None)
    ));
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
}

#[test]
fn unavailable_output_capacity_returns_the_exact_prepared_fetch() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let prepared = prepared(effect, 100, 4_096);
    let mut executor = DirectFetchExecutor::create_unbound(1, 0, 0);
    let mut driver = owner();

    let submission = executor
        .submit(&driver, &mut machine, prepared, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("bounded admission: {error:?}"));
    let FetchSubmission::Backpressured(prepared) = submission else {
        panic!("store pressure must return prepared ownership");
    };
    assert_eq!(prepared.fence(), fence);
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
}

#[test]
fn call_capacity_backpressure_rolls_back_output_reservation_and_returns_prepared() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let prepared = prepared(effect, 100, 4_096);
    let mut executor = DirectFetchExecutor::create_unbound(0, 1, 4_096);
    let mut driver = owner();

    let submission = executor
        .submit(&driver, &mut machine, prepared, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("bounded call admission: {error:?}"));
    let FetchSubmission::Backpressured(prepared) = submission else {
        panic!("call pressure must return exact prepared ownership");
    };
    assert_eq!(prepared.fence(), fence);
    assert_eq!(executor.retained(), (0, 0, 0));
    shutdown(&mut driver);
}

pub(super) fn assignment(
    partition: u32,
    deadline: Deadline,
) -> (AssignedConsumerEffect, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(partition),
                ),
                StartPosition::Offset(offset(10)),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: deadline,
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    (transition.effects()[0], machine)
}

pub(super) fn prepared(
    effect: AssignedConsumerEffect,
    deadline: u64,
    hard_output_bytes: usize,
) -> PreparedFetchExecution {
    let fence = fetch_fence(effect);
    PreparedFetchExecution::new(
        effect,
        "events".to_owned(),
        settings(0),
        FetchDecodeLimits::default(),
        FetchAttemptDeadline::from_parts_for_test(fence, operation_deadline(deadline)),
        hard_output_bytes,
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"))
}

fn settings(isolation: i8) -> FetchRequestSettings {
    FetchRequestSettings::new(500, 1, 1_048_576, 1_048_576, isolation)
}

pub(super) fn operation_deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(60),
    )
}

pub(super) fn fetch_fence(effect: AssignedConsumerEffect) -> kafka_client_core::FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

pub(super) fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}

pub(super) fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver: {error}"))
}

pub(super) fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}
