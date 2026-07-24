//! Raw-terminal, core-authorization, and delivery-reclamation Fetch scenarios.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFailure, FetchFence, Moment, NextFetchOffset,
    PartitionIndex, StartPosition, TopicId,
};

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{
        FetchDecodeLimits, FetchRequestSettings, fixture::encoded_data_batch_for_test,
    },
};

use super::{DirectFetchExecutor, FetchExecutionError, PreparedFetchExecution};

const TOPIC: &str = "events";
const PARTITION: u32 = 3;
pub(super) const OFFSET: i64 = 10;
pub(super) const OUTPUT_BYTES: usize = 64 * 1024;

#[test]
fn deliverable_is_authorized_before_progress_and_reclaimed_explicitly() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::Success(Some(encoded_data_batch_for_test(OFFSET))),
    );

    let transition = executor
        .poll(&mut machine, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("settle delivery: {error:?}"))
        .unwrap_or_else(|| panic!("Fetch terminal transition"));
    assert_eq!(
        transition.effects().first(),
        Some(&AssignedConsumerEffect::AuthorizeFetchDelivery {
            fence,
            next_offset: offset(11),
        })
    );
    assert!(matches!(
        transition.effects().get(1),
        Some(AssignedConsumerEffect::FetchReady { next_offset, .. })
            if *next_offset == offset(11)
    ));
    assert_eq!(executor.retained().0, 0);

    let delivery = executor
        .take_ready()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("authorized delivery"));
    assert_eq!(delivery.fence(), fence);
    assert_eq!(delivery.next_offset(), offset(11));
    assert!(
        !delivery
            .outcome()
            .outcome()
            .data_batches()
            .unwrap_or(&[])
            .is_empty()
    );
    executor
        .reclaim(delivery)
        .unwrap_or_else(|failure| panic!("reclaim delivery: {:?}", failure.into_parts().0));
    assert_eq!(executor.retained(), (0, 0, 0));
}

#[test]
fn empty_success_advances_without_publishing_or_retaining_a_delivery() {
    let (effect, mut machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::Success(None),
    );

    let transition = executor
        .poll(&mut machine, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("settle empty Fetch: {error:?}"))
        .unwrap_or_else(|| panic!("empty Fetch transition"));
    assert!(matches!(
        transition.effects(),
        [AssignedConsumerEffect::FetchReady { next_offset, .. }]
            if *next_offset == offset(10)
    ));
    assert!(
        executor
            .take_ready()
            .unwrap_or_else(|error| panic!("empty delivery query: {error:?}"))
            .is_none()
    );
    assert_eq!(executor.retained(), (0, 0, 0));
}

#[test]
fn exact_broker_and_driver_failures_release_output_capacity_without_retry() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(&mut executor, prepared(effect), TerminalFixture::Broker(-7));

    let transition = executor
        .poll(&mut machine, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("settle broker failure: {error:?}"))
        .unwrap_or_else(|| panic!("broker failure transition"));
    assert!(matches!(
        transition.effects(),
        [AssignedConsumerEffect::FetchFailed {
            fence: actual,
            failure: FetchFailure::Broker(code),
        }] if *actual == fence && code.get() == -7
    ));
    assert_eq!(executor.retained(), (0, 0, 0));

    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::RouteUnavailable,
    );
    let transition = executor
        .poll(&mut machine, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("settle driver failure: {error:?}"))
        .unwrap_or_else(|| panic!("driver failure transition"));
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::FetchFailed {
            fence,
            failure: FetchFailure::Transport,
        }]
    );
    assert_eq!(executor.retained(), (0, 0, 0));
}

#[test]
fn unexpected_core_rejection_retains_store_and_route_ownership_until_shutdown() {
    let (effect, _) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::Success(Some(encoded_data_batch_for_test(OFFSET))),
    );
    let mut unassigned = AssignedConsumerMachine::new();

    assert!(matches!(
        executor.poll(&mut unassigned, Moment::from_tick(8)),
        Err(FetchExecutionError::Core(
            kafka_client_core::AssignedConsumerMachineError::NoAssignment
        ))
    ));
    let (calls, deliveries, bytes) = executor.retained();
    assert_eq!((calls, deliveries), (1, 1));
    assert!(bytes > 0);
    let recovery = executor.recover_after_driver_shutdown();
    assert!(recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert!(requests.is_empty());
    assert_eq!(completion, None);
}

pub(super) fn install(
    executor: &mut DirectFetchExecutor,
    prepared: PreparedFetchExecution,
    fixture: TerminalFixture,
) {
    let (request, output_bytes) = prepared.into_parts_for_test();
    executor
        .reserve_output_for_test(request.fence(), output_bytes)
        .unwrap_or_else(|error| panic!("reserve output: {error:?}"));
    let calls = executor.tracked_calls_for_test();
    match fixture {
        TerminalFixture::Success(records) => {
            let partition_index =
                i32::try_from(request.fence().position().partition().partition().get())
                    .unwrap_or_else(|error| panic!("test partition must fit i32: {error}"));
            calls.install_success_terminal_for_test(
                request,
                Moment::from_tick(7),
                12,
                partition_index,
                records,
            );
        }
        TerminalFixture::Broker(code) => {
            calls.install_broker_terminal_for_test(request, Moment::from_tick(7), 12, code);
        }
        TerminalFixture::RouteUnavailable => {
            calls.install_route_unavailable_terminal_for_test(request, Moment::from_tick(7));
        }
    }
}

pub(super) enum TerminalFixture {
    Success(Option<Bytes>),
    Broker(i16),
    RouteUnavailable,
}

pub(super) fn assignment() -> (AssignedConsumerEffect, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(PARTITION),
                ),
                StartPosition::Offset(offset(OFFSET)),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    (transition.effects()[0], machine)
}

pub(super) fn prepared(effect: AssignedConsumerEffect) -> PreparedFetchExecution {
    PreparedFetchExecution::new(
        effect,
        TOPIC.to_owned(),
        FetchRequestSettings::new(500, 1, 1_048_576, 1_048_576, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(1_000_000_000),
            Instant::now() + Duration::from_secs(60),
        ),
        OUTPUT_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"))
}

pub(super) fn fetch_fence(effect: AssignedConsumerEffect) -> FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

pub(super) fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
