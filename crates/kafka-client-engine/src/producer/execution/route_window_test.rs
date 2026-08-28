//! Borrow-only route-window selection and atomic transfer scenarios.

use std::{sync::OnceLock, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
    partitioning::TopicMetadataGeneration,
};

use super::{
    PreparedExecution, PreparedExecutionLimits, PreparedProduceHandoffError,
    PreparedProduceRouteCandidate, PreparedProduceSubmission,
};
use crate::{clock::OperationDeadline, protocol::produce::MaterializedProduce};

#[test]
fn route_window_snapshot_keeps_bytes_and_deadlines_under_prepared_ownership() {
    let mut owner = candidate_owner(2, 1_024);
    retain(&mut owner, 1, "orders", 0, b"one");
    retain(&mut owner, 2, "orders", 1, b"two");
    let before = owner.prepared_stats();

    let window = owner
        .next_driver_route_window(usize::MAX)
        .unwrap_or_else(|error| panic!("borrow route window: {error}"))
        .unwrap_or_else(|| panic!("armed route window"));
    let (_key, candidates) = window.into_parts();

    assert_eq!(
        candidates
            .iter()
            .map(PreparedProduceRouteCandidate::execution)
            .collect::<Vec<_>>(),
        [execution(1), execution(2)],
    );
    assert_eq!(owner.prepared_stats(), before);
    assert_eq!(owner.submission_count(), 2);
}

#[test]
fn stale_route_snapshot_cannot_partially_detach_its_remaining_group() {
    let mut owner = candidate_owner(2, 1_024);
    retain(&mut owner, 1, "orders", 0, b"one");
    retain(&mut owner, 2, "orders", 1, b"two");
    let window = owner
        .next_driver_route_window(usize::MAX)
        .unwrap_or_else(|error| panic!("borrow route window: {error}"))
        .unwrap_or_else(|| panic!("armed route window"));
    let (key, candidates) = window.into_parts();
    let detached = owner
        .take_driver_submission(execution(1))
        .unwrap_or_else(|error| panic!("simulate cancellation revision: {error}"));
    let before = owner.prepared_stats();

    assert!(matches!(
        owner.take_driver_submission_group(&key, &candidates),
        Err(PreparedProduceHandoffError::OwnershipMismatch {
            requested,
            retained: None,
        }) if requested == execution(1)
    ));
    assert_eq!(owner.prepared_stats(), before);
    assert_eq!(owner.submission_count(), 1);
    drop(detached);
}

#[test]
fn candidate_window_is_the_same_topic_admission_order_prefix() {
    let mut owner = candidate_owner(4, 1_024);
    for (batch, topic, partition) in [
        (1, "orders", 0),
        (2, "orders", 1),
        (3, "payments", 0),
        (4, "orders", 2),
    ] {
        retain(&mut owner, batch, topic, partition, b"encoded");
    }

    assert_eq!(take_ids(&mut owner), vec![execution(1), execution(2)]);
    assert_eq!(take_ids(&mut owner), vec![execution(3)]);
    assert_eq!(take_ids(&mut owner), vec![execution(4)]);
}

#[test]
fn candidate_window_stops_before_a_duplicate_partition() {
    let mut owner = candidate_owner(4, 1_024);
    for (batch, partition) in [(1, 0), (2, 1), (3, 1), (4, 2)] {
        retain(&mut owner, batch, "orders", partition, b"encoded");
    }

    assert_eq!(take_ids(&mut owner), vec![execution(1), execution(2)]);
    assert_eq!(take_ids(&mut owner), vec![execution(3), execution(4)]);
}

#[test]
fn candidate_window_stops_before_request_bytes_are_exceeded() {
    let mut owner = candidate_owner(3, 10);
    retain(&mut owner, 1, "orders", 0, b"123456");
    retain(&mut owner, 2, "orders", 1, b"12345");
    retain(&mut owner, 3, "orders", 2, b"1234");

    assert_eq!(take_ids(&mut owner), vec![execution(1)]);
    assert_eq!(take_ids(&mut owner), vec![execution(2), execution(3)]);
}

#[test]
fn candidate_window_stops_at_the_remaining_turn_budget() {
    let mut owner = candidate_owner(3, 1_024);
    for (batch, partition) in [(1, 0), (2, 1), (3, 2)] {
        retain(&mut owner, batch, "orders", partition, b"encoded");
    }
    let before = owner.prepared_stats();

    let window = owner
        .next_driver_route_window(2)
        .unwrap_or_else(|error| panic!("budgeted route window: {error}"))
        .unwrap_or_else(|| panic!("budgeted route window must retain candidates"));
    let (key, candidates) = window.into_parts();

    assert_eq!(
        candidates
            .iter()
            .map(PreparedProduceRouteCandidate::execution)
            .collect::<Vec<_>>(),
        [execution(1), execution(2)],
    );
    assert_eq!(owner.prepared_stats(), before);
    let submissions = owner
        .take_driver_submission_group(&key, &candidates)
        .unwrap_or_else(|error| panic!("budgeted group handoff: {error}"));
    assert_eq!(submissions.len(), 2);
    assert_eq!(take_ids(&mut owner), vec![execution(3)]);
}

#[test]
fn candidate_window_requires_one_exact_operation_deadline() {
    let mut owner = candidate_owner(3, 1_024);
    let first = shared_deadline();
    let later = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(first.core().tick().saturating_add(1)),
        first.transport(),
    );
    retain_at(&mut owner, execution(1), "orders", 0, b"one", first);
    retain_at(&mut owner, execution(2), "orders", 1, b"two", later);
    retain_at(&mut owner, execution(3), "orders", 2, b"three", later);

    assert_eq!(take_ids(&mut owner), vec![execution(1)]);
    assert_eq!(take_ids(&mut owner), vec![execution(2), execution(3)]);
}

#[test]
fn identity_cohort_requires_matching_expectation_retry_need_and_floor() {
    let mut owner = candidate_owner(6, 1_024);
    retain_identity(&mut owner, 1, 1, 0, [7; 16], 4);
    retain_identity(&mut owner, 2, 1, 1, [7; 16], 5);
    retain_identity(&mut owner, 3, 2, 2, [7; 16], 5);
    retain_identity(&mut owner, 4, 3, 3, [7; 16], 5);
    retain_identity(&mut owner, 5, 2, 4, [7; 16], 6);
    retain_identity(&mut owner, 6, 1, 5, [8; 16], 6);

    assert_eq!(take_ids(&mut owner), vec![execution(1), execution(2)]);
    assert_eq!(
        take_ids(&mut owner),
        vec![execution_at(3, 2), execution_at(4, 3)]
    );
    assert_eq!(take_ids(&mut owner), vec![execution_at(5, 2)]);
    assert_eq!(take_ids(&mut owner), vec![execution(6)]);
}

fn candidate_owner(capacity: usize, max_request_bytes: usize) -> PreparedExecution {
    PreparedExecution::new(
        capacity,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes,
        },
    )
}

fn retain(
    owner: &mut PreparedExecution,
    batch: u64,
    topic: &'static str,
    partition: i32,
    records: &'static [u8],
) {
    retain_at(
        owner,
        execution(batch),
        topic,
        partition,
        records,
        shared_deadline(),
    );
}

fn retain_identity(
    owner: &mut PreparedExecution,
    batch: u64,
    generation: u64,
    partition: i32,
    expected: [u8; 16],
    floor: u64,
) {
    let execution = execution_at(batch, generation);
    let materialized = materialized("orders", partition, b"encoded").with_expected_topic_identity(
        Some(expected),
        Some(TopicMetadataGeneration::from_raw(floor)),
    );
    retain_materialized(owner, execution, materialized, shared_deadline());
}

fn retain_at(
    owner: &mut PreparedExecution,
    execution: BatchExecutionId,
    topic: &'static str,
    partition: i32,
    records: &'static [u8],
    deadline: OperationDeadline,
) {
    retain_materialized(
        owner,
        execution,
        materialized(topic, partition, records),
        deadline,
    );
}

fn retain_materialized(
    owner: &mut PreparedExecution,
    execution: BatchExecutionId,
    materialized: MaterializedProduce,
    deadline: OperationDeadline,
) {
    owner
        .retain_for_test(execution, materialized)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    owner
        .arm_for_test(
            execution,
            OperationId::from_raw(execution.batch_id().get()),
            deadline,
        )
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
}

fn take_ids(owner: &mut PreparedExecution) -> Vec<BatchExecutionId> {
    owner
        .take_next_driver_submissions()
        .unwrap_or_else(|error| panic!("candidate-window handoff: {error}"))
        .iter()
        .map(PreparedProduceSubmission::execution)
        .collect()
}

fn shared_deadline() -> OperationDeadline {
    static TRANSPORT: OnceLock<Instant> = OnceLock::new();
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(100),
        *TRANSPORT.get_or_init(Instant::now),
    )
}

const fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn execution_at(batch: u64, generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("execution generation must be nonzero")),
    )
}

fn materialized(
    topic: &'static str,
    partition: i32,
    records: &'static [u8],
) -> MaterializedProduce {
    MaterializedProduce::from_encoded_test_parts(topic, partition, Bytes::from_static(records))
}
