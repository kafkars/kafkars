//! Stable prepared-submission selection and linear transfer scenarios.

use std::{sync::OnceLock, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::{PreparedExecution, PreparedExecutionLimits, PreparedProduceSubmission};
use crate::{
    clock::OperationDeadline,
    producer::materialization::{MaterializationBatch, MaterializationRecord},
    protocol::produce::{MaterializedProduce, materialize_explicit_produce_batch},
};

#[test]
fn next_submission_uses_lowest_batch_id_as_core_admission_order() {
    let mut owner = PreparedExecution::new(
        3,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
        },
    );
    // Arming out of order proves selection follows core's monotonically
    // allocated BatchId rather than mechanism insertion order.
    for (batch, topic, partition) in [(9, "newest", 9), (3, "oldest", 3), (7, "middle", 7)] {
        retain(&mut owner, batch, topic, partition);
    }

    for (batch, topic, partition) in [(3, "oldest", 3), (7, "middle", 7), (9, "newest", 9)] {
        let submission = owner
            .take_next_driver_submission()
            .unwrap_or_else(|error| panic!("ordered handoff failed: {error}"))
            .unwrap_or_else(|| panic!("armed submission should be ready"));
        assert_eq!(submission.execution(), execution(batch));
        let (_, _, materialized) = submission.into_parts();
        assert_eq!(materialized.topic_name(), topic);
        assert_eq!(materialized.partition(), partition);
    }
    assert!(
        owner
            .take_next_driver_submission()
            .unwrap_or_else(|error| panic!("empty handoff failed: {error}"))
            .is_none()
    );
    assert_eq!(owner.prepared_stats().batches, 0);
    assert_eq!(owner.submission_count(), 0);
}

#[test]
fn materialized_but_unarmed_batch_is_not_a_driver_submission() {
    let mut owner = PreparedExecution::new(
        2,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
        },
    );
    owner
        .retain_for_test(execution(1), prepared("waiting", 1))
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    retain(&mut owner, 2, "ready", 2);

    let submission = owner
        .take_next_driver_submission()
        .unwrap_or_else(|error| panic!("ready handoff failed: {error}"))
        .unwrap_or_else(|| panic!("armed submission should be selected"));
    assert_eq!(submission.execution(), execution(2));
    assert_eq!(owner.prepared_stats().batches, 1);
    assert_eq!(owner.submission_count(), 0);
}

#[test]
fn next_submission_detaches_one_name_routed_entry_even_when_brokers_match() {
    let mut owner = PreparedExecution::new(
        4,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
        },
    );
    for (batch, broker) in [(1, 7), (2, 7), (3, 8), (4, 7)] {
        owner
            .retain_for_test(
                execution(batch),
                MaterializedProduce::from_broker_routed_test_parts(
                    "orders",
                    i32::try_from(batch).unwrap_or_else(|_| panic!("partition")),
                    broker,
                    Bytes::from_static(b"encoded"),
                ),
            )
            .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
        arm(&mut owner, batch);
    }

    let first = owner
        .take_next_driver_submissions()
        .unwrap_or_else(|error| panic!("broker group handoff: {error}"));
    assert_eq!(
        first
            .iter()
            .map(|submission| submission.execution())
            .collect::<Vec<_>>(),
        vec![execution(1)]
    );
    let second = owner
        .take_next_driver_submissions()
        .unwrap_or_else(|error| panic!("second broker group handoff: {error}"));
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].execution(), execution(2));
}

#[test]
fn matching_routes_remain_individually_name_routed() {
    let mut owner = broker_owner(4, 1_024);
    for (batch, partition) in [(1, 0), (2, 1), (3, 1), (4, 2)] {
        retain_broker_batch(&mut owner, batch, partition, b"encoded");
    }

    assert_eq!(take_group_ids(&mut owner), vec![execution(1)]);
    assert_eq!(take_group_ids(&mut owner), vec![execution(2)]);
}

#[test]
fn request_bound_does_not_combine_name_routed_submissions() {
    let mut owner = broker_owner(3, 10);
    retain_broker_batch(&mut owner, 1, 0, b"123456");
    retain_broker_batch(&mut owner, 2, 1, b"12345");
    retain_broker_batch(&mut owner, 3, 2, b"1234");

    assert_eq!(take_group_ids(&mut owner), vec![execution(1)]);
    assert_eq!(take_group_ids(&mut owner), vec![execution(2)]);
}

#[test]
fn bounded_pipeline_still_detaches_only_one_submission_per_driver_call() {
    let mut owner = broker_owner(64, 1_024);
    for batch in 1..=64 {
        retain_broker_batch(
            &mut owner,
            batch,
            i32::try_from(batch).unwrap_or_else(|_| panic!("partition")),
            b"x",
        );
    }

    assert_eq!(take_group_ids(&mut owner), vec![execution(1)]);
}

#[test]
fn different_deadlines_remain_individually_name_routed() {
    let mut owner = broker_owner(3, 1_024);
    let first_deadline = shared_deadline();
    let later_deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(first_deadline.core().tick().saturating_add(1)),
        first_deadline.transport(),
    );
    retain_broker_batch_at(&mut owner, 1, 0, b"one", first_deadline);
    retain_broker_batch_at(&mut owner, 2, 1, b"two", later_deadline);
    retain_broker_batch_at(&mut owner, 3, 2, b"three", later_deadline);

    assert_eq!(take_group_ids(&mut owner), vec![execution(1)]);
    assert_eq!(take_group_ids(&mut owner), vec![execution(2)]);
}

#[test]
fn corrupt_selected_entry_accounting_rejects_before_any_entry_is_detached() {
    let mut owner = broker_owner(2, 1_024);
    retain_broker_batch(&mut owner, 1, 0, b"123456");
    retain_broker_batch(&mut owner, 2, 1, b"12345");
    owner.replace_retained_bytes_for_test(5);
    let before = owner.prepared_stats();

    assert!(matches!(
        owner.take_next_driver_submissions(),
        Err(super::PreparedProduceHandoffError::AccountingInconsistent { .. })
    ));
    assert_eq!(owner.prepared_stats(), before);
    assert_eq!(owner.submission_count(), 2);
    assert!(owner.next_deadline().is_some());
}

fn broker_owner(capacity: usize, max_request_bytes: usize) -> PreparedExecution {
    PreparedExecution::new(
        capacity,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes,
        },
    )
}

fn retain_broker_batch(
    owner: &mut PreparedExecution,
    batch: u64,
    partition: i32,
    records: &'static [u8],
) {
    retain_broker_batch_at(owner, batch, partition, records, shared_deadline());
}

fn retain_broker_batch_at(
    owner: &mut PreparedExecution,
    batch: u64,
    partition: i32,
    records: &'static [u8],
    deadline: OperationDeadline,
) {
    owner
        .retain_for_test(
            execution(batch),
            MaterializedProduce::from_broker_routed_test_parts(
                "orders",
                partition,
                7,
                Bytes::from_static(records),
            ),
        )
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    arm_at(owner, batch, deadline);
}

fn take_group_ids(owner: &mut PreparedExecution) -> Vec<BatchExecutionId> {
    owner
        .take_next_driver_submissions()
        .unwrap_or_else(|error| panic!("broker group handoff: {error}"))
        .iter()
        .map(PreparedProduceSubmission::execution)
        .collect()
}

fn retain(owner: &mut PreparedExecution, batch: u64, topic: &'static str, partition: i32) {
    let execution = execution(batch);
    owner
        .retain_for_test(execution, prepared(topic, partition))
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    arm(owner, batch);
}

fn arm(owner: &mut PreparedExecution, batch: u64) {
    arm_at(owner, batch, shared_deadline());
}

fn arm_at(owner: &mut PreparedExecution, batch: u64, deadline: OperationDeadline) {
    let execution = execution(batch);
    owner
        .arm_for_test(execution, OperationId::from_raw(batch), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
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

fn prepared(topic: &'static str, partition: i32) -> MaterializedProduce {
    let batch = MaterializationBatch::try_for_test(
        topic.to_owned(),
        partition,
        vec![MaterializationRecord::new(
            100,
            None,
            Some(Bytes::from_static(b"value")),
            Vec::new(),
        )],
        usize::MAX,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("test materialization failed: {error}"))
}
