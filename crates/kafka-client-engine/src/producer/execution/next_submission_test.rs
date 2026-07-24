//! Stable prepared-submission selection and linear transfer scenarios.

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::{PreparedExecution, PreparedExecutionLimits};
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

fn retain(owner: &mut PreparedExecution, batch: u64, topic: &'static str, partition: i32) {
    let execution = execution(batch);
    owner
        .retain_for_test(execution, prepared(topic, partition))
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    arm(owner, batch);
}

fn arm(owner: &mut PreparedExecution, batch: u64) {
    let execution = execution(batch);
    owner
        .arm_for_test(
            execution,
            OperationId::from_raw(batch),
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(100 + batch),
                Instant::now(),
            ),
        )
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
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
