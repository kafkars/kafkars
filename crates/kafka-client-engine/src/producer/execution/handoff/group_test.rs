//! Atomic prepared-group detachment from one freshly validated route window.

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::super::{PreparedExecution, PreparedExecutionLimits};
use crate::{clock::OperationDeadline, protocol::produce::MaterializedProduce};

#[test]
fn exact_group_detaches_all_preflighted_owners_in_one_commit() {
    let mut owner = PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
        },
    );
    let execution =
        BatchExecutionId::new(BatchId::from_raw(1), BatchExecutionGeneration::initial());
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now());
    owner
        .retain_for_test(
            execution,
            MaterializedProduce::from_encoded_test_parts(
                "orders",
                0,
                Bytes::from_static(b"encoded"),
            ),
        )
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    owner
        .arm_for_test(execution, OperationId::from_raw(1), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
    let window = owner
        .next_driver_route_window(usize::MAX)
        .unwrap_or_else(|error| panic!("borrow route window: {error}"))
        .unwrap_or_else(|| panic!("armed route window"));
    let (key, candidates) = window.into_parts();

    let submissions = owner
        .take_driver_submission_group(&key, &candidates)
        .unwrap_or_else(|error| panic!("atomic group handoff failed: {error}"));

    assert_eq!(submissions.len(), 1);
    assert_eq!(submissions[0].execution(), execution);
    assert_eq!(owner.submission_count(), 0);
    assert_eq!(owner.prepared_stats().encoded_record_bytes, 0);
}
