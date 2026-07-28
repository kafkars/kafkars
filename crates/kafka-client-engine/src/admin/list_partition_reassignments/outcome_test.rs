//! Stable engine translation tests for reassignment listing.

use kafka_client_core::{
    ListPartitionReassignmentsBatch as CoreBatch,
    ListPartitionReassignmentsTerminal as CoreTerminal, PartitionReassignment as CoreReassignment,
    PartitionReassignmentOutcome as CoreOutcome,
};

use super::{ListPartitionReassignmentsOutcome, outcome::translate_terminal};

#[test]
fn core_rows_translate_without_losing_ordered_broker_sets() {
    let translated = translate_terminal(CoreTerminal::Reassignments(CoreBatch::new(
        11,
        vec![CoreOutcome::new(
            "orders".to_owned(),
            2,
            CoreReassignment::new(vec![3, 1], vec![3], vec![2]),
        )],
    )));
    let ListPartitionReassignmentsOutcome::Reassignments(batch) = translated else {
        panic!("successful batch expected");
    };
    let (throttle, mut rows) = batch.into_parts();
    assert_eq!(throttle, 11);
    let (topic, partition, value) = rows.remove(0).into_parts();
    assert_eq!((topic.as_str(), partition), ("orders", 2));
    assert_eq!(value.into_parts(), (vec![3, 1], vec![3], vec![2]));
}
