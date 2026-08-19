//! Evidence for O(1) aggregate Produce settlement ownership.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline};

use super::produce_call_entries::{TrackedProduceEntries, TrackedProduceEntry};

#[test]
fn aggregate_entries_advance_without_moving() {
    let mut entries =
        TrackedProduceEntries::batch(vec![entry(1, 90, "orders", 0), entry(2, 40, "orders", 1)]);
    let second_address = entries
        .iter()
        .nth(1)
        .map_or_else(|| panic!("second entry"), std::ptr::from_ref);

    assert_eq!(entries.first().execution, execution(1));
    assert!(entries.advance());
    assert_eq!(entries.first().execution, execution(2));
    assert_eq!(std::ptr::from_ref(entries.first()), second_address);
    assert!(!entries.advance());
}

fn entry(batch: u64, deadline: u64, topic: &str, partition: i32) -> TrackedProduceEntry {
    TrackedProduceEntry {
        execution: execution(batch),
        deadline: Deadline::from_tick(deadline),
        topic: topic.into(),
        partition,
    }
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}
