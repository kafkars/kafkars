//! Stable replacement and explicit cancellation construction.

use super::PartitionReassignmentChange;

#[test]
fn replacement_preserves_replica_order_and_cancellation_is_null() {
    let replacement = PartitionReassignmentChange::new("orders", 2, [4, 1, 7]);
    assert_eq!(replacement.topic(), "orders");
    assert_eq!(replacement.partition(), 2);
    assert_eq!(replacement.replicas(), Some(&[4, 1, 7][..]));

    let cancellation = PartitionReassignmentChange::cancel("orders", 2);
    assert_eq!(cancellation.replicas(), None);
}
