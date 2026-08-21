//! Stable reassignment value tests.

use super::PartitionReassignment;

#[test]
fn ordered_broker_lists_are_preserved_exactly() {
    let value = PartitionReassignment::new(vec![3, 1], vec![3], vec![2]);
    assert_eq!(value.replicas(), &[3, 1]);
    assert_eq!(value.adding_replicas(), &[3]);
    assert_eq!(value.removing_replicas(), &[2]);
}
