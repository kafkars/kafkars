//! Public partition-increase value scenarios.

use super::NewPartitions;

#[test]
fn new_total_count_is_explicit_and_lossless() {
    let increase = NewPartitions::new("orders", 48);
    assert_eq!(increase.topic(), "orders");
    assert_eq!(increase.total_count(), 48);
    assert_eq!(increase.replica_assignments(), None);
    assert_eq!(increase.into_parts(), ("orders".to_owned(), 48, None));
}

#[test]
fn explicit_replica_assignments_preserve_partition_and_broker_order() {
    let increase = NewPartitions::new("orders", 5).with_replica_assignments([[3, 1, 2], [2, 3, 1]]);

    assert_eq!(
        increase.replica_assignments(),
        Some([vec![3, 1, 2], vec![2, 3, 1]].as_slice())
    );
    assert_eq!(
        increase.into_parts(),
        (
            "orders".to_owned(),
            5,
            Some(vec![vec![3, 1, 2], vec![2, 3, 1]])
        )
    );
}
