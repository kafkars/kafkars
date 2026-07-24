//! Public automatic partition-increase value scenarios.

use super::NewPartitions;

#[test]
fn new_total_count_is_explicit_and_lossless() {
    let increase = NewPartitions::new("orders", 48);
    assert_eq!(increase.topic(), "orders");
    assert_eq!(increase.total_count(), 48);
    assert_eq!(increase.into_parts(), ("orders".to_owned(), 48));
}
