//! Retained-result charge tests for reassignment normalization.

use super::retention::{MINIMUM_ENTRY_CHARGE, successful_result_charge};

#[test]
fn charge_includes_every_topic_byte_and_broker_id() {
    let (_, empty) = successful_result_charge(core::iter::empty(), 0, 0)
        .unwrap_or_else(|| panic!("expected base charge"));
    let (_, one) = successful_result_charge(core::iter::once(("orders", 1, 1, 1)), 1, 0)
        .unwrap_or_else(|| panic!("expected entry charge"));
    assert!(one >= empty + "orders".len() + MINIMUM_ENTRY_CHARGE);
    let (_, short) = successful_result_charge(core::iter::once(("x", 1, 1, 1)), 1, 0)
        .unwrap_or_else(|| panic!("expected short entry charge"));
    assert_eq!(one - short, 5);
}
