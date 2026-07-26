//! Owned terminal and temporary correlation allocation accounting evidence.

use super::retention::{MINIMUM_ENTRY_CHARGE, request_grouping_charge, validated_result_charge};

#[test]
fn each_result_charges_owned_topic_and_borrowed_correlation_storage() {
    let (_, empty) =
        validated_result_charge(core::iter::empty()).unwrap_or_else(|| panic!("base charge fits"));
    let (_, one) = validated_result_charge(core::iter::once("orders"))
        .unwrap_or_else(|| panic!("entry charge fits"));
    assert!(one >= empty + MINIMUM_ENTRY_CHARGE + "orders".len());
}

#[test]
fn request_grouping_charges_one_index_per_caller_target() {
    let empty = request_grouping_charge(0).unwrap_or_else(|| panic!("empty charge fits"));
    let one = request_grouping_charge(1).unwrap_or_else(|| panic!("one-target charge fits"));
    assert_eq!(one - empty, core::mem::size_of::<usize>());
}
