//! Owned terminal and generated `OffsetCommit` peak-allocation accounting evidence.

use kafka_wire::{
    OffsetCommitRequest,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};

use super::{
    OffsetCommitTargetRef,
    retention::{MINIMUM_ENTRY_CHARGE, generated_request_peak_charge, validated_result_charge},
};

#[test]
fn each_result_charges_owned_topic_and_borrowed_correlation_storage() {
    let (_, empty) =
        validated_result_charge(core::iter::empty()).unwrap_or_else(|| panic!("base charge fits"));
    let (_, one) = validated_result_charge(core::iter::once("orders"))
        .unwrap_or_else(|| panic!("entry charge fits"));
    assert!(one >= empty + MINIMUM_ENTRY_CHARGE + "orders".len());
}

#[test]
fn generated_request_peak_charges_owned_text_structures_and_sort_scratch() {
    let empty = generated_request_peak_charge("g", core::iter::empty())
        .unwrap_or_else(|| panic!("empty charge fits"));
    let target = OffsetCommitTargetRef::new("orders", 1, 4, None, Some("checkpoint"));
    let one = generated_request_peak_charge("g", core::iter::once(target))
        .unwrap_or_else(|| panic!("one-target charge fits"));
    let exact_increment = core::mem::size_of::<usize>()
        + core::mem::size_of::<OffsetCommitRequestTopic>()
        + core::mem::size_of::<OffsetCommitRequestPartition>()
        + "orders".len()
        + "checkpoint".len();

    assert_eq!(
        empty,
        core::mem::size_of::<OffsetCommitRequest>()
            + core::mem::size_of::<Vec<usize>>()
            + "g".len()
    );
    assert_eq!(one - empty, exact_increment);
}
