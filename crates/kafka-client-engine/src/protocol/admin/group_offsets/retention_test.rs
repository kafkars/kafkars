//! Conservative owned-result and temporary-sort accounting evidence.

use super::{
    model::GroupOffsetValueRef,
    model_test::{partition, topic},
    retention::{MINIMUM_ENTRY_CHARGE, OwnedGroupOffsetValueCharge, validated_result_charge},
};

#[test]
fn every_entry_charges_owned_output_and_temporary_sort_storage() {
    let topic = topic("orders", vec![partition(0, 1, -1, Some("checkpoint"), 0)]);
    let (_, charge_without_entry) = validated_result_charge(core::iter::empty())
        .unwrap_or_else(|| panic!("base charge must fit"));
    let (_, charge_with_entry) = validated_result_charge(core::iter::once((
        topic.name.as_str(),
        0,
        Some("checkpoint"),
    )))
    .unwrap_or_else(|| panic!("entry charge must fit"));
    assert!(
        charge_with_entry
            >= charge_without_entry + MINIMUM_ENTRY_CHARGE + "orders".len() + "checkpoint".len()
    );
    assert!(value_charge_is_representative());
}

fn value_charge_is_representative() -> bool {
    core::mem::size_of::<OwnedGroupOffsetValueCharge>()
        >= core::mem::size_of::<GroupOffsetValueRef<'static>>()
}

#[test]
fn rejected_entries_do_not_charge_ignored_metadata_payloads() {
    let (_, rejected) = validated_result_charge(core::iter::once(("orders", -1, Some("ignored"))))
        .unwrap_or_else(|| panic!("rejected charge must fit"));
    let (_, without_metadata) = validated_result_charge(core::iter::once(("orders", -1, None)))
        .unwrap_or_else(|| panic!("rejected charge must fit"));
    assert_eq!(rejected, without_metadata);
}
