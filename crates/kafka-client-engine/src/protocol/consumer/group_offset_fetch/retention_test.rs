//! Normalized-result charge coverage.

use super::retention::{MINIMUM_ENTRY_CHARGE, normalized_result_charge};

#[test]
fn charge_covers_every_entry_and_only_success_metadata() {
    let (_, base) = normalized_result_charge(core::iter::empty::<(i16, Option<&str>)>())
        .unwrap_or_else(|| panic!("base charge"));
    let (_, charge) =
        normalized_result_charge([(0, Some("meta")), (-7, Some("ignored"))].into_iter())
            .unwrap_or_else(|| panic!("entry charge"));

    assert_eq!(charge, base + (2 * MINIMUM_ENTRY_CHARGE) + "meta".len());
}
