//! Per-entry deterministic classic-group ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupPhase, GroupId};

use super::registry_entry::GroupConsumerEntry;

#[test]
fn entry_owns_one_dormant_machine_for_its_exact_group() {
    let group_id =
        GroupId::try_from_raw(17).unwrap_or_else(|| panic!("group identity must be nonzero"));
    let entry =
        GroupConsumerEntry::try_new(group_id, &Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("entry creation failed: {error:?}"));

    assert_eq!(entry.classic.machine().group_id(), group_id);
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Dormant);
    assert!(entry.classic.is_dormant());
}
