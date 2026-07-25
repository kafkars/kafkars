//! Linear entry ownership and session installation scenarios.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, GroupId, PartitionIndex};

use super::{registry_entry::GroupConsumerEntry, session_catalog::GroupSessionPartition};

#[test]
fn entry_owns_one_catalog_without_an_execution_host() {
    let group_id =
        GroupId::try_from_raw(17).unwrap_or_else(|| panic!("group identity must be nonzero"));
    let generation = AssignmentGeneration::try_from_raw(3)
        .unwrap_or_else(|| panic!("assignment generation must be nonzero"));
    let mut entry = GroupConsumerEntry::try_new(group_id, Arc::from("workers"))
        .unwrap_or_else(|error| panic!("entry creation failed: {error:?}"));

    entry
        .prepare_replacement(
            Arc::from("member-a"),
            9,
            generation,
            vec![GroupSessionPartition::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(2),
            )],
        )
        .unwrap_or_else(|error| panic!("session staging failed: {error:?}"))
        .commit();

    assert_eq!(entry.group_id(), group_id);
    assert_eq!(entry.group_bytes(), "workers".len());
    assert!(entry.is_active());
    assert_eq!(entry.catalog.assignment_generation(), Some(generation));
}
