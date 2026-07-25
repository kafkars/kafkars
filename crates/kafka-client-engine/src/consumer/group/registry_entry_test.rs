//! Linear entry ownership of catalog and deterministic membership policy.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupPhase, GroupId, TopicId};

use super::classic_group_test_support;
use super::registry_entry::GroupConsumerEntry;

#[test]
fn entry_owns_one_catalog_and_machine_with_the_same_identity() {
    let group_id =
        GroupId::try_from_raw(17).unwrap_or_else(|| panic!("group identity must be nonzero"));
    let timing = classic_group_test_support::timing();
    let entry = GroupConsumerEntry::try_new(
        group_id,
        &Arc::from("workers"),
        &[Arc::from("payments"), Arc::from("orders")],
        timing,
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    )
    .unwrap_or_else(|error| panic!("entry creation failed: {error:?}"));

    assert_eq!(entry.group_id(), group_id);
    assert_eq!(entry.group_bytes(), "workers".len());
    assert!(entry.is_active());
    assert_eq!(entry.catalog.local_subscription().len(), 2);
    assert_eq!(entry.catalog.topic_id("orders"), Some(TopicId::from_raw(1)));
    assert_eq!(
        entry.catalog.topic_id("payments"),
        Some(TopicId::from_raw(2))
    );
    assert_eq!(entry.classic.machine().group_id(), group_id);
    assert_eq!(entry.classic.machine().timing(), timing);
    assert_eq!(
        entry.classic.machine().rejoin_policy(),
        classic_group_test_support::rejoin_policy()
    );
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Dormant);
}
