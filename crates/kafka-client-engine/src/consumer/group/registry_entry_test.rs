//! Linear entry ownership of catalog and deterministic membership policy.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGroupPhase, ClassicProtocol, GroupId, GroupPositionMissingOffsetPolicy, ReadIsolation,
    TopicId,
};

use super::classic_group_test_support;
use super::registry_entry::{GroupConsumerEntry, default_classic_processing_lease_policy};
use crate::config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits};
use crate::consumer::group_registration_request::{
    GroupConsumerClassicAssignor, GroupConsumerProtocol,
};

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
    assert!(entry.fetch.activation().is_none());
    assert!(entry.fetch.fault().is_none());
}

#[test]
fn cooperative_sticky_registration_reaches_the_classic_machine() {
    let group_id = GroupId::try_from_raw(19).unwrap_or_else(|| panic!("group identity"));
    let entry = GroupConsumerEntry::try_new_with_protocol_configuration(
        group_id,
        &Arc::from("workers"),
        None,
        &[Arc::from("orders")],
        GroupConsumerProtocol::Classic,
        GroupConsumerClassicAssignor::CooperativeSticky,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
        GroupPositionMissingOffsetPolicy::Error,
        ReadIsolation::ReadUncommitted,
        default_classic_processing_lease_policy(),
        ValidatedConsumerFetchConfig::default(),
        ValidatedConsumerLimits::default(),
    )
    .unwrap_or_else(|error| panic!("cooperative entry: {error:?}"));

    assert_eq!(
        entry.classic.machine().protocol(),
        ClassicProtocol::CooperativeSticky
    );
}
