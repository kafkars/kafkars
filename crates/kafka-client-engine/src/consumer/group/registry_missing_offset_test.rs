//! Immutable missing-offset policy ownership at registry admission.

use std::sync::Arc;

use kafka_client_core::{GroupPositionMissingOffsetPolicy, ReadIsolation};

use super::{
    registry_entry::default_classic_processing_lease_policy,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn registration_retains_missing_offset_policy_per_entry() {
    let mut registry = started_registry();
    let default_group = register(&mut registry, "default");
    let reset_group = registry
        .try_register_with_configuration(
            Arc::from("reset"),
            None,
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
            GroupPositionMissingOffsetPolicy::Latest,
            ReadIsolation::ReadUncommitted,
            default_classic_processing_lease_policy(),
        )
        .unwrap_or_else(|failure| panic!("reset registration: {:?}", failure.kind));

    assert_eq!(
        registry
            .entry(default_group)
            .unwrap_or_else(|| panic!("default entry expected"))
            .missing_offset_policy,
        GroupPositionMissingOffsetPolicy::Error
    );
    assert_eq!(
        registry
            .entry(reset_group)
            .unwrap_or_else(|| panic!("reset entry expected"))
            .missing_offset_policy,
        GroupPositionMissingOffsetPolicy::Latest
    );
    stop_registry(&mut registry);
}
