//! Duplicate-fence position admission retains every exact owner.

use kafka_client_core::Moment;

use crate::driver::GroupPositionOffsetFetchKey;

use super::{
    super::{
        classic_group_entry_fault::ClassicGroupEntryFault,
        classic_group_execution::ClassicGroupExecutionError, registry_test_support::stop_registry,
    },
    submission_test::{confirmed_registry, driver, prepared_identity, shutdown_driver},
};

#[test]
fn duplicate_restores_prepared_then_freezes_exact_fence() {
    let (mut registry, group_id, _identity) = confirmed_registry();
    let expected = prepared_identity(&registry, group_id);
    registry
        .position_calls
        .as_mut()
        .unwrap_or_else(|| panic!("position calls expected"))
        .install_empty_terminal_for_test(
            GroupPositionOffsetFetchKey::new(expected.0, expected.1),
            Some(8),
        );
    let mut driver = driver();

    assert_eq!(
        registry.submit_one_classic_group_position(&driver, Moment::from_tick(5)),
        Err(ClassicGroupExecutionError::PositionDuplicateFence(
            expected.0
        ))
    );
    assert_eq!(prepared_identity(&registry, group_id), expected);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(matches!(
        entry.fault.take(),
        Some(ClassicGroupEntryFault::PositionDuplicateFence(fence)) if fence == expected.0
    ));

    shutdown_driver(&mut driver);
    let mut calls = registry
        .position_calls
        .take()
        .unwrap_or_else(|| panic!("position calls expected"));
    let mut recovery = calls.recover_group_position_offset_fetches_after_driver_shutdown();
    drop(
        recovery
            .take_settled()
            .unwrap_or_else(|| panic!("settled duplicate owner expected")),
    );
    assert!(recovery.is_empty());
    stop_registry(&mut registry);
}
