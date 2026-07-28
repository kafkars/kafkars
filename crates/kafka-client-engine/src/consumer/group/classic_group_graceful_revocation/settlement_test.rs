//! Settlement admission remains idle without a completed or lost terminal.

use super::super::registry_test_support::{register, started_registry};

#[test]
fn dormant_owner_cannot_retire_assignment() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));

    assert_eq!(
        entry.revocation.settle_terminal(
            &entry.classic,
            &mut entry.catalog,
            &mut entry.processing_lease,
            &mut entry.fetch,
        ),
        Ok(false)
    );
    assert!(entry.revocation.is_dormant());
    assert!(entry.catalog.live_assignment().is_none());
}
