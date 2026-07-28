//! Static group-instance identity bounds and retained-byte ownership.

use std::sync::Arc;

use kafka_client_core::GroupId;

use super::{GroupSessionCatalog, GroupSessionCatalogError, MAX_KAFKA_GROUP_STRING_BYTES};

fn group_id() -> GroupId {
    GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group identity"))
}

#[test]
fn static_identity_remains_exact_nonempty_and_bounded() {
    let instance: Arc<str> = Arc::from("instance-a");
    let catalog = GroupSessionCatalog::try_new_with_group_instance_id(
        group_id(),
        Arc::from("workers"),
        Some(Arc::clone(&instance)),
        &[],
    )
    .unwrap_or_else(|error| panic!("static catalog creation failed: {error:?}"));
    assert!(Arc::ptr_eq(
        catalog
            .group_instance_id()
            .unwrap_or_else(|| panic!("static identity expected")),
        &instance
    ));
    assert_eq!(
        catalog.retained_identity_bytes(),
        "workers".len() + "instance-a".len()
    );
    assert!(matches!(
        GroupSessionCatalog::try_new_with_group_instance_id(
            group_id(),
            Arc::from("workers"),
            Some(Arc::from("")),
            &[],
        ),
        Err(GroupSessionCatalogError::EmptyGroupInstance)
    ));
    let oversized: Arc<str> = Arc::from("i".repeat(MAX_KAFKA_GROUP_STRING_BYTES + 1));
    assert!(matches!(
        GroupSessionCatalog::try_new_with_group_instance_id(
            group_id(),
            Arc::from("workers"),
            Some(oversized),
            &[],
        ),
        Err(GroupSessionCatalogError::GroupInstanceBytes { .. })
    ));
}
