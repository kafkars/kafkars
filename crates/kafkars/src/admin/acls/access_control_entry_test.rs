//! Owned access-control entry construction and validation tests.

use super::{AccessControlEntry, AclOperation, AclPermissionType};

#[test]
fn entry_owns_exact_principal_host_operation_and_permission() {
    let mut principal = "User:alice".to_owned();
    let entry = AccessControlEntry::new(
        principal.clone(),
        "*",
        AclOperation::READ,
        AclPermissionType::ALLOW,
    );
    principal.clear();

    assert_eq!(entry.principal(), "User:alice");
    assert_eq!(entry.host(), "*");
    assert_eq!(entry.operation(), AclOperation::READ);
    assert_eq!(entry.permission_type(), AclPermissionType::ALLOW);
    assert!(entry.is_valid_for_binding());
}

#[test]
fn concrete_entry_rejects_empty_strings_and_filter_only_codes() {
    assert!(
        !AccessControlEntry::new("", "*", AclOperation::READ, AclPermissionType::ALLOW)
            .is_valid_for_binding()
    );
    assert!(
        !AccessControlEntry::new(
            "User:alice",
            "",
            AclOperation::READ,
            AclPermissionType::ALLOW
        )
        .is_valid_for_binding()
    );
    assert!(
        !AccessControlEntry::new("User:alice", "*", AclOperation::ANY, AclPermissionType::ANY)
            .is_valid_for_binding()
    );
}
