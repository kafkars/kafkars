//! Concrete ACL binding composition and ownership tests.

use super::{
    AccessControlEntry, AclBinding, AclOperation, AclPatternType, AclPermissionType,
    AclResourceType, ResourcePattern,
};

#[test]
fn binding_composes_wire_free_owned_values() {
    let binding = binding();

    assert_eq!(binding.pattern().name(), "orders");
    assert_eq!(binding.entry().principal(), "User:alice");
    assert!(binding.is_valid_for_binding());

    let (pattern, entry) = binding.into_parts();
    assert_eq!(pattern.resource_type(), AclResourceType::TOPIC);
    assert_eq!(entry.operation(), AclOperation::READ);
}

#[test]
fn binding_validation_composes_pattern_and_entry_validation() {
    let invalid = AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, "orders", AclPatternType::ANY),
        AccessControlEntry::new(
            "User:alice",
            "*",
            AclOperation::READ,
            AclPermissionType::ALLOW,
        ),
    );

    assert!(!invalid.is_valid_for_binding());
}

fn binding() -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, "orders", AclPatternType::LITERAL),
        AccessControlEntry::new(
            "User:alice",
            "*",
            AclOperation::READ,
            AclPermissionType::ALLOW,
        ),
    )
}
