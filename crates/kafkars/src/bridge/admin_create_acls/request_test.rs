//! Public-to-engine ACL creation request translation tests.

use crate::admin::{
    AccessControlEntry, AclBinding, AclOperation, AclPatternType, AclPermissionType,
    AclResourceType, ResourcePattern,
};

use super::CreateAclsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_caller_order_and_exact_codes() {
    let request = CreateAclsAdminRequest::new(vec![binding("first", 103), binding("second", 113)]);
    let engine = request.into_engine();

    assert_eq!(engine.bindings().len(), 2);
    assert_eq!(engine.bindings()[0].resource_name(), "first");
    assert_eq!(engine.bindings()[1].resource_name(), "second");
    assert_eq!(engine.bindings()[0].resource_type(), 101);
    assert_eq!(engine.bindings()[0].pattern_type(), 102);
    assert_eq!(engine.bindings()[0].operation(), 103);
    assert_eq!(engine.bindings()[0].permission_type(), 104);
    assert_eq!(engine.bindings()[0].principal(), "User:alice");
    assert_eq!(engine.bindings()[0].host(), "*");
    assert_eq!(engine.bindings()[1].operation(), 113);
}

fn binding(name: &str, operation: i8) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(
            AclResourceType::from_code(101),
            name,
            AclPatternType::from_code(102),
        ),
        AccessControlEntry::new(
            "User:alice",
            "*",
            AclOperation::from_code(operation),
            AclPermissionType::from_code(104),
        ),
    )
}
