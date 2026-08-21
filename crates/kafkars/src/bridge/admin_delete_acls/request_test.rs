//! Public-to-engine positional ACL deletion filter translation tests.

use crate::admin::{
    AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
};

use super::DeleteAclsAdminRequest;

#[test]
fn translation_preserves_duplicate_positions_exact_codes_and_nulls() {
    let duplicate = AclBindingFilter::new(
        AclResourceType::from_code(101),
        AclPatternType::from_code(102),
        AclOperation::from_code(103),
        AclPermissionType::from_code(104),
    )
    .with_resource_name("orders")
    .with_principal("User:alice");
    let engine = DeleteAclsAdminRequest::new(vec![duplicate.clone(), duplicate]).into_engine();

    assert_eq!(engine.filters().len(), 2);
    assert_eq!(engine.filters()[0], engine.filters()[1]);
    assert_eq!(engine.filters()[0].resource_type(), 101);
    assert_eq!(engine.filters()[0].resource_name(), Some("orders"));
    assert_eq!(engine.filters()[0].pattern_type(), 102);
    assert_eq!(engine.filters()[0].principal(), Some("User:alice"));
    assert_eq!(engine.filters()[0].host(), None);
    assert_eq!(engine.filters()[0].operation(), 103);
    assert_eq!(engine.filters()[0].permission_type(), 104);
}
