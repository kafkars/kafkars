//! Public-to-engine ACL filter translation tests.

use crate::admin::{
    AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
};

use super::DescribeAclsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_exact_filter_values() {
    let request = DescribeAclsAdminRequest::new(
        AclBindingFilter::new(
            AclResourceType::from_code(101),
            AclPatternType::from_code(102),
            AclOperation::from_code(103),
            AclPermissionType::from_code(104),
        )
        .with_resource_name("orders-")
        .with_principal("User:alice")
        .with_host("10.0.0.1"),
    );
    let engine = request.into_engine();
    let filter = engine.filter();

    assert_eq!(filter.resource_type(), 101);
    assert_eq!(filter.resource_name(), Some("orders-"));
    assert_eq!(filter.pattern_type(), 102);
    assert_eq!(filter.principal(), Some("User:alice"));
    assert_eq!(filter.host(), Some("10.0.0.1"));
    assert_eq!(filter.operation(), 103);
    assert_eq!(filter.permission_type(), 104);
}

#[test]
fn nullable_filter_strings_remain_null() {
    let engine = DescribeAclsAdminRequest::new(AclBindingFilter::any()).into_engine();
    let filter = engine.filter();

    assert_eq!(filter.resource_name(), None);
    assert_eq!(filter.principal(), None);
    assert_eq!(filter.host(), None);
}
