//! Nullable ACL binding filter construction and validation tests.

use super::{
    AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, ResourcePattern,
};

#[test]
fn any_filter_preserves_null_string_wildcards() {
    let filter = AclBindingFilter::any();

    assert_eq!(filter.resource_type(), AclResourceType::ANY);
    assert_eq!(filter.resource_name(), None);
    assert_eq!(filter.pattern_type(), AclPatternType::ANY);
    assert_eq!(filter.principal(), None);
    assert_eq!(filter.host(), None);
    assert_eq!(filter.operation(), AclOperation::ANY);
    assert_eq!(filter.permission_type(), AclPermissionType::ANY);
    assert!(filter.is_valid_for_filter());
}

#[test]
fn selective_filter_owns_present_strings_without_losing_nulls() {
    let filter = AclBindingFilter::new(
        AclResourceType::TOPIC,
        AclPatternType::MATCH,
        AclOperation::READ,
        AclPermissionType::ANY,
    )
    .with_resource_name("orders")
    .with_principal("User:alice");

    assert_eq!(filter.resource_name(), Some("orders"));
    assert_eq!(filter.principal(), Some("User:alice"));
    assert_eq!(filter.host(), None);
    assert!(filter.is_valid_for_filter());
}

#[test]
fn exact_binding_converts_into_an_exact_filter() {
    let binding = AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, "orders", AclPatternType::LITERAL),
        AccessControlEntry::new(
            "User:alice",
            "*",
            AclOperation::READ,
            AclPermissionType::ALLOW,
        ),
    );
    let filter = AclBindingFilter::from(&binding);

    assert_eq!(filter.resource_name(), Some("orders"));
    assert_eq!(filter.principal(), Some("User:alice"));
    assert_eq!(filter.host(), Some("*"));
    assert!(filter.is_valid_for_filter());
}

#[test]
fn filter_validation_rejects_unknown_codes_and_present_empty_strings() {
    let unknown = AclBindingFilter::new(
        AclResourceType::UNKNOWN,
        AclPatternType::ANY,
        AclOperation::ANY,
        AclPermissionType::ANY,
    );
    assert!(!unknown.is_valid_for_filter());

    let empty = AclBindingFilter::any().with_host("");
    assert_eq!(empty.host(), Some(""));
    assert!(!empty.is_valid_for_filter());
}
