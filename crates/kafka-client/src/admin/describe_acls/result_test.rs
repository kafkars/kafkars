//! ACL description result ownership and ordering tests.

use std::time::Duration;

use super::{
    super::{
        AccessControlEntry, AclBinding, AclOperation, AclPatternType, AclPermissionType,
        AclResourceType, ResourcePattern,
    },
    DescribeAclsResult,
};

#[test]
fn throttle_and_deterministic_binding_order_remain_explicit() {
    let bindings = vec![binding("alpha", "User:alice"), binding("beta", "User:bob")];
    let result = DescribeAclsResult::new(Duration::from_millis(9), bindings);

    assert_eq!(result.throttle_time(), Duration::from_millis(9));
    assert_eq!(result.bindings()[0].pattern().name(), "alpha");
    assert_eq!(result.bindings()[1].entry().principal(), "User:bob");
    assert_eq!(result.into_bindings().len(), 2);
}

fn binding(name: &str, principal: &str) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, name, AclPatternType::LITERAL),
        AccessControlEntry::new(principal, "*", AclOperation::READ, AclPermissionType::ALLOW),
    )
}
