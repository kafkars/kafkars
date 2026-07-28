//! Evidence for the exact generated `DescribeAcls` version window.

use super::version::{
    DESCRIBE_ACLS_MAX_VERSION, DESCRIBE_ACLS_MIN_VERSION, supports_describe_acls_version,
};

#[test]
fn generated_window_is_exactly_v1_through_v3() {
    assert_eq!(DESCRIBE_ACLS_MIN_VERSION, 1);
    assert_eq!(DESCRIBE_ACLS_MAX_VERSION, 3);
    assert!(!supports_describe_acls_version(0));
    assert!(supports_describe_acls_version(1));
    assert!(supports_describe_acls_version(2));
    assert!(supports_describe_acls_version(3));
    assert!(!supports_describe_acls_version(4));
}
