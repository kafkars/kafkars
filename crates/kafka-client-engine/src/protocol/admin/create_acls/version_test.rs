//! Evidence for the exact generated `CreateAcls` version window.

use super::version::{
    CREATE_ACLS_MAX_VERSION, CREATE_ACLS_MIN_VERSION, supports_create_acls_version,
};

#[test]
fn generated_window_is_exactly_v1_through_v3() {
    assert_eq!(CREATE_ACLS_MIN_VERSION, 1);
    assert_eq!(CREATE_ACLS_MAX_VERSION, 3);
    assert!(!supports_create_acls_version(0));
    assert!(supports_create_acls_version(1));
    assert!(supports_create_acls_version(2));
    assert!(supports_create_acls_version(3));
    assert!(!supports_create_acls_version(4));
}
