//! Evidence for the exact generated `DeleteAcls` version window.

use super::version::{
    DELETE_ACLS_MAX_VERSION, DELETE_ACLS_MIN_VERSION, supports_delete_acls_version,
};

#[test]
fn generated_window_is_exactly_v1_through_v3() {
    assert_eq!(DELETE_ACLS_MIN_VERSION, 1);
    assert_eq!(DELETE_ACLS_MAX_VERSION, 3);
    assert!(!supports_delete_acls_version(0));
    assert!(supports_delete_acls_version(1));
    assert!(supports_delete_acls_version(2));
    assert!(supports_delete_acls_version(3));
    assert!(!supports_delete_acls_version(4));
}
