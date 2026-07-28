//! Focused evidence for the generated API-key 48 version window.

use super::version::{
    DESCRIBE_CLIENT_QUOTAS_MAX_VERSION, DESCRIBE_CLIENT_QUOTAS_MIN_VERSION,
    supports_describe_client_quotas_version,
};

#[test]
fn version_window_is_exactly_v0_through_v1() {
    assert_eq!(DESCRIBE_CLIENT_QUOTAS_MIN_VERSION, 0);
    assert_eq!(DESCRIBE_CLIENT_QUOTAS_MAX_VERSION, 1);
    assert!(!supports_describe_client_quotas_version(-1));
    assert!(supports_describe_client_quotas_version(0));
    assert!(supports_describe_client_quotas_version(1));
    assert!(!supports_describe_client_quotas_version(2));
}
