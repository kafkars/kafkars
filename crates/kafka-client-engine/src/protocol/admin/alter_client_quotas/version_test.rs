//! Evidence for the deliberately closed API-key 49 generated version window.

use super::{
    ALTER_CLIENT_QUOTAS_MAX_VERSION, ALTER_CLIENT_QUOTAS_MIN_VERSION,
    version::supports_alter_client_quotas_version,
};

#[test]
fn supports_exactly_generated_v0_and_v1() {
    assert_eq!(ALTER_CLIENT_QUOTAS_MIN_VERSION, 0);
    assert_eq!(ALTER_CLIENT_QUOTAS_MAX_VERSION, 1);
    assert!(!supports_alter_client_quotas_version(-1));
    assert!(supports_alter_client_quotas_version(0));
    assert!(supports_alter_client_quotas_version(1));
    assert!(!supports_alter_client_quotas_version(2));
}
