//! Exact API-key 45 version window scenarios.

use kafka_wire_core::ApiVersion;

use super::version::{minimum_version_for_policy, validate_selected_version};

#[test]
fn default_policy_accepts_v0_v1_and_explicit_false_requires_v1() {
    assert_eq!(minimum_version_for_policy(true), ApiVersion::new(0));
    assert_eq!(minimum_version_for_policy(false), ApiVersion::new(1));
    assert!(validate_selected_version(0, true).is_ok());
    assert!(validate_selected_version(1, true).is_ok());
    assert!(validate_selected_version(0, false).is_err());
    assert!(validate_selected_version(1, false).is_ok());
    assert!(validate_selected_version(-1, true).is_err());
    assert!(validate_selected_version(2, true).is_err());
}
