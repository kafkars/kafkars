//! Election-type-sensitive API compatibility tests.

use kafka_client_core::LeaderElectionType;

use super::version::validate_selected_version;

#[test]
fn preferred_supports_v0_through_v2() {
    assert!(validate_selected_version(0, LeaderElectionType::Preferred).is_ok());
    assert!(validate_selected_version(2, LeaderElectionType::Preferred).is_ok());
    assert!(validate_selected_version(3, LeaderElectionType::Preferred).is_err());
}

#[test]
fn unclean_requires_v1() {
    assert!(validate_selected_version(0, LeaderElectionType::Unclean).is_err());
    assert!(validate_selected_version(1, LeaderElectionType::Unclean).is_ok());
    assert!(validate_selected_version(2, LeaderElectionType::Unclean).is_ok());
}
