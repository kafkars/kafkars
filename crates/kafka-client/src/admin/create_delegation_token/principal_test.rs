//! Public delegation-token principal ownership and inert-shape scenarios.

use super::DelegationTokenPrincipal;

#[test]
fn principal_preserves_exact_caller_spelling_without_early_validation() {
    let principal = DelegationTokenPrincipal::new("User", "alice");
    assert_eq!(principal.principal_type(), "User");
    assert_eq!(principal.principal_name(), "alice");

    let empty = DelegationTokenPrincipal::new("", "");
    assert_eq!(empty.principal_type(), "");
    assert_eq!(empty.principal_name(), "");
}

#[test]
fn principal_is_stable_owned_vocabulary() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<DelegationTokenPrincipal>();
    let principal = DelegationTokenPrincipal::new("Service", "reporter");
    assert_eq!(principal.clone(), principal);
}
