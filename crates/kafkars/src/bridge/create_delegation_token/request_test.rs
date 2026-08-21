//! Public-to-engine delegation-token request translation scenarios.

use std::time::Duration;

use crate::admin::DelegationTokenPrincipal;

use super::CreateDelegationTokenAdminRequest;

#[test]
fn owner_renewers_and_lifetime_translate_losslessly_after_capture() {
    let request = CreateDelegationTokenAdminRequest::new(
        Some(principal("User", "owner")),
        vec![
            principal("User", "renewer-a"),
            principal("Service", "renewer-b"),
        ],
        Some(Duration::from_millis(86_400_000)),
    )
    .into_engine();

    assert_eq!(
        request
            .owner()
            .map(|value| (value.principal_type(), value.principal_name())),
        Some(("User", "owner"))
    );
    assert_eq!(request.renewers().len(), 2);
    assert_eq!(request.renewers()[0].principal_name(), "renewer-a");
    assert_eq!(request.renewers()[1].principal_name(), "renewer-b");
    assert_eq!(request.max_lifetime_ms(), Some(86_400_000));
}

#[test]
fn omitted_lifetime_preserves_server_default_and_invalid_values_remain_inert() {
    let defaulted = CreateDelegationTokenAdminRequest::new(None, Vec::new(), None).into_engine();
    assert!(defaulted.owner().is_none());
    assert_eq!(defaulted.max_lifetime_ms(), None);

    let malformed = CreateDelegationTokenAdminRequest::new(
        Some(principal("", "")),
        vec![principal("", ""), principal("", "")],
        Some(Duration::ZERO),
    )
    .into_engine();
    assert_eq!(
        malformed
            .owner()
            .map(|value| (value.principal_type(), value.principal_name())),
        Some(("", ""))
    );
    assert_eq!(malformed.renewers().len(), 2);
    assert_eq!(malformed.max_lifetime_ms(), Some(0));
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type, principal_name)
}
