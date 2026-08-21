//! Public-to-engine all-or-owner selection translation scenarios.

use crate::admin::DelegationTokenPrincipal;

use super::DescribeDelegationTokensAdminRequest;

#[test]
fn all_and_selected_owners_remain_distinct_after_conversion() {
    assert!(
        DescribeDelegationTokensAdminRequest::all()
            .into_engine()
            .owners()
            .is_none()
    );

    let selected = DescribeDelegationTokensAdminRequest::owners(vec![
        principal("User", "alice"),
        principal("Service", "reporter"),
    ])
    .into_engine();
    let owners = selected
        .owners()
        .unwrap_or_else(|| panic!("selected owners must remain explicit"));
    assert_eq!(owners.len(), 2);
    assert_eq!(owners[0].principal_name(), "alice");
    assert_eq!(owners[1].principal_name(), "reporter");
}

#[test]
fn empty_and_duplicate_owner_filters_remain_explicit_and_inert() {
    let empty = DescribeDelegationTokensAdminRequest::owners(Vec::new()).into_engine();
    assert_eq!(
        empty
            .owners()
            .map(<[kafka_client_engine::DescribeDelegationTokenPrincipal]>::len),
        Some(0)
    );

    let duplicate = DescribeDelegationTokensAdminRequest::owners(vec![
        principal("User", "alice"),
        principal("User", "alice"),
    ])
    .into_engine();
    assert_eq!(
        duplicate
            .owners()
            .map(<[kafka_client_engine::DescribeDelegationTokenPrincipal]>::len),
        Some(2)
    );
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type, principal_name)
}
