//! Deferred validation and exact request-to-core conversion evidence.

use super::{
    CreateDelegationTokenPrincipal, CreateDelegationTokenRequest,
    model::CreateDelegationTokenPlanFailure,
};

#[test]
fn request_preserves_owner_renewers_lifetime_and_order() {
    let plan = CreateDelegationTokenRequest::new(
        Some(principal("owner")),
        vec![principal("renewer-a"), principal("renewer-b")],
        Some(60_000),
    )
    .into_plan()
    .unwrap_or_else(|error| panic!("valid request: {error:?}"));

    assert_eq!(plan.minimum_version(), 3);
    assert_eq!(
        plan.owner()
            .map(kafka_client_core::DelegationTokenPrincipal::principal_name),
        Some("owner")
    );
    assert_eq!(plan.renewers()[0].principal_name(), "renewer-a");
    assert_eq!(plan.renewers()[1].principal_name(), "renewer-b");
    assert_eq!(plan.max_lifetime_ms(), Some(60_000));
}

#[test]
fn default_owner_uses_legacy_floor_and_duplicate_renewer_is_rejected() {
    let default_owner = CreateDelegationTokenRequest::new(None, vec![principal("renewer")], None)
        .into_plan()
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));
    assert_eq!(default_owner.minimum_version(), 1);

    let duplicate =
        CreateDelegationTokenRequest::new(None, vec![principal("same"), principal("same")], None);
    assert!(matches!(
        duplicate.into_plan(),
        Err(CreateDelegationTokenPlanFailure::Invalid)
    ));
}

#[test]
fn empty_principal_and_zero_lifetime_are_rejected_after_capture_conversion() {
    let empty = CreateDelegationTokenRequest::new(
        Some(CreateDelegationTokenPrincipal::new(
            "User".to_owned(),
            String::new(),
        )),
        Vec::new(),
        None,
    );
    assert!(matches!(
        empty.into_plan(),
        Err(CreateDelegationTokenPlanFailure::Invalid)
    ));

    let zero = CreateDelegationTokenRequest::new(None, vec![principal("renewer")], Some(0));
    assert!(matches!(
        zero.into_plan(),
        Err(CreateDelegationTokenPlanFailure::Invalid)
    ));
}

fn principal(name: &str) -> CreateDelegationTokenPrincipal {
    CreateDelegationTokenPrincipal::new("User".to_owned(), name.to_owned())
}
