//! Principal, owner-version, renewer-order, and lifetime validation scenarios.

use super::{
    CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES, CREATE_DELEGATION_TOKEN_MAX_RENEWERS,
    CreateDelegationTokenPlan, CreateDelegationTokenPlanError, DelegationTokenPrincipal,
};

#[test]
fn plan_preserves_owner_renewer_order_lifetime_and_version_requirement() {
    let default_owner = CreateDelegationTokenPlan::new(
        None,
        vec![principal("User", "bob"), principal("Service", "billing")],
        None,
    )
    .unwrap_or_else(|error| panic!("default owner plan: {error}"));
    assert_eq!(default_owner.minimum_version(), 1);
    assert_eq!(default_owner.max_lifetime_ms(), None);
    assert_eq!(
        default_owner
            .renewers()
            .iter()
            .map(DelegationTokenPrincipal::principal_name)
            .collect::<Vec<_>>(),
        vec!["bob", "billing"]
    );

    let explicit_owner = CreateDelegationTokenPlan::new(
        Some(principal("User", "alice")),
        Vec::new(),
        Some(86_400_000),
    )
    .unwrap_or_else(|error| panic!("explicit owner plan: {error}"));
    assert_eq!(explicit_owner.minimum_version(), 3);
    assert_eq!(
        explicit_owner
            .owner()
            .map(DelegationTokenPrincipal::principal_name),
        Some("alice")
    );
    assert_eq!(explicit_owner.max_lifetime_ms(), Some(86_400_000));
}

#[test]
fn principal_fields_are_nonempty_and_bounded() {
    for (principal_type, principal_name, error) in [
        (
            "",
            "alice",
            CreateDelegationTokenPlanError::EmptyPrincipalType,
        ),
        (
            "User",
            "",
            CreateDelegationTokenPlanError::EmptyPrincipalName,
        ),
    ] {
        assert_eq!(
            DelegationTokenPrincipal::new(principal_type.to_owned(), principal_name.to_owned(),),
            Err(error)
        );
    }

    assert_eq!(
        DelegationTokenPrincipal::new(
            "x".repeat(CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES + 1),
            "alice".to_owned(),
        ),
        Err(CreateDelegationTokenPlanError::PrincipalTypeTooLong)
    );
    assert_eq!(
        DelegationTokenPrincipal::new(
            "User".to_owned(),
            "x".repeat(CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES + 1),
        ),
        Err(CreateDelegationTokenPlanError::PrincipalNameTooLong)
    );
}

#[test]
fn renewers_are_unique_by_exact_principal_and_bounded_by_count_and_bytes() {
    assert_eq!(
        CreateDelegationTokenPlan::new(
            None,
            vec![principal("User", "bob"), principal("User", "bob")],
            None,
        ),
        Err(CreateDelegationTokenPlanError::DuplicateRenewer)
    );

    let too_many = (0..=CREATE_DELEGATION_TOKEN_MAX_RENEWERS)
        .map(|index| principal("User", &format!("user-{index}")))
        .collect();
    assert_eq!(
        CreateDelegationTokenPlan::new(None, too_many, None),
        Err(CreateDelegationTokenPlanError::TooManyRenewers)
    );

    let oversized = (0..5)
        .map(|index| {
            principal(
                &"t".repeat(CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES),
                &format!(
                    "{index}{}",
                    "n".repeat(CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES - 1)
                ),
            )
        })
        .collect();
    assert_eq!(
        CreateDelegationTokenPlan::new(None, oversized, None),
        Err(CreateDelegationTokenPlanError::RequestTextBytesExceeded)
    );
}

#[test]
fn explicit_lifetime_is_positive_and_fits_the_wire_signed_domain() {
    assert_eq!(
        CreateDelegationTokenPlan::new(None, Vec::new(), Some(0)),
        Err(CreateDelegationTokenPlanError::ZeroMaxLifetime)
    );
    assert_eq!(
        CreateDelegationTokenPlan::new(None, Vec::new(), Some(i64::MAX as u64 + 1),),
        Err(CreateDelegationTokenPlanError::MaxLifetimeTooLarge)
    );
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type.to_owned(), principal_name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}
