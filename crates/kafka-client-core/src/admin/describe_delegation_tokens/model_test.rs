//! Explicit-all and caller-ordered owner-selection scenarios.

use super::{
    super::DelegationTokenPrincipal, DESCRIBE_DELEGATION_TOKENS_MAX_OWNERS,
    DescribeDelegationTokensPlan, DescribeDelegationTokensPlanError,
    DescribeDelegationTokensSelection,
};

#[test]
fn all_is_explicit_and_empty_owners_never_mean_all() {
    assert_eq!(
        DescribeDelegationTokensPlan::all().selection(),
        &DescribeDelegationTokensSelection::All
    );
    assert_eq!(
        DescribeDelegationTokensPlan::for_owners(Vec::new()),
        Err(DescribeDelegationTokensPlanError::EmptyOwners)
    );
}

#[test]
fn owner_filter_is_unique_bounded_and_preserves_caller_order() {
    let plan = DescribeDelegationTokensPlan::for_owners(vec![
        principal("User", "bob"),
        principal("Service", "billing"),
    ])
    .unwrap_or_else(|error| panic!("valid owners: {error}"));
    let DescribeDelegationTokensSelection::Owners(owners) = plan.selection() else {
        panic!("filtered selection expected");
    };
    assert_eq!(owners[0].principal_name(), "bob");
    assert_eq!(owners[1].principal_name(), "billing");

    assert_eq!(
        DescribeDelegationTokensPlan::for_owners(vec![
            principal("User", "bob"),
            principal("User", "bob"),
        ]),
        Err(DescribeDelegationTokensPlanError::DuplicateOwner)
    );
    assert_eq!(
        DescribeDelegationTokensPlan::for_owners(
            (0..=DESCRIBE_DELEGATION_TOKENS_MAX_OWNERS)
                .map(|index| principal("User", &format!("owner-{index}")))
                .collect()
        ),
        Err(DescribeDelegationTokensPlanError::TooManyOwners)
    );
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type.to_owned(), principal_name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}
