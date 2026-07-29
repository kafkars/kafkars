//! Deferred validation and exact request-to-core conversion evidence.

use kafka_client_core::DescribeDelegationTokensSelection;

use super::{
    DescribeDelegationTokenPrincipal, DescribeDelegationTokensRequest,
    model::DescribeDelegationTokensPlanFailure,
};

#[test]
fn all_selection_is_explicit_and_distinct_from_empty_owners() {
    let all = DescribeDelegationTokensRequest::all()
        .plan()
        .unwrap_or_else(|error| panic!("all plan: {error:?}"));
    assert!(matches!(
        all.selection(),
        DescribeDelegationTokensSelection::All
    ));

    let empty = DescribeDelegationTokensRequest::for_owners(Vec::new());
    assert!(matches!(
        empty.plan(),
        Err(DescribeDelegationTokensPlanFailure::Invalid)
    ));
}

#[test]
fn selected_owner_order_is_preserved_and_duplicates_are_rejected() {
    let request = DescribeDelegationTokensRequest::for_owners(vec![
        principal("Service", "billing"),
        principal("User", "alice"),
    ]);
    let plan = request
        .plan()
        .unwrap_or_else(|error| panic!("selected plan: {error:?}"));
    let DescribeDelegationTokensSelection::Owners(owners) = plan.selection() else {
        panic!("owners expected");
    };
    assert_eq!(owners[0].principal_type(), "Service");
    assert_eq!(owners[1].principal_name(), "alice");

    let duplicate = DescribeDelegationTokensRequest::for_owners(vec![
        principal("User", "same"),
        principal("User", "same"),
    ]);
    assert!(matches!(
        duplicate.plan(),
        Err(DescribeDelegationTokensPlanFailure::Invalid)
    ));
}

#[test]
fn invalid_principal_is_rejected_only_during_plan_conversion() {
    let request =
        DescribeDelegationTokensRequest::for_owners(vec![DescribeDelegationTokenPrincipal::new(
            "User".to_owned(),
            String::new(),
        )]);
    assert_eq!(request.owners().map(<[_]>::len), Some(1));
    assert!(matches!(
        request.plan(),
        Err(DescribeDelegationTokensPlanFailure::Invalid)
    ));
}

fn principal(principal_type: &str, name: &str) -> DescribeDelegationTokenPrincipal {
    DescribeDelegationTokenPrincipal::new(principal_type.to_owned(), name.to_owned())
}
