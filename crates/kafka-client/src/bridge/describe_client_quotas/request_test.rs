//! Public-to-engine client-quota filter translation tests.

use crate::admin::ClientQuotaFilterComponent;

use super::{DescribeClientQuotasAdminRequest, engine::Match};

#[test]
fn empty_components_and_non_strict_default_are_preserved() {
    let request = DescribeClientQuotasAdminRequest::new(Vec::new()).into_engine();

    assert!(request.components().is_empty());
    assert!(!request.strict());
}

#[test]
fn exact_default_and_any_specified_matches_translate_losslessly() {
    let request = DescribeClientQuotasAdminRequest::new(vec![
        ClientQuotaFilterComponent::exact("user", "alice"),
        ClientQuotaFilterComponent::default_entity("client-id"),
        ClientQuotaFilterComponent::any_specified("ip"),
    ])
    .with_strict(true)
    .into_engine();

    assert!(request.strict());
    assert_eq!(request.components()[0].entity_type(), "user");
    assert_eq!(
        request.components()[0].selection(),
        &Match::Exact("alice".to_owned())
    );
    assert_eq!(request.components()[1].selection(), &Match::Default);
    assert_eq!(request.components()[2].selection(), &Match::AnySpecified);
}
