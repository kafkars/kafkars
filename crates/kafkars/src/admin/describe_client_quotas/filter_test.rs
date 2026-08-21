//! Stable client-quota filter value tests.

use super::{ClientQuotaFilterComponent, ClientQuotaMatch};

#[test]
fn exact_default_and_any_specified_selections_remain_distinct() {
    let exact = ClientQuotaFilterComponent::exact("user", "alice");
    let default = ClientQuotaFilterComponent::default_entity("client-id");
    let any = ClientQuotaFilterComponent::any_specified("ip");

    assert_eq!(exact.entity_type(), "user");
    assert_eq!(
        exact.selection(),
        &ClientQuotaMatch::Exact("alice".to_owned())
    );
    assert_eq!(default.selection(), &ClientQuotaMatch::Default);
    assert_eq!(any.selection(), &ClientQuotaMatch::AnySpecified);
}

#[test]
fn construction_is_inert_even_for_values_validated_at_submission() {
    let component = ClientQuotaFilterComponent::new("", ClientQuotaMatch::exact(""));
    let (entity_type, selection) = component.into_parts();

    assert!(entity_type.is_empty());
    assert_eq!(selection, ClientQuotaMatch::Exact(String::new()));
}
