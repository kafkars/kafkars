//! Client-quota filter validation and caller-order scenarios.

use super::{
    ClientQuotaMatch, DescribeClientQuotaFilterComponent, DescribeClientQuotasPlan,
    DescribeClientQuotasPlanError,
};

#[test]
fn plan_preserves_caller_order_match_modes_and_strictness() {
    let plan = DescribeClientQuotasPlan::new(
        vec![
            component("user", ClientQuotaMatch::Exact("alice".to_owned())),
            component("client-id", ClientQuotaMatch::Default),
            component("ip", ClientQuotaMatch::AnySpecified),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid filter: {error}"));

    assert!(plan.strict());
    assert_eq!(
        plan.components()
            .iter()
            .map(DescribeClientQuotaFilterComponent::entity_type)
            .collect::<Vec<_>>(),
        vec!["user", "client-id", "ip"]
    );
    assert_eq!(
        plan.components()[0].match_kind(),
        &ClientQuotaMatch::Exact("alice".to_owned())
    );
}

#[test]
fn empty_components_are_the_explicit_all_entities_filter() {
    let plan = DescribeClientQuotasPlan::new(Vec::new(), false)
        .unwrap_or_else(|error| panic!("all-entities filter: {error}"));

    assert!(plan.components().is_empty());
    assert!(!plan.strict());
}

#[test]
fn plan_rejects_invalid_strings_and_duplicate_entity_types() {
    for (components, expected) in [
        (
            vec![component("", ClientQuotaMatch::Default)],
            DescribeClientQuotasPlanError::EmptyEntityType,
        ),
        (
            vec![component("user", ClientQuotaMatch::Exact(String::new()))],
            DescribeClientQuotasPlanError::EmptyExactEntityName,
        ),
        (
            vec![
                component("user", ClientQuotaMatch::Default),
                component("user", ClientQuotaMatch::AnySpecified),
            ],
            DescribeClientQuotasPlanError::DuplicateEntityType,
        ),
    ] {
        assert_eq!(
            DescribeClientQuotasPlan::new(components, false),
            Err(expected)
        );
    }
}

#[test]
fn plan_bounds_component_count_and_version_zero_strings() {
    let too_long = "x".repeat(i16::MAX as usize + 1);
    assert_eq!(
        DescribeClientQuotasPlan::new(vec![component(&too_long, ClientQuotaMatch::Default)], false),
        Err(DescribeClientQuotasPlanError::EntityTypeTooLong)
    );
    assert_eq!(
        DescribeClientQuotasPlan::new(
            vec![component("user", ClientQuotaMatch::Exact(too_long.clone()))],
            false
        ),
        Err(DescribeClientQuotasPlanError::ExactEntityNameTooLong)
    );

    let components = (0..129)
        .map(|index| component(&format!("entity-{index}"), ClientQuotaMatch::Default))
        .collect();
    assert_eq!(
        DescribeClientQuotasPlan::new(components, false),
        Err(DescribeClientQuotasPlanError::TooManyFilterComponents)
    );
}

fn component(
    entity_type: &str,
    match_kind: ClientQuotaMatch,
) -> DescribeClientQuotaFilterComponent {
    DescribeClientQuotaFilterComponent::new(entity_type.to_owned(), match_kind)
}
