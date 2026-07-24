//! Scenarios for validated `DescribeConfigs` semantic input.

use super::{DescribeConfigsPlan, DescribeConfigsPlanError, DescribeConfigsResourceQuery};

#[test]
fn plan_preserves_order_flags_and_optional_key_selection() {
    let plan = DescribeConfigsPlan::new(
        vec![
            query(2, "orders", Some(&["cleanup.policy", "retention.ms"])),
            query(4, "7", None),
        ],
        true,
        true,
    )
    .unwrap_or_else(|error| panic!("valid DescribeConfigs plan: {error}"));

    assert_eq!(plan.resources().len(), 2);
    assert_eq!(
        (
            plan.resources()[0].resource_type(),
            plan.resources()[0].resource_name()
        ),
        (2, "orders")
    );
    assert_eq!(
        plan.resources()[0]
            .configuration_keys()
            .map(|keys| keys.iter().map(String::as_str).collect::<Vec<_>>()),
        Some(vec!["cleanup.policy", "retention.ms"])
    );
    assert_eq!(plan.resources()[1].configuration_keys(), None);
    assert!(plan.include_synonyms());
    assert!(plan.include_documentation());
}

#[test]
fn plan_rejects_empty_invalid_and_ambiguous_resource_queries() {
    assert_eq!(
        DescribeConfigsPlan::new(Vec::new(), false, false),
        Err(DescribeConfigsPlanError::EmptyBatch)
    );
    assert_eq!(
        DescribeConfigsPlan::new(vec![query(0, "orders", None)], false, false),
        Err(DescribeConfigsPlanError::InvalidResourceType)
    );
    assert_eq!(
        DescribeConfigsPlan::new(vec![query(-1, "orders", None)], false, false),
        Err(DescribeConfigsPlanError::InvalidResourceType)
    );
    assert_eq!(
        DescribeConfigsPlan::new(vec![query(2, "", None)], false, false),
        Err(DescribeConfigsPlanError::EmptyResourceName)
    );
    assert_eq!(
        DescribeConfigsPlan::new(
            vec![query(2, "orders", None), query(2, "orders", None)],
            false,
            false,
        ),
        Err(DescribeConfigsPlanError::DuplicateResource)
    );
    assert!(
        DescribeConfigsPlan::new(
            vec![query(2, "orders", None), query(4, "orders", None)],
            false,
            false,
        )
        .is_ok()
    );
}

#[test]
fn selected_keys_are_nonempty_unique_and_may_explicitly_select_none() {
    assert_eq!(
        DescribeConfigsPlan::new(vec![query(2, "orders", Some(&[""]))], false, false,),
        Err(DescribeConfigsPlanError::EmptyConfigurationKey)
    );
    assert_eq!(
        DescribeConfigsPlan::new(
            vec![query(
                2,
                "orders",
                Some(&["cleanup.policy", "cleanup.policy"])
            )],
            false,
            false,
        ),
        Err(DescribeConfigsPlanError::DuplicateConfigurationKey)
    );
    let explicit_empty =
        DescribeConfigsPlan::new(vec![query(2, "orders", Some(&[]))], false, false);
    assert!(explicit_empty.is_ok());
}

fn query(
    resource_type: i8,
    resource_name: &str,
    keys: Option<&[&str]>,
) -> DescribeConfigsResourceQuery {
    DescribeConfigsResourceQuery::new(
        resource_type,
        resource_name.to_owned(),
        keys.map(|keys| keys.iter().map(|key| (*key).to_owned()).collect()),
    )
}
