//! Generic resource validation and retained-capacity scenarios for `DescribeConfigs`.

use super::{
    DescribeConfigsRequest, DescribeConfigsResourceQuery, model::DescribeConfigsRequestError,
};

fn resource(resource_type: i8, name: &str) -> DescribeConfigsResourceQuery {
    DescribeConfigsResourceQuery::new(
        resource_type,
        name.to_owned(),
        Some(vec!["cleanup.policy".to_owned()]),
    )
}

#[test]
fn topic_batches_reserve_result_capacity_before_acceptance() {
    let request = DescribeConfigsRequest::new(vec![resource(2, "orders")], true, true);
    let retention = request
        .retention()
        .unwrap_or_else(|| panic!("bounded request charge should fit"));
    assert_eq!(retention.result_limit(), 256 * 1024);
    assert!(retention.total() > retention.result_limit());
    assert!(request.into_plan().is_ok());
}

#[test]
fn all_positive_resource_types_remain_exact_and_caller_ordered() {
    let request = DescribeConfigsRequest::new(
        vec![
            resource(4, "7"),
            resource(32, "orders-workers"),
            resource(16, "telemetry"),
            resource(64, "future-resource"),
        ],
        false,
        false,
    );
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("positive resource types should validate: {error:?}"));
    assert_eq!(
        plan.resources()
            .iter()
            .map(|resource| (resource.resource_type(), resource.resource_name()))
            .collect::<Vec<_>>(),
        [
            (4, "7"),
            (32, "orders-workers"),
            (16, "telemetry"),
            (64, "future-resource"),
        ]
    );
}

#[test]
fn nonpositive_resource_types_are_rejected_before_admission() {
    for resource_type in [0, -1, i8::MIN] {
        let request =
            DescribeConfigsRequest::new(vec![resource(resource_type, "invalid")], false, false);
        assert!(matches!(
            request.into_plan(),
            Err(DescribeConfigsRequestError::Invalid(
                kafka_client_core::DescribeConfigsPlanError::InvalidResourceType
            ))
        ));
    }
}

#[test]
fn include_flags_are_prepared_without_changing_resource_order() {
    let request = DescribeConfigsRequest::new(
        vec![resource(2, "orders"), resource(2, "audit")],
        false,
        false,
    )
    .with_include_synonyms(true)
    .with_include_documentation(true);
    let Ok(plan) = request.into_plan() else {
        panic!("resource request should validate");
    };
    assert!(plan.include_synonyms());
    assert!(plan.include_documentation());
    assert_eq!(plan.resources()[0].resource_name(), "orders");
    assert_eq!(plan.resources()[1].resource_name(), "audit");
}
