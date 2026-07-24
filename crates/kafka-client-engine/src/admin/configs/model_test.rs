//! Topic routing and retained-capacity scenarios for `DescribeConfigs`.

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
    assert!(request.into_topic_plan().is_ok());
}

#[test]
fn broker_specific_and_mixed_batches_are_definitely_unsupported() {
    for request in [
        DescribeConfigsRequest::new(vec![resource(4, "7")], false, false),
        DescribeConfigsRequest::new(vec![resource(2, "orders"), resource(4, "7")], false, false),
    ] {
        assert!(matches!(
            request.into_topic_plan(),
            Err(DescribeConfigsRequestError::UnsupportedResource)
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
    let Ok(plan) = request.into_topic_plan() else {
        panic!("topic request should validate");
    };
    assert!(plan.include_synonyms());
    assert!(plan.include_documentation());
    assert_eq!(plan.resources()[0].resource_name(), "orders");
    assert_eq!(plan.resources()[1].resource_name(), "audit");
}
