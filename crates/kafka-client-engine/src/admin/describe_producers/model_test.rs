//! Scenarios for inert engine Admin `DescribeProducers` requests.

use super::{AdminDescribeProducersRequest, AdminDescribeProducersRequestTarget};

#[test]
fn request_preserves_caller_order_until_core_validation() {
    let plan =
        AdminDescribeProducersRequest::new(vec![target("orders", 2), target("audit", 0)], Some(7))
            .canonicalize()
            .into_plan()
            .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(
        (plan.targets()[0].topic(), plan.targets()[0].partition()),
        ("orders", 2)
    );
    assert_eq!(
        (plan.targets()[1].topic(), plan.targets()[1].partition()),
        ("audit", 0)
    );
    assert_eq!(plan.broker_id(), Some(7));
}

#[test]
fn invalid_scalar_facts_remain_inert_until_plan_conversion() {
    let request = AdminDescribeProducersRequest::new(vec![target("orders", -1)], None);
    assert!(request.into_plan().is_err());

    let request = AdminDescribeProducersRequest::new(vec![target("orders", 0)], Some(-1));
    assert!(request.into_plan().is_err());
}

fn target(topic: &str, partition: i32) -> AdminDescribeProducersRequestTarget {
    AdminDescribeProducersRequestTarget::new(topic.to_owned(), partition)
}
