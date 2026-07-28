//! Scenarios for inert engine Admin `DeleteRecords` requests.

use super::{DeleteRecordsRequest, DeleteRecordsRequestTarget};

#[test]
fn request_preserves_caller_order_and_specs_until_core_validation() {
    let request = DeleteRecordsRequest::new(vec![target("orders", 2, 91), target("audit", 0, -1)])
        .canonicalize();
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].before_offset(), 91);
    assert_eq!(plan.targets()[1].topic(), "audit");
    assert_eq!(plan.targets()[1].before_offset(), -1);
}

#[test]
fn invalid_scalar_facts_remain_inert_until_plan_conversion() {
    let request = DeleteRecordsRequest::new(vec![target("orders", -1, -2)]);
    assert!(request.into_plan().is_err());
}

fn target(topic: &str, partition: i32, before_offset: i64) -> DeleteRecordsRequestTarget {
    DeleteRecordsRequestTarget::new(topic.to_owned(), partition, before_offset)
}
