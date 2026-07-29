//! Prepared topic-description request ownership scenarios.

use kafka_client_engine::DescribeTopicsRequest as EngineDescribeTopicsRequest;

use super::admin_topics_request::DescribeTopicsAdminRequest;

#[test]
fn topic_description_request_is_linear_sendable_and_opaque() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeTopicsAdminRequest>();

    let request = DescribeTopicsAdminRequest::from_topics(["orders"]);
    assert!(format!("{request:?}").starts_with("DescribeTopicsAdminRequest"));
}

#[test]
fn all_topic_options_compose_without_call_order_changing_intent() {
    let expected = EngineDescribeTopicsRequest::all(true).with_authorized_operations(true);
    let authorized_then_internal = DescribeTopicsAdminRequest::all(false)
        .with_authorized_operations(true)
        .with_include_internal(true)
        .into_engine();
    let internal_then_authorized = DescribeTopicsAdminRequest::all(false)
        .with_include_internal(true)
        .with_authorized_operations(true)
        .into_engine();

    assert_eq!(authorized_then_internal, expected);
    assert_eq!(internal_then_authorized, expected);
}
