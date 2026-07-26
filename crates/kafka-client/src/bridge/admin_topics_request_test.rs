//! Prepared topic-description request ownership scenarios.

use super::admin_topics_request::DescribeTopicsAdminRequest;

#[test]
fn topic_description_request_is_linear_sendable_and_opaque() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeTopicsAdminRequest>();

    let request = DescribeTopicsAdminRequest::from_topics(["orders"]);
    assert!(format!("{request:?}").starts_with("DescribeTopicsAdminRequest"));
}
