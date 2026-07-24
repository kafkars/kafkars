//! Private topic-only `DescribeConfigs` request bridge scenarios.

use super::admin_configs_request::DescribeConfigsAdminRequest;
use crate::TopicConfigQuery;

#[test]
fn request_bridge_is_send_and_accepts_only_topic_vocabulary() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeConfigsAdminRequest>();

    let request = DescribeConfigsAdminRequest::from_topics([
        TopicConfigQuery::new("orders").configuration_keys(["cleanup.policy"]),
        TopicConfigQuery::new("audit"),
    ])
    .with_include_synonyms(true)
    .with_include_documentation(true);
    assert!(format!("{request:?}").starts_with("DescribeConfigsAdminRequest"));
}
