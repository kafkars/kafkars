//! Public topic configuration query construction scenarios.

use super::TopicConfigQuery;

#[test]
fn strings_request_all_configs_and_explicit_keys_preserve_order() {
    let all = TopicConfigQuery::from(String::from("orders"));
    assert_eq!(all.topic(), "orders");
    assert_eq!(all.selected_configuration_keys(), None);

    let selected =
        TopicConfigQuery::new("audit").configuration_keys(["cleanup.policy", "retention.ms"]);
    assert_eq!(selected.topic(), "audit");
    assert_eq!(
        selected.selected_configuration_keys(),
        Some([String::from("cleanup.policy"), String::from("retention.ms")].as_slice())
    );
}
