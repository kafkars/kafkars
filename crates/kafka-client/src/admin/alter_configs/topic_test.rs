//! Public ordered topic alteration construction scenarios.

use super::{ConfigAlteration, TopicConfigAlterations};

#[test]
fn topic_and_change_order_remain_inert_until_submission() {
    let topic = TopicConfigAlterations::new(
        "orders",
        [
            ConfigAlteration::set("cleanup.policy", "compact"),
            ConfigAlteration::delete("retention.ms"),
        ],
    );
    assert_eq!(topic.topic(), "orders");
    assert_eq!(topic.alterations()[0].key(), "cleanup.policy");
    assert_eq!(topic.alterations()[1].key(), "retention.ms");

    let empty = TopicConfigAlterations::new("audit", []);
    assert!(empty.alterations().is_empty());
}
