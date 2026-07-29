//! Legacy full-snapshot topic replacement ownership scenarios.

use super::{LegacyTopicConfigEntry, LegacyTopicConfigReplacement};

#[test]
fn caller_order_and_empty_full_replacement_remain_representable() {
    let ordered = LegacyTopicConfigReplacement::new(
        "orders",
        [
            LegacyTopicConfigEntry::set("cleanup.policy", "compact"),
            LegacyTopicConfigEntry::restore_default("retention.ms"),
        ],
    );
    assert_eq!(ordered.topic(), "orders");
    assert_eq!(ordered.entries()[0].key(), "cleanup.policy");
    assert_eq!(ordered.entries()[1].key(), "retention.ms");

    let empty = LegacyTopicConfigReplacement::new("audit", []);
    assert_eq!(empty.topic(), "audit");
    assert!(empty.entries().is_empty());
}
