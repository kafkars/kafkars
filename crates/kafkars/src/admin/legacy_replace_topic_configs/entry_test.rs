//! Legacy replacement entry value-presence scenarios.

use super::LegacyTopicConfigEntry;

#[test]
fn null_and_explicit_empty_values_remain_distinct() {
    let empty = LegacyTopicConfigEntry::set("cleanup.policy", "");
    let reset = LegacyTopicConfigEntry::restore_default("retention.ms");

    assert_eq!(empty.key(), "cleanup.policy");
    assert_eq!(empty.value(), Some(""));
    assert_eq!(reset.key(), "retention.ms");
    assert_eq!(reset.value(), None);
}
