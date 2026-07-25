//! Exact public incremental configuration operation scenarios.

use super::{ConfigAlteration, ConfigAlterationOperation};

#[test]
fn set_delete_append_and_subtract_preserve_exact_value_presence() {
    let changes = [
        ConfigAlteration::set("cleanup.policy", ""),
        ConfigAlteration::delete("retention.ms"),
        ConfigAlteration::append("compression.type", "zstd"),
        ConfigAlteration::subtract("cleanup.policy", "delete"),
    ];
    assert_eq!(changes[0].key(), "cleanup.policy");
    assert_eq!(changes[0].operation().value(), Some(""));
    assert_eq!(changes[1].operation().value(), None);
    assert_eq!(changes[2].operation().value(), Some("zstd"));
    assert_eq!(changes[3].operation().value(), Some("delete"));
    assert!(matches!(
        changes[0].operation(),
        ConfigAlterationOperation::Set(_)
    ));
    assert!(matches!(
        changes[1].operation(),
        ConfigAlterationOperation::Delete
    ));
    assert!(matches!(
        changes[2].operation(),
        ConfigAlterationOperation::Append(_)
    ));
    assert!(matches!(
        changes[3].operation(),
        ConfigAlterationOperation::Subtract(_)
    ));
}
