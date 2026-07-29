//! Public generic legacy replacement identity and empty-snapshot scenarios.

use crate::{ConfigResourceType, LegacyConfigResourceReplacement, LegacyTopicConfigEntry};

#[test]
fn replacement_preserves_known_future_and_empty_destructive_snapshots() {
    for (resource_type, name) in [
        (ConfigResourceType::Broker, "7"),
        (ConfigResourceType::BrokerLogger, "7"),
        (ConfigResourceType::ClientMetrics, "telemetry"),
        (ConfigResourceType::Group, "orders-workers"),
        (ConfigResourceType::from_raw(64), "future-resource"),
    ] {
        let replacement = LegacyConfigResourceReplacement::new(
            resource_type,
            name,
            [LegacyTopicConfigEntry::restore_default("key")],
        );
        assert_eq!(replacement.resource_type(), resource_type);
        assert_eq!(replacement.resource_name(), name);
        assert_eq!(replacement.entries()[0].value(), None);
    }

    let empty = LegacyConfigResourceReplacement::new(ConfigResourceType::BrokerLogger, "8", []);
    assert!(empty.entries().is_empty());
}

#[test]
fn nonpositive_type_remains_inert_until_submit() {
    let replacement =
        LegacyConfigResourceReplacement::new(ConfigResourceType::from_raw(0), "invalid", []);
    assert_eq!(replacement.resource_type().as_raw(), 0);
}
