//! Public generic incremental configuration change-set scenarios.

use crate::{ConfigAlteration, ConfigResourceAlterations, ConfigResourceType};

#[test]
fn resource_change_set_preserves_known_and_future_type_identity() {
    for (resource_type, name) in [
        (ConfigResourceType::Broker, "7"),
        (ConfigResourceType::Group, "orders-workers"),
        (ConfigResourceType::ClientMetrics, "telemetry"),
        (ConfigResourceType::from_raw(64), "future-resource"),
    ] {
        let changes = ConfigResourceAlterations::new(
            resource_type,
            name,
            [
                ConfigAlteration::set("one", ""),
                ConfigAlteration::delete("two"),
            ],
        );
        assert_eq!(changes.resource_type(), resource_type);
        assert_eq!(changes.resource_name(), name);
        assert_eq!(
            changes
                .alterations()
                .iter()
                .map(ConfigAlteration::key)
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
    }
}

#[test]
fn nonpositive_type_remains_inert_until_submit() {
    let changes = ConfigResourceAlterations::new(
        ConfigResourceType::from_raw(0),
        "invalid",
        [ConfigAlteration::delete("key")],
    );
    assert_eq!(changes.resource_type().as_raw(), 0);
}
