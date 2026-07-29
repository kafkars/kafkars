//! Scenarios for explicit legacy full-snapshot configuration-resource input.

use super::{
    LegacyAlterConfigsPlan, LegacyAlterConfigsPlanError, LegacyConfigEntry,
    LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};

#[test]
fn plan_preserves_topic_config_order_nullable_values_and_validate_only() {
    let plan = LegacyAlterConfigsPlan::new(
        vec![
            topic(
                "orders",
                vec![
                    LegacyConfigEntry::new("retention.ms".to_owned(), Some(String::new())),
                    LegacyConfigEntry::new("segment.ms".to_owned(), None),
                ],
            ),
            topic(
                "audit",
                vec![LegacyConfigEntry::new(
                    "cleanup.policy".to_owned(),
                    Some("compact".to_owned()),
                )],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid legacy plan: {error}"));

    assert!(plan.validate_only());
    assert_eq!(
        plan.topics()
            .iter()
            .map(LegacyTopicConfigReplacement::topic)
            .collect::<Vec<_>>(),
        ["orders", "audit"]
    );
    let configs = plan.topics()[0].configs();
    assert_eq!(configs[0].key(), "retention.ms");
    assert_eq!(configs[0].value(), Some(""));
    assert_eq!(configs[1].key(), "segment.ms");
    assert_eq!(configs[1].value(), None);
}

#[test]
fn empty_topic_snapshot_is_valid_and_unambiguously_resets_dynamic_configs() {
    let plan = LegacyAlterConfigsPlan::new(vec![topic("orders", Vec::new())], false)
        .unwrap_or_else(|error| panic!("empty snapshot is meaningful: {error}"));

    assert!(plan.topics()[0].configs().is_empty());
}

#[test]
fn plan_rejects_empty_duplicate_or_unnamed_topics() {
    assert_eq!(
        LegacyAlterConfigsPlan::new(Vec::new(), false),
        Err(LegacyAlterConfigsPlanError::EmptyBatch)
    );
    assert_eq!(
        LegacyAlterConfigsPlan::new(vec![topic("", Vec::new())], false),
        Err(LegacyAlterConfigsPlanError::EmptyTopicName)
    );
    assert_eq!(
        LegacyAlterConfigsPlan::new(
            vec![topic("orders", Vec::new()), topic("orders", Vec::new())],
            false,
        ),
        Err(LegacyAlterConfigsPlanError::DuplicateTopic)
    );
}

#[test]
fn configuration_keys_are_nonempty_and_unique_within_each_snapshot() {
    assert_eq!(
        LegacyAlterConfigsPlan::new(
            vec![topic(
                "orders",
                vec![LegacyConfigEntry::new(String::new(), None)],
            )],
            false,
        ),
        Err(LegacyAlterConfigsPlanError::EmptyConfigurationKey)
    );
    assert_eq!(
        LegacyAlterConfigsPlan::new(
            vec![topic(
                "orders",
                vec![
                    LegacyConfigEntry::new("retention.ms".to_owned(), Some("10".to_owned())),
                    LegacyConfigEntry::new("retention.ms".to_owned(), None),
                ],
            )],
            false,
        ),
        Err(LegacyAlterConfigsPlanError::DuplicateConfigurationKey)
    );
}

#[test]
fn generic_plan_preserves_known_future_and_empty_resource_snapshots() {
    let plan = LegacyAlterConfigsPlan::for_resources(
        vec![
            resource(4, "1", vec![entry("broker.key", Some("broker-value"))]),
            resource(8, "1", Vec::new()),
            resource(16, "payments-client", vec![entry("metrics", Some(""))]),
            resource(
                32,
                "payments-group",
                vec![entry("consumer.session.timeout.ms", None)],
            ),
            resource(
                64,
                "future-resource",
                vec![entry("future.key", Some("future"))],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid generic legacy plan: {error}"));

    assert!(plan.validate_only());
    assert_eq!(
        plan.resources()
            .iter()
            .map(|resource| (resource.resource_type(), resource.resource_name()))
            .collect::<Vec<_>>(),
        [
            (4, "1"),
            (8, "1"),
            (16, "payments-client"),
            (32, "payments-group"),
            (64, "future-resource"),
        ]
    );
    assert!(plan.resources()[1].configs().is_empty());
    assert_eq!(plan.resources()[2].configs()[0].value(), Some(""));
    assert_eq!(plan.resources()[3].configs()[0].value(), None);
}

#[test]
fn generic_plan_rejects_invalid_or_duplicate_exact_resource_identities() {
    for resource_type in [i8::MIN, -1, 0] {
        assert_eq!(
            LegacyAlterConfigsPlan::for_resources(
                vec![resource(resource_type, "name", Vec::new())],
                false,
            ),
            Err(LegacyAlterConfigsPlanError::NonPositiveResourceType)
        );
    }
    assert_eq!(
        LegacyAlterConfigsPlan::for_resources(vec![resource(4, "", Vec::new())], false),
        Err(LegacyAlterConfigsPlanError::EmptyResourceName)
    );
    assert_eq!(
        LegacyAlterConfigsPlan::for_resources(
            vec![
                resource(4, "1", vec![entry("first", None)]),
                resource(4, "1", vec![entry("second", None)]),
            ],
            false,
        ),
        Err(LegacyAlterConfigsPlanError::DuplicateResource)
    );
    assert!(
        LegacyAlterConfigsPlan::for_resources(
            vec![resource(4, "1", Vec::new()), resource(8, "1", Vec::new()),],
            false,
        )
        .is_ok()
    );
}

#[test]
fn broker_resource_names_are_canonical_nonnegative_i32_ids() {
    for resource_type in [4, 8] {
        for invalid_name in ["-1", "+1", "00", "01", " 1", "1 ", "1.0", "2147483648"] {
            assert_eq!(
                LegacyAlterConfigsPlan::for_resources(
                    vec![resource(resource_type, invalid_name, Vec::new())],
                    false,
                ),
                Err(LegacyAlterConfigsPlanError::InvalidBrokerResourceName)
            );
        }
    }
    assert!(
        LegacyAlterConfigsPlan::for_resources(
            vec![
                resource(4, "0", Vec::new()),
                resource(8, "2147483647", Vec::new()),
            ],
            false,
        )
        .is_ok()
    );
}

fn topic(name: &str, configs: Vec<LegacyConfigEntry>) -> LegacyTopicConfigReplacement {
    LegacyTopicConfigReplacement::new(name.to_owned(), configs)
}

fn resource(
    resource_type: i8,
    resource_name: &str,
    configs: Vec<LegacyConfigEntry>,
) -> LegacyConfigResourceReplacement {
    LegacyConfigResourceReplacement::resource(resource_type, resource_name.to_owned(), configs)
}

fn entry(key: &str, value: Option<&str>) -> LegacyConfigEntry {
    LegacyConfigEntry::new(key.to_owned(), value.map(str::to_owned))
}
