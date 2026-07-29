//! Canonical resource-generic API 33 request-storage and retained-capacity scenarios.

use super::{
    LegacyAlterConfigsRequest, LegacyConfigEntry, LegacyConfigResourceReplacement,
    LegacyTopicConfigReplacement,
};

#[test]
fn canonical_request_preserves_snapshot_order_nullable_values_and_validate_only() {
    let mut topic = String::with_capacity(64);
    topic.push_str("orders");
    let mut value = String::with_capacity(64);
    value.push_str("compact");
    let mut topics = Vec::with_capacity(8);
    topics.push(LegacyTopicConfigReplacement::new(
        topic,
        vec![
            LegacyConfigEntry::new("cleanup.policy".to_owned(), Some(value)),
            LegacyConfigEntry::new("segment.ms".to_owned(), None),
        ],
    ));
    topics.push(LegacyTopicConfigReplacement::new(
        "reset-me".to_owned(),
        Vec::new(),
    ));

    let request = LegacyAlterConfigsRequest::new(topics)
        .with_validate_only(true)
        .canonicalize();
    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid legacy replacement plan: {error}"));
    assert!(plan.validate_only());
    assert_eq!(plan.topics()[0].topic(), "orders");
    assert_eq!(plan.topics()[0].configs()[0].value(), Some("compact"));
    assert_eq!(plan.topics()[0].configs()[1].value(), None);
    assert!(plan.topics()[1].configs().is_empty());
}

#[test]
fn shared_operation_charge_covers_the_separate_terminal_result_limit() {
    let request = LegacyAlterConfigsRequest::new(vec![LegacyTopicConfigReplacement::new(
        "orders".to_owned(),
        vec![LegacyConfigEntry::new(
            "cleanup.policy".to_owned(),
            Some("compact".to_owned()),
        )],
    )]);
    let retention = request
        .retention()
        .unwrap_or_else(|| panic!("small request retention fits"));

    let topic_result_floor = crate::admin::retention::result_fixed_charge(1, "orders".len())
        .and_then(|fixed| {
            fixed.checked_add(crate::admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        })
        .unwrap_or_else(|| panic!("small result retention fits"));
    assert_eq!(retention.result_limit(), topic_result_floor);
    assert!(retention.total() >= retention.result_limit());
}

#[test]
fn core_validation_rejects_ambiguous_engine_request_before_machine_construction() {
    let request = LegacyAlterConfigsRequest::new(vec![LegacyTopicConfigReplacement::new(
        "orders".to_owned(),
        vec![
            LegacyConfigEntry::new("retention.ms".to_owned(), None),
            LegacyConfigEntry::new("retention.ms".to_owned(), Some("10".to_owned())),
        ],
    )]);
    assert!(request.into_plan().is_err());
}

#[test]
fn generic_request_canonicalizes_known_future_and_empty_resource_snapshots() {
    let mut resource_name = String::with_capacity(64);
    resource_name.push_str("payments-client");
    let mut resources = Vec::with_capacity(8);
    resources.extend([
        resource(4, "1", vec![entry("broker.key", Some("value"))]),
        resource(8, "1", Vec::new()),
        resource(16, &resource_name, vec![entry("metrics", Some(""))]),
        resource(
            32,
            "payments-group",
            vec![entry("consumer.session.timeout.ms", None)],
        ),
        resource(64, "future-resource", vec![entry("future.key", Some("x"))]),
    ]);

    let request = LegacyAlterConfigsRequest::for_resources(resources)
        .with_validate_only(true)
        .canonicalize();
    assert!(request.storage_is_canonical());
    let retention = request
        .retention()
        .unwrap_or_else(|| panic!("small generic request fits"));
    assert!(retention.total() >= retention.result_limit());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid generic plan: {error}"));

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
}

#[test]
fn generic_request_rejects_invalid_and_duplicate_resource_or_key_identities() {
    for resource_type in [i8::MIN, -1, 0] {
        assert!(
            LegacyAlterConfigsRequest::for_resources(vec![resource(
                resource_type,
                "name",
                Vec::new(),
            )])
            .into_plan()
            .is_err()
        );
    }
    for resources in [
        vec![resource(4, "", Vec::new())],
        vec![
            resource(4, "1", vec![entry("first", None)]),
            resource(4, "1", vec![entry("second", None)]),
        ],
        vec![resource(
            16,
            "client",
            vec![entry("same", None), entry("same", Some("value"))],
        )],
    ] {
        assert!(
            LegacyAlterConfigsRequest::for_resources(resources)
                .into_plan()
                .is_err()
        );
    }
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
