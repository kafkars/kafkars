//! Scenarios for validated incremental configuration-resource input.

use super::{
    ConfigAlteration, ConfigAlterationOperation, IncrementalAlterConfigsPlan,
    IncrementalAlterConfigsPlanError, IncrementalConfigResourceAlteration, TopicConfigAlteration,
};

#[test]
fn plan_preserves_topic_operation_order_flags_and_exact_value_semantics() {
    let plan = IncrementalAlterConfigsPlan::new(
        vec![
            topic(
                "orders",
                vec![
                    ConfigAlteration::set("retention.ms".to_owned(), String::new()),
                    ConfigAlteration::delete("segment.ms".to_owned()),
                    ConfigAlteration::append("cleanup.policy".to_owned(), "compact".to_owned()),
                    ConfigAlteration::subtract("compression.type".to_owned(), "gzip".to_owned()),
                ],
            ),
            topic(
                "audit",
                vec![ConfigAlteration::set(
                    "min.insync.replicas".to_owned(),
                    "2".to_owned(),
                )],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid incremental plan: {error}"));

    assert!(plan.validate_only());
    assert_eq!(
        plan.topics()
            .iter()
            .map(TopicConfigAlteration::topic)
            .collect::<Vec<_>>(),
        ["orders", "audit"]
    );
    let alterations = plan.topics()[0].alterations();
    assert_eq!(alterations[0].key(), "retention.ms");
    assert_eq!(
        alterations[0].operation(),
        &ConfigAlterationOperation::Set(String::new())
    );
    assert_eq!(alterations[0].operation().value(), Some(""));
    assert_eq!(alterations[1].operation().value(), None);
    assert_eq!(alterations[2].operation().value(), Some("compact"));
    assert_eq!(alterations[3].operation().value(), Some("gzip"));
}

#[test]
fn plan_rejects_empty_duplicate_or_ambiguous_topic_inputs() {
    assert_eq!(
        IncrementalAlterConfigsPlan::new(Vec::new(), false),
        Err(IncrementalAlterConfigsPlanError::EmptyBatch)
    );
    assert_eq!(
        IncrementalAlterConfigsPlan::new(
            vec![topic(
                "",
                vec![ConfigAlteration::delete("segment.ms".to_owned())],
            )],
            false,
        ),
        Err(IncrementalAlterConfigsPlanError::EmptyTopicName)
    );
    assert_eq!(
        IncrementalAlterConfigsPlan::new(
            vec![
                topic(
                    "orders",
                    vec![ConfigAlteration::delete("segment.ms".to_owned())],
                ),
                topic(
                    "orders",
                    vec![ConfigAlteration::delete("retention.ms".to_owned())],
                ),
            ],
            false,
        ),
        Err(IncrementalAlterConfigsPlanError::DuplicateTopic)
    );
    assert_eq!(
        IncrementalAlterConfigsPlan::new(vec![topic("orders", Vec::new())], false),
        Err(IncrementalAlterConfigsPlanError::EmptyAlterations)
    );
}

#[test]
fn configuration_keys_are_nonempty_and_unique_within_each_topic() {
    assert_eq!(
        IncrementalAlterConfigsPlan::new(
            vec![topic(
                "orders",
                vec![ConfigAlteration::delete(String::new())],
            )],
            false,
        ),
        Err(IncrementalAlterConfigsPlanError::EmptyConfigurationKey)
    );
    assert_eq!(
        IncrementalAlterConfigsPlan::new(
            vec![topic(
                "orders",
                vec![
                    ConfigAlteration::set("retention.ms".to_owned(), "10".to_owned()),
                    ConfigAlteration::delete("retention.ms".to_owned()),
                ],
            )],
            false,
        ),
        Err(IncrementalAlterConfigsPlanError::DuplicateConfigurationKey)
    );
}

#[test]
fn generic_plan_preserves_known_and_future_positive_resource_identities() {
    let plan = IncrementalAlterConfigsPlan::for_resources(
        vec![
            resource(4, "1", "log.cleaner.threads"),
            resource(8, "1", "kafka.controller"),
            resource(16, "payments-client", "metrics"),
            resource(32, "payments-group", "consumer.session.timeout.ms"),
            resource(64, "future-resource", "future.key"),
        ],
        true,
    )
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
}

#[test]
fn generic_plan_rejects_invalid_or_duplicate_exact_resource_identities() {
    for resource_type in [i8::MIN, -1, 0] {
        assert_eq!(
            IncrementalAlterConfigsPlan::for_resources(
                vec![resource(resource_type, "name", "key")],
                false,
            ),
            Err(IncrementalAlterConfigsPlanError::NonPositiveResourceType)
        );
    }
    assert_eq!(
        IncrementalAlterConfigsPlan::for_resources(vec![resource(4, "", "key")], false),
        Err(IncrementalAlterConfigsPlanError::EmptyResourceName)
    );
    assert_eq!(
        IncrementalAlterConfigsPlan::for_resources(
            vec![resource(4, "1", "first"), resource(4, "1", "second")],
            false,
        ),
        Err(IncrementalAlterConfigsPlanError::DuplicateResource)
    );

    let distinct_types = IncrementalAlterConfigsPlan::for_resources(
        vec![resource(4, "1", "first"), resource(8, "1", "second")],
        false,
    );
    assert!(distinct_types.is_ok());
}

#[test]
fn broker_resource_names_are_canonical_nonnegative_i32_ids() {
    for resource_type in [4, 8] {
        for invalid_name in ["-1", "+1", "00", "01", " 1", "1 ", "1.0", "2147483648"] {
            assert_eq!(
                IncrementalAlterConfigsPlan::for_resources(
                    vec![resource(resource_type, invalid_name, "key")],
                    false,
                ),
                Err(IncrementalAlterConfigsPlanError::InvalidBrokerResourceName)
            );
        }
    }

    let plan = IncrementalAlterConfigsPlan::for_resources(
        vec![
            resource(4, "0", "broker.key"),
            resource(8, "2147483647", "logger.key"),
        ],
        false,
    );
    assert!(plan.is_ok());
}

fn topic(name: &str, alterations: Vec<ConfigAlteration>) -> TopicConfigAlteration {
    TopicConfigAlteration::new(name.to_owned(), alterations)
}

fn resource(
    resource_type: i8,
    resource_name: &str,
    key: &str,
) -> IncrementalConfigResourceAlteration {
    IncrementalConfigResourceAlteration::resource(
        resource_type,
        resource_name.to_owned(),
        vec![ConfigAlteration::delete(key.to_owned())],
    )
}
