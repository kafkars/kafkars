//! Scenarios for validated incremental topic configuration input.

use super::{
    ConfigAlteration, ConfigAlterationOperation, IncrementalAlterConfigsPlan,
    IncrementalAlterConfigsPlanError, TopicConfigAlteration,
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

fn topic(name: &str, alterations: Vec<ConfigAlteration>) -> TopicConfigAlteration {
    TopicConfigAlteration::new(name.to_owned(), alterations)
}
