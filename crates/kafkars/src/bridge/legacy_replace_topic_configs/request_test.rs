//! Inert legacy full-snapshot replacement request bridge tests.

use crate::admin::{
    ConfigResourceType, LegacyConfigResourceReplacement, LegacyTopicConfigEntry,
    LegacyTopicConfigReplacement,
};

use super::{
    engine::{
        Entry as EngineEntry, Request as EngineRequest, ResourceReplacement, TopicReplacement,
    },
    request::LegacyReplaceTopicConfigsAdminRequest,
};

#[test]
fn request_is_sendable_and_preserves_null_empty_order_and_empty_snapshots() {
    fn assert_send<T: Send>() {}
    assert_send::<LegacyReplaceTopicConfigsAdminRequest>();

    let request = LegacyReplaceTopicConfigsAdminRequest::new(vec![
        LegacyTopicConfigReplacement::new(
            "orders",
            [
                LegacyTopicConfigEntry::set("cleanup.policy", ""),
                LegacyTopicConfigEntry::restore_default("retention.ms"),
            ],
        ),
        LegacyTopicConfigReplacement::new("audit", []),
    ])
    .with_validate_only(true);

    assert_eq!(
        request.into_engine(),
        EngineRequest::new(vec![
            TopicReplacement::new(
                "orders".to_owned(),
                vec![
                    EngineEntry::new("cleanup.policy".to_owned(), Some(String::new())),
                    EngineEntry::new("retention.ms".to_owned(), None),
                ],
            ),
            TopicReplacement::new("audit".to_owned(), Vec::new()),
        ])
        .with_validate_only(true),
    );
}

#[test]
fn generic_request_preserves_type_name_empty_snapshot_and_validate_only() {
    let request = LegacyReplaceTopicConfigsAdminRequest::for_resources(vec![
        LegacyConfigResourceReplacement::new(
            ConfigResourceType::Broker,
            "7",
            [LegacyTopicConfigEntry::set("num.partitions", "3")],
        ),
        LegacyConfigResourceReplacement::new(ConfigResourceType::BrokerLogger, "7", []),
        LegacyConfigResourceReplacement::new(
            ConfigResourceType::from_raw(64),
            "future-resource",
            [LegacyTopicConfigEntry::restore_default("future.key")],
        ),
    ])
    .with_validate_only(true);

    assert_eq!(
        request.into_engine(),
        EngineRequest::for_resources(vec![
            ResourceReplacement::resource(
                4,
                "7".to_owned(),
                vec![EngineEntry::new(
                    "num.partitions".to_owned(),
                    Some("3".to_owned()),
                )],
            ),
            ResourceReplacement::resource(8, "7".to_owned(), Vec::new()),
            ResourceReplacement::resource(
                64,
                "future-resource".to_owned(),
                vec![EngineEntry::new("future.key".to_owned(), None)],
            ),
        ])
        .with_validate_only(true),
    );
}
