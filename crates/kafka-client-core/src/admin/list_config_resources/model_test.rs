//! Stable resource-type and bounded request-plan scenarios.

use super::{
    ConfigResourceType, ConfigResourceTypeError, LIST_CONFIG_RESOURCES_MAX_REQUEST_TYPES,
    ListConfigResourcesPlan, ListConfigResourcesPlanError,
};

#[test]
fn known_and_future_positive_resource_types_are_stable() {
    assert_eq!(ConfigResourceType::TOPIC.code(), 2);
    assert_eq!(ConfigResourceType::BROKER.code(), 4);
    assert_eq!(ConfigResourceType::BROKER_LOGGER.code(), 8);
    assert_eq!(ConfigResourceType::CLIENT_METRICS.code(), 16);
    assert_eq!(ConfigResourceType::GROUP.code(), 32);
    assert_eq!(
        ConfigResourceType::new(127)
            .unwrap_or_else(|error| panic!("future positive type: {error}"))
            .code(),
        127
    );
    assert_eq!(
        ConfigResourceType::new(0),
        Err(ConfigResourceTypeError::NonPositive)
    );
    assert_eq!(
        ConfigResourceType::new(-1),
        Err(ConfigResourceTypeError::NonPositive)
    );
}

#[test]
fn empty_plan_selects_all_and_explicit_plan_preserves_caller_order() {
    let all = ListConfigResourcesPlan::new(Vec::new())
        .unwrap_or_else(|error| panic!("all-types plan: {error}"));
    assert!(all.lists_all_types());

    let explicit =
        ListConfigResourcesPlan::new(vec![ConfigResourceType::GROUP, ConfigResourceType::TOPIC])
            .unwrap_or_else(|error| panic!("explicit plan: {error}"));
    assert!(!explicit.lists_all_types());
    assert_eq!(
        explicit.resource_types(),
        [ConfigResourceType::GROUP, ConfigResourceType::TOPIC]
    );
}

#[test]
fn plan_rejects_duplicate_and_excessive_type_selections() {
    assert_eq!(
        ListConfigResourcesPlan::new(vec![ConfigResourceType::TOPIC, ConfigResourceType::TOPIC]),
        Err(ListConfigResourcesPlanError::DuplicateResourceType)
    );
    let types = (1..=LIST_CONFIG_RESOURCES_MAX_REQUEST_TYPES + 1)
        .map(|code| {
            ConfigResourceType::new(
                i8::try_from(code).unwrap_or_else(|_| panic!("bounded test type")),
            )
            .unwrap_or_else(|error| panic!("positive test type: {error}"))
        })
        .collect();
    assert_eq!(
        ListConfigResourcesPlan::new(types),
        Err(ListConfigResourcesPlanError::TooManyResourceTypes)
    );
}
