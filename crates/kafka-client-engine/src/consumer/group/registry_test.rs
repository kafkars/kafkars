//! Bounded registration, lossless rejection, and identity scenarios.

use std::sync::Arc;

use super::{
    registry::{
        GROUP_CONSUMER_CAPACITY, GROUP_CONSUMER_RETAINED_NAME_BYTES,
        GroupConsumerRegistrationFailureKind,
    },
    registry_test_support::{register, started_registry, stop_registry},
    session_catalog::{GroupSessionCatalogError, MAX_KAFKA_GROUP_STRING_BYTES},
};

#[test]
fn registration_is_bounded_and_group_identities_are_monotonic() {
    let mut registry = started_registry();
    let first = register(&mut registry, "alpha");
    let second = register(&mut registry, "beta");

    assert!(second > first);
    assert_eq!(registry.registered_group_count(), 2);
    assert_eq!(
        registry.retained_group_bytes(),
        "alpha".len() + "beta".len()
    );
    assert_eq!(registry.entries.capacity(), GROUP_CONSUMER_CAPACITY);
    stop_registry(&mut registry);
}

#[test]
fn duplicate_group_spellings_receive_distinct_owner_identities() {
    let mut registry = started_registry();
    let first = register(&mut registry, "same-kafka-group");
    let second = register(&mut registry, "same-kafka-group");

    assert!(second > first);
    assert_ne!(second, first);
    assert_eq!(registry.registered_group_count(), 2);
    assert_eq!(
        registry.retained_group_bytes(),
        2 * "same-kafka-group".len()
    );
    stop_registry(&mut registry);
}

#[test]
fn catalog_rejection_returns_the_exact_group_allocation() {
    let mut registry = started_registry();
    let group: Arc<str> = Arc::from("");
    let retained = Arc::clone(&group);
    let failure = registry
        .try_register(
            group,
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
        )
        .err()
        .unwrap_or_else(|| panic!("empty group must be rejected"));

    assert_eq!(
        failure.kind,
        GroupConsumerRegistrationFailureKind::Catalog(GroupSessionCatalogError::EmptyGroup)
    );
    assert!(Arc::ptr_eq(&failure.group, &retained));
    assert_eq!(failure.local_topics, vec![Arc::<str>::from("orders")]);
    assert_eq!(registry.registered_group_count(), 0);
    assert_eq!(registry.retained_group_bytes(), 0);
    stop_registry(&mut registry);
}

#[test]
fn count_and_aggregate_group_name_bytes_have_exact_caps() {
    let mut count_registry = started_registry();
    for index in 0..GROUP_CONSUMER_CAPACITY {
        register(&mut count_registry, &format!("group-{index}"));
    }
    let count_failure = count_registry
        .try_register(
            Arc::from("one-too-many"),
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
        )
        .err()
        .unwrap_or_else(|| panic!("entry capacity must reject"));
    assert_eq!(
        count_failure.kind,
        GroupConsumerRegistrationFailureKind::Capacity
    );
    stop_registry(&mut count_registry);

    let mut byte_registry = started_registry();
    let maximum = "x".repeat(MAX_KAFKA_GROUP_STRING_BYTES);
    for _index in 0..GROUP_CONSUMER_CAPACITY {
        register(&mut byte_registry, &maximum);
    }
    assert_eq!(
        byte_registry.retained_group_bytes(),
        GROUP_CONSUMER_RETAINED_NAME_BYTES
    );
    stop_registry(&mut byte_registry);
}
