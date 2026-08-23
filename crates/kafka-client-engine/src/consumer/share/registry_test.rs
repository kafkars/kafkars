//! Bounded share registration and capture-first start scenarios.

use std::{sync::Arc, time::Duration};

use super::{ShareConsumerRegistrationFailureKind, ShareConsumerRegistry, ShareConsumerStartError};

#[test]
fn registration_is_bounded_and_assigns_stable_distinct_member_identity() {
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("share registry: {error}"));
    let first = registry
        .try_register(
            Arc::from("share-a"),
            None,
            topics(),
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .unwrap_or_else(|error| panic!("first registration: {:?}", error.kind));
    let second = registry
        .try_register(
            Arc::from("share-b"),
            Some(Arc::from("r1")),
            topics(),
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .unwrap_or_else(|error| panic!("second registration: {:?}", error.kind));
    assert_ne!(first, second);
    let first_entry = registry
        .entry(first)
        .unwrap_or_else(|| panic!("first entry"));
    let second_entry = registry
        .entry(second)
        .unwrap_or_else(|| panic!("second entry"));
    assert_ne!(first_entry.member(), second_entry.member());
    assert_eq!(
        first_entry
            .local_topic_id(0)
            .unwrap_or_else(|| panic!("first topic"))
            .get(),
        1
    );
    assert_eq!(
        first_entry
            .local_topic_id(1)
            .unwrap_or_else(|| panic!("second topic"))
            .get(),
        2
    );
    assert_eq!(registry.registered_count(), 2);
    assert!(registry.retained_name_bytes() > 0);
}

#[test]
fn invalid_registration_returns_every_exact_name_without_mutation() {
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("share registry: {error}"));
    let group: Arc<str> = Arc::from("share-a");
    let rack: Arc<str> = Arc::from("r1");
    let duplicate: Arc<str> = Arc::from("orders");
    let error = registry
        .try_register(
            Arc::clone(&group),
            Some(Arc::clone(&rack)),
            vec![Arc::clone(&duplicate), Arc::clone(&duplicate)],
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .err()
        .unwrap_or_else(|| panic!("duplicate must reject"));
    assert_eq!(
        error.kind,
        ShareConsumerRegistrationFailureKind::InvalidInput
    );
    assert!(Arc::ptr_eq(&error.group, &group));
    assert!(Arc::ptr_eq(
        error.rack.as_ref().unwrap_or_else(|| panic!("rack")),
        &rack
    ));
    assert!(Arc::ptr_eq(&error.topics[0], &duplicate));
    assert!(Arc::ptr_eq(&error.topics[1], &duplicate));
    assert_eq!(registry.registered_count(), 0);
    assert_eq!(registry.retained_name_bytes(), 0);
}

#[test]
fn invalid_fetch_policy_returns_the_exact_configuration_without_mutation() {
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("share registry: {error}"));
    let defaults = crate::EngineShareConsumerFetchConfig::default();
    let fetch = crate::EngineShareConsumerFetchConfig::new(
        defaults.max_wait(),
        defaults.min_bytes(),
        defaults.max_bytes(),
        0,
        defaults.batch_size(),
        defaults.attempt_timeout(),
    );
    let error = registry
        .try_register(Arc::from("share-a"), None, topics(), fetch)
        .err()
        .unwrap_or_else(|| panic!("zero max records must reject"));

    assert_eq!(
        error.kind,
        ShareConsumerRegistrationFailureKind::InvalidInput
    );
    assert_eq!(*error.fetch, fetch);
    assert_eq!(registry.registered_count(), 0);
    assert_eq!(registry.retained_name_bytes(), 0);
}

#[test]
fn start_retains_the_original_capture_and_rejects_replacement() {
    let clock = crate::clock::MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("share registry: {error}"));
    let group_id = registry
        .try_register(
            Arc::from("share-a"),
            None,
            topics(),
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .unwrap_or_else(|error| panic!("registration: {:?}", error.kind));
    registry
        .try_begin(group_id, capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert_eq!(
        registry.entry(group_id).and_then(|entry| entry.start),
        Some(capture)
    );
    assert_eq!(
        registry.try_begin(group_id, capture),
        Err(ShareConsumerStartError::AlreadyStarted)
    );
}

fn topics() -> Vec<Arc<str>> {
    vec![Arc::from("orders"), Arc::from("payments")]
}
