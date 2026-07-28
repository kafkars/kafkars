//! Bounded registration, lossless rejection, and identity scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{GroupPositionMissingOffsetPolicy, MembershipCycle, ReadIsolation};

use crate::clock::MonotonicClock;

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    registry::{
        GROUP_CONSUMER_CAPACITY, GROUP_CONSUMER_RETAINED_NAME_BYTES,
        GroupConsumerRegistrationFailureKind,
    },
    registry_commit::GroupConsumerCommitFailureKind,
    registry_cycle::GroupConsumerCycleAdmissionError,
    registry_entry::default_classic_processing_lease_policy,
    registry_test_support::{
        checkpoint, deadline, install_session, register, started_registry, stop_registry,
    },
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
fn registration_retains_read_isolation_per_entry() {
    registration_retains_configuration_per_entry();
}

fn registration_retains_configuration_per_entry() {
    let mut registry = started_registry();
    let default_group = register(&mut registry, "default");
    let committed_group = registry
        .try_register_with_configuration(
            Arc::from("committed"),
            None,
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
            GroupPositionMissingOffsetPolicy::Error,
            ReadIsolation::ReadCommitted,
            default_classic_processing_lease_policy(),
        )
        .unwrap_or_else(|failure| panic!("committed registration: {:?}", failure.kind));

    assert_eq!(
        registry
            .entry(default_group)
            .unwrap_or_else(|| panic!("default entry expected"))
            .read_isolation,
        ReadIsolation::ReadUncommitted
    );
    assert_eq!(
        registry
            .entry(committed_group)
            .unwrap_or_else(|| panic!("committed entry expected"))
            .read_isolation,
        ReadIsolation::ReadCommitted
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
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
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
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
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
        GROUP_CONSUMER_CAPACITY * MAX_KAFKA_GROUP_STRING_BYTES
    );
    stop_registry(&mut byte_registry);
}

#[test]
fn aggregate_identity_budget_accepts_maximum_group_and_instance_for_every_entry() {
    let mut registry = started_registry();
    let maximum_group: Arc<str> = Arc::from("g".repeat(MAX_KAFKA_GROUP_STRING_BYTES));
    let maximum_instance: Arc<str> = Arc::from("i".repeat(MAX_KAFKA_GROUP_STRING_BYTES));
    for _index in 0..GROUP_CONSUMER_CAPACITY {
        registry
            .try_register_with_configuration(
                Arc::clone(&maximum_group),
                Some(Arc::clone(&maximum_instance)),
                vec![Arc::from("orders")],
                super::classic_group_test_support::timing(),
                super::classic_group_test_support::heartbeat_policy(),
                super::classic_group_test_support::rejoin_policy(),
                GroupPositionMissingOffsetPolicy::Error,
                ReadIsolation::ReadUncommitted,
                default_classic_processing_lease_policy(),
            )
            .unwrap_or_else(|failure| {
                panic!(
                    "valid maximum group plus instance must fit aggregate budget: {:?}",
                    failure.kind
                )
            });
    }
    assert_eq!(
        registry.retained_group_bytes(),
        GROUP_CONSUMER_RETAINED_NAME_BYTES
    );
    stop_registry(&mut registry);
}

#[test]
fn confirmed_static_state_carries_the_configured_instance_identity() {
    let mut registry = started_registry();
    let group_id = registry
        .try_register_with_configuration(
            Arc::from("workers"),
            Some(Arc::from("instance-a")),
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
            GroupPositionMissingOffsetPolicy::Error,
            ReadIsolation::ReadUncommitted,
            default_classic_processing_lease_policy(),
        )
        .unwrap_or_else(|failure| panic!("static registration failed: {:?}", failure.kind));
    install_session(&mut registry, group_id);
    {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("registered entry expected"));
        entry.catalog.stage_installed_assignment_event();
        entry.catalog.confirm_sync_event();
    }

    let state = registry
        .group_state(group_id)
        .unwrap_or_else(|error| panic!("state snapshot failed: {error:?}"))
        .unwrap_or_else(|| panic!("confirmed membership state expected"));
    assert_eq!(state.metadata().group_instance_id(), Some("instance-a"));
    stop_registry(&mut registry);
}

#[test]
fn one_entry_fault_fences_only_that_groups_cycle_and_commit_admission() {
    let mut registry = started_registry();
    let faulted_group = register(&mut registry, "workers");
    install_session(&mut registry, faulted_group);
    let faulted_checkpoint = checkpoint(&registry, faulted_group);
    let cycle = MembershipCycle::try_from_raw(99)
        .unwrap_or_else(|| panic!("nonzero fault correlation cycle"));
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == faulted_group)
        .unwrap_or_else(|| panic!("registered entry expected"))
        .fault = Some(ClassicGroupEntryFault::SyncRecoverySemantic(cycle));

    let healthy_group = registry
        .try_register(
            Arc::from("other"),
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
        )
        .unwrap_or_else(|failure| panic!("healthy registration failed: {:?}", failure.kind));
    install_session(&mut registry, healthy_group);
    let healthy_checkpoint = checkpoint(&registry, healthy_group);
    let accepted_commit = registry
        .try_commit(healthy_group, deadline(100), healthy_checkpoint)
        .unwrap_or_else(|failure| panic!("healthy commit failed: {:?}", failure.kind));
    let cycle_group = register(&mut registry, "cycle");
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    assert!(
        registry
            .try_begin_classic_cycle(cycle_group, capture)
            .is_ok()
    );
    let rejected_capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    assert_eq!(
        registry.try_begin_classic_cycle(faulted_group, rejected_capture),
        Err(GroupConsumerCycleAdmissionError::EntryFault)
    );
    let commit = registry
        .try_commit(faulted_group, deadline(100), faulted_checkpoint)
        .err()
        .unwrap_or_else(|| panic!("faulted registry must reject commit"));
    assert_eq!(commit.kind, GroupConsumerCommitFailureKind::EntryFault);

    let fault = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == faulted_group)
        .and_then(|entry| entry.fault.take())
        .unwrap_or_else(|| panic!("fault owner expected"));
    assert_eq!(fault.retained_owner_count(), 1);
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let terminal = accepted_commit
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("accepted commit terminal: {error}"));
    assert!(matches!(
        terminal,
        kafka_client_core::GroupOffsetCommitTerminal::Failed(_)
    ));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
