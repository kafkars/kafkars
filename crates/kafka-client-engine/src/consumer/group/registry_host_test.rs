//! Bounded registry-host turn, deadline, and notifier scenarios.

use kafka_client_core::{
    DeliveryStatus, GroupOffsetCommitFailureKind, GroupOffsetCommitTerminal, MembershipCycle,
    Moment,
};

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    registry_test_support::{
        checkpoint, deadline, install_session, register, started_registry, stop_registry,
    },
};

#[test]
fn one_registry_turn_expires_one_queued_commit_at_its_original_deadline() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    let accepted = registry
        .try_commit(group_id, deadline(7), checkpoint(&registry, group_id))
        .unwrap_or_else(|failure| panic!("commit admission: {:?}", failure.kind));
    assert_eq!(registry.unsettled(), 2);
    assert_eq!(
        registry.next_deadline(),
        registry
            .entry(group_id)
            .and_then(|entry| entry.heartbeat.next_deadline())
    );
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));

    assert!(
        registry
            .turn(Moment::from_tick(7), &MonotonicClock::new(), &driver)
            .unwrap_or_else(|error| panic!("registry turn: {error}"))
            .progressed
    );
    assert_eq!(registry.unsettled(), 1);
    assert!(accepted.observer.wait().is_ok());
    registry.close_admission();
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn registry_exposes_one_transferable_notifier_identity() {
    let mut registry = started_registry();
    assert!(registry.notifier_thread_id().is_some());
    registry.close_admission();
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry stop: {error}"));
    assert!(registry.take_notifier().is_none());
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn offset_turn_runs_before_a_retained_membership_fault_is_reported() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    let accepted = registry
        .try_commit(group_id, deadline(7), checkpoint(&registry, group_id))
        .unwrap_or_else(|failure| panic!("commit admission: {:?}", failure.kind));
    retain_test_fault(&mut registry, group_id);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));

    assert!(
        registry
            .turn(Moment::from_tick(7), &MonotonicClock::new(), &driver)
            .is_err()
    );
    assert!(accepted.observer.wait().is_ok());

    clear_test_fault(&mut registry, group_id);
    stop_registry(&mut registry);
}

#[test]
fn shutdown_recovery_terminalizes_accepted_commit_despite_entry_fault() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    let accepted = registry
        .try_commit(group_id, deadline(100), checkpoint(&registry, group_id))
        .unwrap_or_else(|failure| panic!("commit admission: {:?}", failure.kind));
    retain_test_fault(&mut registry, group_id);

    assert!(registry.recover_after_driver_shutdown().is_err());
    let terminal = accepted
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("accepted terminal missing: {error}"));
    let GroupOffsetCommitTerminal::Failed(failure) = terminal else {
        panic!("queued recovery must fail definitely unsent");
    };
    assert_eq!(failure.kind(), GroupOffsetCommitFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);

    clear_test_fault(&mut registry, group_id);
    stop_registry(&mut registry);
}

fn retain_test_fault(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) {
    let cycle = MembershipCycle::try_from_raw(99)
        .unwrap_or_else(|| panic!("nonzero fault correlation cycle"));
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"))
        .fault = Some(ClassicGroupEntryFault::SyncRecoverySemantic(cycle));
}

fn clear_test_fault(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) {
    let fault = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .and_then(|entry| entry.fault.take())
        .unwrap_or_else(|| panic!("fault owner expected"));
    assert_eq!(fault.retained_owner_count(), 1);
}
