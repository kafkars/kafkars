//! Bounded registry-host turn, deadline, and notifier scenarios.

use kafka_client_core::Moment;

use crate::{EngineConfig, driver::DriverOwner};

use super::registry_test_support::{
    checkpoint, deadline, install_session, register, started_registry,
};

#[test]
fn one_registry_turn_expires_one_queued_commit_at_its_original_deadline() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "group-a");
    install_session(&mut registry, group_id);
    let accepted = registry
        .try_commit(group_id, deadline(7), checkpoint(&registry, group_id))
        .unwrap_or_else(|failure| panic!("commit admission: {:?}", failure.kind));
    assert_eq!(registry.unsettled(), 1);
    assert_eq!(registry.next_deadline(), Some(deadline(7).core()));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));

    assert!(
        registry
            .turn(Moment::from_tick(7), &driver)
            .unwrap_or_else(|error| panic!("registry turn: {error}"))
    );
    assert_eq!(registry.unsettled(), 0);
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
