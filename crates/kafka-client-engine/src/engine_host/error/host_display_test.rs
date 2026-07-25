//! Consumer host diagnostics preserve their concrete lifecycle failures.

use crate::{
    completion::CompletionRegistryError,
    consumer::{AssignedConsumerFaultKind, GroupConsumerRegistry},
};

use super::host::EngineHostError;

#[test]
fn assigned_consumer_fault_names_its_owner() {
    let error = EngineHostError::AssignedConsumerFault(AssignedConsumerFaultKind::Clock);

    assert_eq!(error.to_string(), "assigned-consumer owner faulted: Clock");
}

#[test]
fn assigned_consumer_notifier_failure_names_its_domain() {
    let error =
        EngineHostError::AssignedConsumerCompletion(CompletionRegistryError::NotifierStopped);

    assert_eq!(
        error.to_string(),
        "assigned-consumer completion notifier failed: completion notifier has stopped"
    );
}

#[test]
fn group_consumer_failure_names_its_registry() {
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));
    let group_error = registry
        .finish_shutdown()
        .err()
        .unwrap_or_else(|| panic!("open registry cannot finish"));
    let error = EngineHostError::GroupConsumer(group_error);

    assert_eq!(
        error.to_string(),
        "group-consumer registry failed: group offset commit host invariant failed: Unsettled"
    );
    let notifier = registry
        .take_notifier()
        .unwrap_or_else(|| panic!("fallback notifier"));
    notifier
        .join_off_notifier()
        .unwrap_or_else(|join_error| panic!("notifier join: {join_error}"));
}
