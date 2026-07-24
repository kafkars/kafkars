//! Assigned-consumer host diagnostics preserve their concrete lifecycle failure.

use crate::{completion::CompletionRegistryError, consumer::AssignedConsumerFaultKind};

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
