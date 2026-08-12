//! Completion-generation fencing and retirement scenarios.

use kafka_client_core::OperationId;

use super::{
    binding::OperationBindings,
    flush::FlushBindings,
    reclaim::{CompletionReclaimError, CompletionReclaimOutcome, CompletionReclaimer},
    reclaim_test::{binding_deadline, publish_observed, stop, terminal_operation},
    terminal_backlog::ProducerTerminalOwner,
};
use crate::completion::{CompletionRegistry, CompletionRegistryError, ReclaimStatus};

#[test]
fn a_stale_slot_generation_cannot_authorize_live_reclamation() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let terminal = terminal_operation();
    let Ok((stale_id, stale_observer)) = registry.reserve() else {
        panic!("stale generation should reserve")
    };
    assert_eq!(registry.rollback_reservation(stale_id), Ok(()));
    drop(stale_observer);
    let live_id = publish_observed(&mut registry, terminal.completion);
    assert_ne!(live_id, stale_id);
    let stale_operation = OperationId::from_raw(99);
    let mut bindings = OperationBindings::new(1);
    assert_eq!(
        bindings.bind(stale_operation, stale_id, binding_deadline()),
        Ok(())
    );
    let mut reclaimer = CompletionReclaimer::new();
    let flush_bindings = FlushBindings::new(1);
    assert_eq!(
        reclaimer.next_input(&mut registry, &bindings, &flush_bindings),
        Err(CompletionReclaimError::UnknownBinding(live_id))
    );
    assert_eq!(bindings.completion(stale_operation), Some(stale_id));
    assert_eq!(
        registry.finish_reclaim(live_id),
        Ok(ReclaimStatus::Reclaimed)
    );
    assert_eq!(bindings.remove(stale_operation), Ok(stale_id));
    stop(registry);
}

#[test]
fn exhausted_generation_retires_capacity_after_core_confirmation() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let Ok((initial_id, initial_observer)) = registry.reserve() else {
        panic!("lazy completion slot should allocate")
    };
    assert_eq!(registry.rollback_reservation(initial_id), Ok(()));
    drop(initial_observer);
    assert_eq!(registry.set_vacant_generation_for_test(0, u64::MAX), Ok(()));
    let mut terminal = terminal_operation();
    let completion_id = publish_observed(&mut registry, terminal.completion);
    let mut bindings = OperationBindings::new(1);
    assert_eq!(
        bindings.bind(terminal.operation_id, completion_id, binding_deadline()),
        Ok(())
    );
    let mut reclaimer = CompletionReclaimer::new();
    let mut flush_bindings = FlushBindings::new(1);
    let Some(input) = reclaimer
        .next_input(&mut registry, &bindings, &flush_bindings)
        .unwrap_or_else(|error| panic!("reclaim input failed: {error}"))
    else {
        panic!("reclaim input should exist")
    };
    assert!(terminal.machine.apply(input).is_ok());
    assert_eq!(
        reclaimer.confirm_core_applied(&mut registry, &mut bindings, &mut flush_bindings),
        Ok(CompletionReclaimOutcome::Retired {
            owner: ProducerTerminalOwner::Record(terminal.operation_id),
            completion_id,
        })
    );
    assert_eq!(bindings.completion(terminal.operation_id), None);
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    stop(registry);
}
