//! Scenarios for exact two-phase producer-completion reclamation.

use kafka_client_core::{
    ByteCount, Deadline, ExplicitRecord, Moment, OperationId, PartitionIndex, PayloadId,
    ProducerCompletion, ProducerEffect, ProducerInput, ProducerMachine, TopicId,
};

use super::{
    binding::OperationBindings,
    flush::FlushBindings,
    reclaim::{CompletionReclaimError, CompletionReclaimOutcome, CompletionReclaimer},
    terminal::ProducerTerminal,
    terminal_backlog::ProducerTerminalOwner,
};
use crate::{
    clock::OperationDeadline,
    completion::{
        CompletionId, CompletionRegistry, CompletionRegistryError, ReclaimStatus,
        test_support::hold_cell_lock,
    },
};

pub(super) struct TerminalOperation {
    pub(super) machine: ProducerMachine,
    pub(super) operation_id: OperationId,
    pub(super) completion: ProducerCompletion,
}

pub(super) fn terminal_operation() -> TerminalOperation {
    let mut machine = ProducerMachine::new(ByteCount::new(64), 1);
    let admitted = machine
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(10),
            record: ExplicitRecord::new(
                PayloadId::from_raw(40),
                TopicId::from_raw(4),
                PartitionIndex::from_raw(0),
                ByteCount::new(32),
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    let Some((operation_id, batch_id)) =
        admitted.effects().iter().find_map(|effect| match effect {
            ProducerEffect::AccumulateExplicit {
                operation_id,
                batch_id,
                ..
            } => Some((*operation_id, *batch_id)),
            _ => None,
        })
    else {
        panic!("admission must identify its accumulator member")
    };
    let terminal = machine
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: ByteCount::new(32),
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("terminal transition failed: {error}"));
    let Some(completion) = terminal.effects().iter().find_map(|effect| match effect {
        ProducerEffect::Complete {
            operation_id: completed,
            completion,
        } if *completed == operation_id => Some(*completion),
        _ => None,
    }) else {
        panic!("deadline settlement must emit a terminal completion")
    };
    TerminalOperation {
        machine,
        operation_id,
        completion,
    }
}

pub(super) fn publish_observed(
    registry: &mut CompletionRegistry<ProducerTerminal>,
    completion: ProducerCompletion,
) -> CompletionId {
    let Ok((completion_id, observer)) = registry.reserve() else {
        panic!("completion capacity should reserve")
    };
    let terminal = ProducerTerminal::record(completion);
    assert_eq!(registry.publish(completion_id, terminal), Ok(()));
    assert_eq!(observer.wait(), Ok(ProducerTerminal::record(completion)));
    completion_id
}

pub(super) fn stop(mut registry: CompletionRegistry<ProducerTerminal>) {
    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("settled completion notifier should stop: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}

#[test]
fn core_confirmation_precedes_exact_binding_and_capacity_release() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let mut terminal = terminal_operation();
    let completion_id = publish_observed(&mut registry, terminal.completion);
    let mut bindings = OperationBindings::new(1);
    assert_eq!(
        bindings.bind(terminal.operation_id, completion_id, binding_deadline()),
        Ok(())
    );
    let mut reclaimer = CompletionReclaimer::new();
    let mut flush_bindings = FlushBindings::new(1);
    let input = reclaimer
        .next_input(&mut registry, &bindings, &flush_bindings)
        .unwrap_or_else(|error| panic!("reclaim input failed: {error}"));
    assert_eq!(
        input,
        Some(ProducerInput::CompletionReclaimed {
            operation_id: terminal.operation_id,
        })
    );
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    assert_eq!(
        bindings.completion(terminal.operation_id),
        Some(completion_id)
    );
    let Some(input) = input else {
        panic!("reclaim input should exist")
    };
    assert!(terminal.machine.apply(input).is_ok());
    assert_eq!(
        reclaimer.confirm_core_applied(&mut registry, &mut bindings, &mut flush_bindings),
        Ok(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Record(terminal.operation_id),
            completion_id,
        })
    );
    assert_eq!(bindings.completion(terminal.operation_id), None);
    let Ok((replacement, observer)) = registry.reserve() else {
        panic!("reclaimed completion capacity should be reusable")
    };
    assert_eq!(registry.rollback_reservation(replacement), Ok(()));
    drop(observer);
    stop(registry);
}

#[test]
fn registry_retry_never_emits_a_second_core_input() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
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
    let Some((release, lock)) = hold_cell_lock(&registry, completion_id) else {
        panic!("completion cell lock should be held")
    };
    assert_eq!(
        reclaimer.confirm_core_applied(&mut registry, &mut bindings, &mut flush_bindings),
        Ok(CompletionReclaimOutcome::Retry)
    );
    assert_eq!(
        reclaimer.next_input(&mut registry, &bindings, &flush_bindings),
        Err(CompletionReclaimError::InvalidPhase)
    );
    assert_eq!(
        bindings.completion(terminal.operation_id),
        Some(completion_id)
    );
    assert!(release.send(()).is_ok());
    assert!(lock.join().is_ok());
    assert_eq!(
        reclaimer.retry_finish(&mut registry, &mut bindings, &mut flush_bindings),
        Ok(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Record(terminal.operation_id),
            completion_id,
        })
    );
    stop(registry);
}

#[test]
fn missing_exact_binding_faults_without_releasing_capacity() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("completion notifier should start")
    };
    let terminal = terminal_operation();
    let completion_id = publish_observed(&mut registry, terminal.completion);
    let bindings = OperationBindings::new(1);
    let flush_bindings = FlushBindings::new(1);
    let mut reclaimer = CompletionReclaimer::new();
    assert_eq!(
        reclaimer.next_input(&mut registry, &bindings, &flush_bindings),
        Err(CompletionReclaimError::UnknownBinding(completion_id))
    );
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    assert_eq!(
        reclaimer.next_input(&mut registry, &bindings, &flush_bindings),
        Err(CompletionReclaimError::InvalidPhase)
    );
    assert_eq!(
        registry.finish_reclaim(completion_id),
        Ok(ReclaimStatus::Reclaimed)
    );
    stop(registry);
}

pub(super) fn binding_deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(10), std::time::Instant::now())
}
