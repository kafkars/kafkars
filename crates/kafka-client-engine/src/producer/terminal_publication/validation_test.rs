//! Host rejection scenarios for invalid terminal identities.

use kafka_client_core::{
    Deadline, DeliveryStatus, Moment, OperationId, ProducerCompletion, ProducerEffect,
    ProducerFailure,
};

use crate::producer::{
    ProducerHost, ProducerHostInvariantError,
    admission::AdmittedExplicit,
    admission_test::{admit, record},
    binding::OperationBindingError,
    host_limits_test::{start, valid_limits},
    terminal_backlog::RetainedTerminal,
};
use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

#[test]
fn unknown_terminal_does_not_alias_a_valid_blocked_entry() {
    let (mut host, first) = blocked_terminal();
    let unknown = OperationId::from_raw(99);

    assert_eq!(
        host.interpret_effect_owned(
            Moment::from_tick(6),
            ProducerEffect::Complete {
                operation_id: unknown,
                completion: terminal(),
            },
        ),
        Err(ProducerHostInvariantError::Binding(
            OperationBindingError::UnknownOperation
        ))
    );
    assert_eq!(host.stats().terminal_backlog, 1);
    assert_eq!(
        host.terminal_front().map(RetainedTerminal::operation_id),
        Some(first.operation_id())
    );
    assert_eq!(
        host.poison_reason(),
        Some(ProducerHostInvariantError::Binding(
            OperationBindingError::UnknownOperation
        ))
    );
    drop(first);
}

#[test]
fn duplicate_terminal_never_creates_a_second_normal_fifo_alias() {
    let (mut host, first) = blocked_terminal();
    let operation_id = first.operation_id();

    assert_eq!(
        host.interpret_effect_owned(
            Moment::from_tick(6),
            ProducerEffect::Complete {
                operation_id,
                completion: terminal(),
            },
        ),
        Err(ProducerHostInvariantError::Binding(
            OperationBindingError::DuplicateOperation
        ))
    );
    assert_eq!(host.stats().terminal_backlog, 1);
    assert_eq!(
        host.poison_reason(),
        Some(ProducerHostInvariantError::Binding(
            OperationBindingError::DuplicateOperation
        ))
    );
    drop(first);
}

#[test]
fn stale_reused_completion_generation_never_enters_the_normal_fifo() {
    let mut host = start(valid_limits());
    let operation_id = OperationId::from_raw(77);
    let (stale, stale_observer) = host
        .completions
        .reserve()
        .unwrap_or_else(|error| panic!("stale slot should reserve: {error}"));
    host.bindings
        .bind(
            operation_id,
            stale,
            OperationDeadline::from_parts_for_test(
                Deadline::from_tick(10),
                std::time::Instant::now(),
            ),
        )
        .unwrap_or_else(|error| panic!("test binding should commit: {error}"));
    host.completions
        .rollback_reservation(stale)
        .unwrap_or_else(|error| panic!("stale generation should vacate: {error}"));
    let (reused, reused_observer) = host
        .completions
        .reserve()
        .unwrap_or_else(|error| panic!("same slot should reserve again: {error}"));
    assert_ne!(stale, reused);

    assert_eq!(
        host.interpret_effect_owned(
            Moment::from_tick(0),
            ProducerEffect::Complete {
                operation_id,
                completion: terminal(),
            },
        ),
        Err(ProducerHostInvariantError::Completion(
            CompletionRegistryError::UnknownCompletion
        ))
    );
    assert_eq!(host.stats().terminal_backlog, 0);
    assert_eq!(
        host.poison_reason(),
        Some(ProducerHostInvariantError::Completion(
            CompletionRegistryError::UnknownCompletion
        ))
    );
    drop((stale_observer, reused_observer));
}

fn blocked_terminal() -> (ProducerHost, AdmittedExplicit) {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    host.inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    (host, admitted)
}

fn terminal() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}
