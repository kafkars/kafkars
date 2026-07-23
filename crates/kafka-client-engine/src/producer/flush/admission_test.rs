//! Producer flush reservation, acceptance, and terminal publication scenarios.

use kafka_client_core::{Deadline, FlushId, Moment};

use super::super::{
    admission::ProducerAdmissionFailure,
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
    reclaim::CompletionReclaimOutcome,
    terminal_backlog::ProducerTerminalOwner,
};
use super::{FlushAdmissionFailure, FlushRejectionReason};
use crate::{
    clock::OperationDeadline, completion::CompletionRegistryError,
    producer::ProducerRejectionReason,
};

#[test]
fn empty_flush_reserves_and_publishes_one_terminal() {
    let mut host = start(valid_limits());
    let admitted = host
        .try_admit_flush(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("empty flush should be accepted: {error:?}"));

    assert_eq!(admitted.flush_id(), FlushId::from_raw(1));
    assert_eq!(host.unsettled_completions(), 0);
    assert_eq!(host.flush_bindings.len(), 1);
    assert_eq!(admitted.into_flush_observer().wait(), Ok(()));
}

#[test]
fn pending_flush_completes_only_after_earlier_record_terminal_decision() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let flush = host
        .try_admit_flush(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"));

    assert_eq!(host.unsettled_completions(), 2);
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    assert_eq!(host.unsettled_completions(), 0);
    assert!(record.into_delivery_observer().wait().is_err());
    assert_eq!(flush.into_flush_observer().wait(), Ok(()));
}

#[test]
fn shared_completion_capacity_rejects_flush_before_core_acceptance() {
    let limits = valid_limits();
    let capacity = limits.completion_capacity;
    let mut host = start(limits);
    let records: Vec<_> = (0..capacity)
        .map(|_| {
            admit(
                &mut host,
                Moment::from_tick(0),
                Deadline::from_tick(10),
                record("orders"),
            )
        })
        .collect();

    assert!(matches!(
        host.try_admit_flush(Moment::from_tick(1)),
        Err(FlushAdmissionFailure::Rejected(
            FlushRejectionReason::Completion(crate::completion::CompletionRegistryError::Full)
        ))
    ));
    assert_eq!(host.core.flush_slots(), 0);
    drop(records);
}

#[test]
fn retained_flushes_block_record_admission_until_exact_reclamation() {
    let limits = valid_limits();
    let mut host = start(limits);
    let flushes: Vec<_> = (0..limits.completion_capacity)
        .map(|_| {
            host.try_admit_flush(Moment::from_tick(0))
                .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"))
        })
        .collect();

    let rejected = host.try_admit_explicit(
        Moment::from_tick(1),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(10), std::time::Instant::now()),
        record("orders"),
    );
    assert!(matches!(
        rejected,
        Err(ProducerAdmissionFailure::Rejected(value))
            if value.reason()
                == ProducerRejectionReason::Completion(CompletionRegistryError::Full)
    ));
    assert_eq!(host.stats().core_completion_slots, 0);

    let mut flushes = flushes.into_iter();
    let first = flushes
        .next()
        .unwrap_or_else(|| panic!("test should retain a first flush"));
    let first_id = first.flush_id();
    assert_eq!(first.into_flush_observer().wait(), Ok(()));
    assert!(matches!(
        host.reclaim_one(Moment::from_tick(1)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Flush(id),
            ..
        })) if id == first_id
    ));

    let record = admit(
        &mut host,
        Moment::from_tick(2),
        Deadline::from_tick(10),
        record("orders"),
    );
    assert_eq!(host.stats().core_completion_slots, 1);
    drop((record, flushes));
}
