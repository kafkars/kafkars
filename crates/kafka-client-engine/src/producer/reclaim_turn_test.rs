//! Host-level observed, abandoned, retry, and mismatch reclamation scenarios.

use std::{
    thread,
    time::{Duration, Instant},
};

use kafka_client_core::{Deadline, Moment};

use crate::{
    ProducerDeliveryError,
    clock::OperationDeadline,
    completion::{CompletionRegistryError, test_support::hold_cell_lock},
    producer::admission::ProducerAdmissionFailure,
};

use super::{
    ProducerHostInvariantError, ProducerRejectionReason,
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
    reclaim::{CompletionReclaimError, CompletionReclaimOutcome},
    terminal_backlog::ProducerTerminalOwner,
};

#[test]
fn observed_flush_reclaims_core_and_shared_registry_capacity_once() {
    let mut host = start(valid_limits());
    let flush = host
        .try_admit_flush(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("empty flush should be accepted: {error:?}"));
    let flush_id = flush.flush_id();
    let Some(completion_id) = host.flush_bindings.completion(flush_id) else {
        panic!("accepted flush should own a completion")
    };
    assert_eq!(flush.into_flush_observer().wait(), Ok(()));

    assert_eq!(
        host.reclaim_one(Moment::from_tick(0)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Flush(flush_id),
            completion_id,
        }))
    );
    assert_eq!(host.stats().core_flush_slots, 0);
    assert_eq!(host.flush_bindings.completion(flush_id), None);
}

#[test]
fn observed_completion_reuses_only_a_new_completion_generation() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();
    let Some(completion_id) = host.bindings.completion(operation_id) else {
        panic!("accepted operation should own a completion")
    };
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    assert!(matches!(
        admitted.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));

    assert_eq!(
        host.reclaim_one(Moment::from_tick(5)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Record(operation_id),
            completion_id,
        }))
    );
    assert_eq!(host.stats().core_completion_slots, 0);
    assert_eq!(host.bindings.completion(operation_id), None);

    let replacement = admit(
        &mut host,
        Moment::from_tick(6),
        Deadline::from_tick(11),
        record("orders"),
    );
    let Some(replacement_id) = host.bindings.completion(replacement.operation_id()) else {
        panic!("replacement operation should own a completion")
    };
    assert_ne!(replacement_id, completion_id);
    drop(replacement);
}

#[test]
fn abandoned_observation_reclaims_after_terminal_publication() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();
    let Some(completion_id) = host.bindings.completion(operation_id) else {
        panic!("accepted operation should own a completion")
    };
    drop(admitted);
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    let Ok(join) = host.completions.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join_off_notifier(), Ok(()));

    assert_eq!(
        host.reclaim_one(Moment::from_tick(5)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Record(operation_id),
            completion_id,
        }))
    );
    assert_eq!(host.reclaim_one(Moment::from_tick(5)), Ok(None));
    assert_eq!(host.stats().core_completion_slots, 0);
}

#[test]
fn abandoned_flush_reclaims_exact_core_slot_binding_and_capacity() {
    let mut host = start(valid_limits());
    let flush = host
        .try_admit_flush(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("empty flush should be accepted: {error:?}"));
    let flush_id = flush.flush_id();
    let Some(completion_id) = host.flush_bindings.completion(flush_id) else {
        panic!("accepted flush should own a completion")
    };
    drop(flush);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match host.reclaim_one(Moment::from_tick(0)) {
            Ok(Some(outcome)) => {
                assert_eq!(
                    outcome,
                    CompletionReclaimOutcome::Reclaimed {
                        owner: ProducerTerminalOwner::Flush(flush_id),
                        completion_id,
                    }
                );
                break;
            }
            Ok(None) => {
                assert!(
                    Instant::now() < deadline,
                    "abandoned flush should become reclaimable"
                );
                thread::yield_now();
            }
            Err(error) => panic!("abandoned flush reclaim should succeed: {error}"),
        }
    }
    assert_eq!(host.reclaim_one(Moment::from_tick(0)), Ok(None));
    assert_eq!(host.stats().core_flush_slots, 0);
    assert_eq!(host.flush_bindings.completion(flush_id), None);
    let Ok((replacement, observer)) = host.completions.reserve() else {
        panic!("abandoned flush capacity should be reusable")
    };
    assert_eq!(host.completions.rollback_reservation(replacement), Ok(()));
    drop(observer);
}

#[test]
fn locked_cell_retry_does_not_repeat_the_core_reclaim_input() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();
    let Some(completion_id) = host.bindings.completion(operation_id) else {
        panic!("accepted operation should own a completion")
    };
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    assert!(matches!(
        admitted.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));
    let Some((release, lock)) = hold_cell_lock(&host.completions, completion_id) else {
        panic!("completion cell lock should be held")
    };

    assert_eq!(
        host.reclaim_one(Moment::from_tick(5)),
        Ok(Some(CompletionReclaimOutcome::Retry))
    );
    assert_eq!(host.stats().core_completion_slots, 0);
    assert_eq!(host.bindings.completion(operation_id), Some(completion_id));
    let limits = valid_limits();
    let spare_capacity = limits
        .completion_capacity
        .saturating_add(limits.waiting_record_capacity)
        .saturating_sub(1);
    let spares: Vec<_> = (0..spare_capacity)
        .map(|_| {
            host.completions
                .reserve()
                .unwrap_or_else(|error| panic!("unrelated spare completion should fit: {error}"))
        })
        .collect();
    assert!(matches!(
        host.completions.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    for (spare_id, spare_observer) in spares {
        assert_eq!(host.completions.rollback_reservation(spare_id), Ok(()));
        drop(spare_observer);
    }
    assert!(release.send(()).is_ok());
    assert!(lock.join().is_ok());

    assert_eq!(
        host.reclaim_one(Moment::from_tick(6)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            owner: ProducerTerminalOwner::Record(operation_id),
            completion_id,
        }))
    );
    assert_eq!(host.reclaim_one(Moment::from_tick(6)), Ok(None));
}

#[test]
fn missing_exact_binding_poisons_host_closed() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();
    let Some(completion_id) = host.bindings.completion(operation_id) else {
        panic!("accepted operation should own a completion")
    };
    assert_eq!(host.fire_due(Moment::from_tick(5), 1), Ok(1));
    assert!(matches!(
        admitted.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));
    assert_eq!(host.bindings.remove(operation_id), Ok(completion_id));

    let invariant =
        ProducerHostInvariantError::Reclaim(CompletionReclaimError::UnknownBinding(completion_id));
    assert_eq!(host.reclaim_one(Moment::from_tick(5)), Err(invariant));
    assert!(!host.stats().healthy);
    let rejected = host.try_admit_explicit(
        Moment::from_tick(6),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(11), std::time::Instant::now()),
        record("payments"),
    );
    assert!(matches!(
        rejected,
        Err(ProducerAdmissionFailure::Rejected(value))
            if value.reason() == ProducerRejectionReason::HostPoisoned(invariant)
    ));
}
