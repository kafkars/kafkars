//! Host-level observed, abandoned, retry, and mismatch reclamation scenarios.

use kafka_client_core::{Deadline, Moment};

use crate::{
    completion::{CompletionRegistryError, test_support::hold_cell_lock},
    producer::ProducerAdmissionFailure,
};

use super::{
    ProducerHostInvariantError, ProducerRejectionReason,
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
    reclaim::{CompletionReclaimError, CompletionReclaimOutcome},
};

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
    assert_eq!(host.fire_due(Moment::from_tick(5)), Ok(1));
    assert!(admitted.into_observer().wait().is_ok());

    assert_eq!(
        host.reclaim_one(Moment::from_tick(5)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            operation_id,
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
    assert_eq!(host.fire_due(Moment::from_tick(5)), Ok(1));
    let Ok(join) = host.completions.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join(), Ok(()));

    assert_eq!(
        host.reclaim_one(Moment::from_tick(5)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            operation_id,
            completion_id,
        }))
    );
    assert_eq!(host.reclaim_one(Moment::from_tick(5)), Ok(None));
    assert_eq!(host.stats().core_completion_slots, 0);
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
    assert_eq!(host.fire_due(Moment::from_tick(5)), Ok(1));
    assert!(admitted.into_observer().wait().is_ok());
    let Some((release, lock)) = hold_cell_lock(&host.completions, completion_id) else {
        panic!("completion cell lock should be held")
    };

    assert_eq!(
        host.reclaim_one(Moment::from_tick(5)),
        Ok(Some(CompletionReclaimOutcome::Retry))
    );
    assert_eq!(host.stats().core_completion_slots, 0);
    assert_eq!(host.bindings.completion(operation_id), Some(completion_id));
    let Ok((spare_id, spare_observer)) = host.completions.reserve() else {
        panic!("unrelated spare completion should remain available")
    };
    assert!(matches!(
        host.completions.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    assert_eq!(host.completions.rollback_reservation(spare_id), Ok(()));
    drop(spare_observer);
    assert!(release.send(()).is_ok());
    assert!(lock.join().is_ok());

    assert_eq!(
        host.reclaim_one(Moment::from_tick(6)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed {
            operation_id,
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
    assert_eq!(host.fire_due(Moment::from_tick(5)), Ok(1));
    assert!(admitted.into_observer().wait().is_ok());
    assert_eq!(host.bindings.remove(operation_id), Ok(completion_id));

    let invariant =
        ProducerHostInvariantError::Reclaim(CompletionReclaimError::UnknownBinding(completion_id));
    assert_eq!(host.reclaim_one(Moment::from_tick(5)), Err(invariant));
    assert!(!host.stats().healthy);
    let rejected = host.try_admit_explicit(
        Moment::from_tick(6),
        Deadline::from_tick(11),
        record("payments"),
    );
    assert!(matches!(
        rejected,
        Err(ProducerAdmissionFailure::Rejected(value))
            if value.reason() == ProducerRejectionReason::HostPoisoned(invariant)
    ));
}
