//! Deterministic scenarios for producer-lifetime transactional sequences.

use std::fmt::Debug;

use crate::{
    OperationId, PartitionIndex, TopicId, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionSendOutcome, TransactionSequenceLease, TransactionalOwnerId,
};

use super::{
    TransactionPartition, TransactionSequenceMachine, TransactionSequenceMachineError,
    TransactionSequenceSettlement, TransactionSequenceState,
};

fn must_succeed<T, E: Debug>(result: Result<T, E>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error:?}"),
    }
}

fn epochs() -> (TransactionEpoch, TransactionEpoch) {
    let owner = TransactionalOwnerId::from_raw(1);
    let operation = OperationId::from_raw(1);
    let mut lifecycle = TransactionLifecycleMachine::new(owner);
    let first = began_epoch(must_succeed(
        lifecycle.apply(owner, TransactionLifecycleInput::Begin),
        "begin first transaction epoch",
    ));
    must_succeed(
        lifecycle.apply(
            owner,
            TransactionLifecycleInput::Commit {
                epoch: first,
                operation_id: operation,
            },
        ),
        "start first transaction commit",
    );
    must_succeed(
        lifecycle.apply(
            owner,
            TransactionLifecycleInput::EndSettled {
                epoch: first,
                outcome: TransactionEndOutcome::Succeeded,
            },
        ),
        "settle first transaction commit",
    );
    let second = began_epoch(must_succeed(
        lifecycle.apply(owner, TransactionLifecycleInput::Begin),
        "begin second transaction epoch",
    ));
    (first, second)
}

fn began_epoch(transition: crate::TransactionLifecycleTransition) -> TransactionEpoch {
    let Some(TransactionLifecycleEffect::Began { epoch, .. }) = transition.into_effect() else {
        panic!("begin emits one epoch")
    };
    epoch
}

fn route(topic: u64, partition: u32) -> TransactionPartition {
    TransactionPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

#[test]
fn success_advances_across_transaction_epochs_without_reset() {
    let (first, second) = epochs();
    let partition = route(7, 2);
    let mut machine = must_succeed(
        TransactionSequenceMachine::try_new(2),
        "create two-partition sequence machine",
    );

    must_succeed(machine.activate(first), "activate first transaction epoch");
    let lease = must_succeed(
        machine.try_lease(first, partition, 3),
        "lease first sequence range",
    );
    let Some(expected_first) = TransactionSequenceLease::try_new(0, 3) else {
        panic!("expected first sequence lease must be valid");
    };
    assert_eq!(lease, expected_first);
    assert_eq!(
        must_succeed(
            machine.settle_accepted(
                first,
                partition,
                lease,
                TransactionSequenceSettlement::Succeeded,
            ),
            "settle first sequence lease",
        ),
        TransactionSendOutcome::Succeeded,
    );
    must_succeed(machine.release(first), "release first transaction epoch");

    must_succeed(
        machine.activate(second),
        "activate second transaction epoch",
    );
    let Some(expected_second) = TransactionSequenceLease::try_new(3, 2) else {
        panic!("expected advanced sequence lease must be valid");
    };
    assert_eq!(
        must_succeed(
            machine.try_lease(second, partition, 2),
            "lease sequence after successful prior epoch",
        ),
        expected_second,
    );
}

#[test]
fn definitely_unsent_release_reuses_exact_sequence() {
    let active = epochs().0;
    let partition = route(1, 0);
    let mut machine = must_succeed(
        TransactionSequenceMachine::try_new(1),
        "create one-partition sequence machine",
    );
    must_succeed(machine.activate(active), "activate transaction epoch");

    let lease = must_succeed(
        machine.try_lease(active, partition, 1),
        "lease definitely-unsent sequence",
    );
    must_succeed(
        machine.release_not_sent(active, partition, lease),
        "release definitely-unsent sequence",
    );

    assert_eq!(
        must_succeed(
            machine.try_lease(active, partition, 1),
            "re-lease definitely-unsent sequence",
        ),
        lease,
    );
}

#[test]
fn concurrent_new_partitions_cannot_escape_the_fixed_capacity() {
    let active = epochs().0;
    let first = route(1, 0);
    let second = route(1, 1);
    let mut machine = must_succeed(
        TransactionSequenceMachine::try_new(1),
        "create capacity-one sequence machine",
    );
    must_succeed(machine.activate(active), "activate transaction epoch");

    let first_lease = must_succeed(
        machine.try_lease(active, first, 1),
        "lease the only partition slot",
    );
    assert_eq!(
        machine.try_lease(active, second, 1),
        Err(TransactionSequenceMachineError::PartitionCapacity),
    );

    must_succeed(
        machine.release_not_sent(active, first, first_lease),
        "release the only partition slot",
    );
    assert!(machine.try_lease(active, second, 1).is_ok());
}

#[test]
fn not_appended_requires_abort_but_preserves_next_sequence() {
    let (first, second) = epochs();
    let partition = route(2, 4);
    let mut machine = must_succeed(
        TransactionSequenceMachine::try_new(1),
        "create one-partition sequence machine",
    );
    must_succeed(machine.activate(first), "activate first transaction epoch");
    let lease = must_succeed(
        machine.try_lease(first, partition, 2),
        "lease sequence before not-appended settlement",
    );

    assert_eq!(
        must_succeed(
            machine.settle_accepted(
                first,
                partition,
                lease,
                TransactionSequenceSettlement::NotAppended,
            ),
            "settle not-appended sequence lease",
        ),
        TransactionSendOutcome::AbortRequired,
    );
    must_succeed(machine.release(first), "release first transaction epoch");
    must_succeed(
        machine.activate(second),
        "activate second transaction epoch",
    );
    assert_eq!(
        must_succeed(
            machine.try_lease(second, partition, 2),
            "lease sequence after not-appended settlement",
        ),
        lease,
    );
}

#[test]
fn uncertain_terminal_fences_admission_and_late_exact_drain_stays_fatal() {
    let active = epochs().0;
    let first = route(3, 0);
    let second = route(3, 1);
    let mut machine = must_succeed(
        TransactionSequenceMachine::try_new(2),
        "create two-partition sequence machine",
    );
    must_succeed(machine.activate(active), "activate transaction epoch");
    let first_lease = must_succeed(
        machine.try_lease(active, first, 1),
        "lease first partition sequence",
    );
    let second_lease = must_succeed(
        machine.try_lease(active, second, 1),
        "lease second partition sequence",
    );

    assert_eq!(
        must_succeed(
            machine.settle_accepted(
                active,
                first,
                first_lease,
                TransactionSequenceSettlement::Uncertain,
            ),
            "settle uncertain first partition sequence",
        ),
        TransactionSendOutcome::Fatal,
    );
    assert_eq!(machine.state(), TransactionSequenceState::Fenced);
    assert_eq!(
        machine.try_lease(active, route(3, 2), 1),
        Err(TransactionSequenceMachineError::Fenced),
    );
    assert_eq!(
        must_succeed(
            machine.settle_accepted(
                active,
                second,
                second_lease,
                TransactionSequenceSettlement::Succeeded,
            ),
            "drain exact second partition sequence after fencing",
        ),
        TransactionSendOutcome::Fatal,
    );
    assert_eq!(machine.outstanding_lease_count(), 0);
}

#[test]
fn wrong_epoch_and_wrong_lease_are_nonmutating() {
    let (active, stale) = epochs();
    let partition = route(9, 1);
    let mut machine = must_succeed(
        TransactionSequenceMachine::try_new(1),
        "create one-partition sequence machine",
    );
    must_succeed(machine.activate(active), "activate transaction epoch");
    let lease = must_succeed(
        machine.try_lease(active, partition, 1),
        "lease active transaction sequence",
    );

    assert_eq!(
        machine.release_not_sent(stale, partition, lease),
        Err(TransactionSequenceMachineError::EpochMismatch),
    );
    let Some(wrong_lease) = TransactionSequenceLease::try_new(1, 1) else {
        panic!("mismatched sequence lease fixture must be valid");
    };
    assert_eq!(
        machine.release_not_sent(active, partition, wrong_lease),
        Err(TransactionSequenceMachineError::LeaseMismatch),
    );
    assert_eq!(machine.outstanding_lease_count(), 1);
}
