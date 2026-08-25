//! Accepted-send ownership, abort-required, drain, and fatal scenarios.

use super::send_retry_test_support::{prepare, send_identity};
use super::test_support::{accept, assert_end, begin, effect, operation, owner, send, settle};
use super::{
    TransactionEndMode, TransactionEndObservation, TransactionLifecycleEffect,
    TransactionLifecycleInput, TransactionLifecycleMachine, TransactionLifecycleMachineError,
    TransactionLifecycleState, TransactionSendOutcome,
};

#[test]
fn abort_required_rejects_new_work_and_drains_before_abort_submission() {
    let owner_id = owner(4);
    let mut machine = TransactionLifecycleMachine::new(owner_id);
    let epoch = begin(&mut machine, owner_id);
    accept(&mut machine, owner_id, epoch, send(1));
    accept(&mut machine, owner_id, epoch, send(2));

    assert_eq!(
        effect(
            &mut machine,
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id: send(1),
                outcome: TransactionSendOutcome::AbortRequired,
            },
        ),
        TransactionLifecycleEffect::AbortRequired { owner_id, epoch }
    );
    assert_eq!(machine.state(), TransactionLifecycleState::AbortRequired);
    assert_eq!(
        machine.apply(
            owner_id,
            TransactionLifecycleInput::SendAccepted {
                epoch,
                send_id: send(3),
            },
        ),
        Err(TransactionLifecycleMachineError::AbortRequired)
    );
    assert_eq!(
        machine.apply(
            owner_id,
            TransactionLifecycleInput::Commit {
                epoch,
                operation_id: operation(3),
            },
        ),
        Err(TransactionLifecycleMachineError::AbortRequired)
    );

    let abort_operation = operation(4);
    assert_eq!(
        effect(
            &mut machine,
            owner_id,
            TransactionLifecycleInput::Abort {
                epoch,
                operation_id: abort_operation,
            },
        ),
        TransactionLifecycleEffect::CancelOutstanding {
            owner_id,
            epoch,
            outstanding_sends: 1,
            observation: TransactionEndObservation::Observed,
        }
    );
    assert_end(
        effect(
            &mut machine,
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id: send(2),
                outcome: TransactionSendOutcome::AbortRequired,
            },
        ),
        epoch,
        TransactionEndMode::Abort,
        TransactionEndObservation::Observed,
        Some(abort_operation),
    );
    assert_eq!(machine.state(), TransactionLifecycleState::EndingAbort);
}

#[test]
fn duplicate_and_unknown_send_terminals_preserve_outstanding_ownership() {
    let owner_id = owner(5);
    let mut machine = TransactionLifecycleMachine::new(owner_id);
    let epoch = begin(&mut machine, owner_id);
    accept(&mut machine, owner_id, epoch, send(1));
    prepare(&mut machine, owner_id, epoch, send(1), send_identity(100));

    assert_eq!(
        machine.apply(
            owner_id,
            TransactionLifecycleInput::SendAccepted {
                epoch,
                send_id: send(1),
            },
        ),
        Err(TransactionLifecycleMachineError::DuplicateSend { send_id: send(1) })
    );
    assert_eq!(
        machine.apply(
            owner_id,
            TransactionLifecycleInput::SendPrepared {
                epoch,
                send_id: send(1),
                identity: send_identity(100),
            },
        ),
        Err(TransactionLifecycleMachineError::DuplicateSendPreparation { send_id: send(1) })
    );
    assert_eq!(
        machine.apply(
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id: send(2),
                outcome: TransactionSendOutcome::Succeeded,
            },
        ),
        Err(TransactionLifecycleMachineError::UnknownSend { send_id: send(2) })
    );
    assert_eq!(machine.outstanding_send_count(), 1);
}

#[test]
fn commit_preflight_is_quiescent_and_does_not_mutate_outstanding_send_ownership() {
    let owner_id = owner(6);
    let mut machine = TransactionLifecycleMachine::new(owner_id);
    let epoch = begin(&mut machine, owner_id);
    accept(&mut machine, owner_id, epoch, send(1));

    assert_eq!(
        machine.preflight_commit(epoch),
        Err(TransactionLifecycleMachineError::OutstandingSends { count: 1 })
    );
    assert_eq!(machine.state(), TransactionLifecycleState::Active);
    assert_eq!(machine.outstanding_send_count(), 1);

    settle(
        &mut machine,
        owner_id,
        epoch,
        send(1),
        TransactionSendOutcome::FailedHealthy,
    );
    assert_eq!(machine.preflight_commit(epoch), Ok(()));
}

#[test]
fn fatal_send_fences_while_later_send_terminals_can_still_drain() {
    let owner_id = owner(9);
    let mut machine = TransactionLifecycleMachine::new(owner_id);
    let epoch = begin(&mut machine, owner_id);
    accept(&mut machine, owner_id, epoch, send(1));
    accept(&mut machine, owner_id, epoch, send(2));

    assert_eq!(
        effect(
            &mut machine,
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id: send(1),
                outcome: TransactionSendOutcome::Fatal,
            },
        ),
        TransactionLifecycleEffect::EnterFatal {
            owner_id,
            epoch,
            operation_id: None,
            terminal: None,
            owner_lost: false,
        }
    );
    assert_eq!(machine.outstanding_send_count(), 1);
    settle(
        &mut machine,
        owner_id,
        epoch,
        send(2),
        TransactionSendOutcome::AbortRequired,
    );
    assert_eq!(machine.outstanding_send_count(), 0);
    assert_eq!(machine.state(), TransactionLifecycleState::Fatal);
}

#[test]
fn definitely_unsent_failure_releases_the_send_without_poisoning_commit() {
    let owner_id = owner(12);
    let mut machine = TransactionLifecycleMachine::new(owner_id);
    let epoch = begin(&mut machine, owner_id);
    accept(&mut machine, owner_id, epoch, send(1));

    settle(
        &mut machine,
        owner_id,
        epoch,
        send(1),
        TransactionSendOutcome::FailedHealthy,
    );

    assert_eq!(machine.state(), TransactionLifecycleState::Active);
    assert_eq!(machine.outstanding_send_count(), 0);
    assert!(matches!(
        effect(
            &mut machine,
            owner_id,
            TransactionLifecycleInput::Commit {
                epoch,
                operation_id: crate::OperationId::from_raw(7),
            },
        ),
        TransactionLifecycleEffect::EndTransaction { .. }
    ));
}
