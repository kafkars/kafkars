//! Explicit and best-effort transaction-end settlement scenarios.

use super::{
    TransactionEndMode, TransactionEndObservation, TransactionEndOutcome,
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionLifecycleState, TransactionLifecycleTerminal,
};
use crate::{OperationId, TransactionalOwnerId};

#[test]
fn explicit_commit_success_returns_idle_and_publishes_one_terminal() {
    let owner = TransactionalOwnerId::from_raw(4);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let epoch = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error}"))
        .into_effect()
        .and_then(|effect| match effect {
            TransactionLifecycleEffect::Began { epoch, .. } => Some(epoch),
            _ => None,
        })
        .unwrap_or_else(|| panic!("epoch"));
    let operation_id = OperationId::from_raw(1);
    machine
        .apply(
            owner,
            TransactionLifecycleInput::Commit {
                epoch,
                operation_id,
            },
        )
        .unwrap_or_else(|error| panic!("commit: {error}"));
    let settled = machine
        .apply(
            owner,
            TransactionLifecycleInput::EndSettled {
                epoch,
                outcome: TransactionEndOutcome::Succeeded,
            },
        )
        .unwrap_or_else(|error| panic!("settle: {error}"));
    assert_eq!(machine.state(), TransactionLifecycleState::Idle);
    assert!(matches!(
        settled.into_effect(),
        Some(TransactionLifecycleEffect::Complete {
            terminal: TransactionLifecycleTerminal::Committed,
            operation_id: actual,
            ..
        }) if actual == operation_id
    ));
}

#[test]
fn active_owner_loss_emits_unobserved_abort_and_never_success() {
    let owner = TransactionalOwnerId::from_raw(5);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let epoch = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error}"))
        .into_effect()
        .and_then(|effect| match effect {
            TransactionLifecycleEffect::Began { epoch, .. } => Some(epoch),
            _ => None,
        })
        .unwrap_or_else(|| panic!("epoch"));
    let lost = machine
        .apply(owner, TransactionLifecycleInput::OwnerLost)
        .unwrap_or_else(|error| panic!("owner loss: {error}"));
    assert!(matches!(
        lost.into_effect(),
        Some(TransactionLifecycleEffect::EndTransaction {
            epoch: actual,
            mode: TransactionEndMode::Abort,
            observation: TransactionEndObservation::BestEffort,
            operation_id: None,
            ..
        }) if actual == epoch
    ));
    let settled = machine
        .apply(
            owner,
            TransactionLifecycleInput::EndSettled {
                epoch,
                outcome: TransactionEndOutcome::Succeeded,
            },
        )
        .unwrap_or_else(|error| panic!("settle cleanup: {error}"));
    assert_eq!(machine.state(), TransactionLifecycleState::Closed);
    assert!(matches!(
        settled.into_effect(),
        Some(TransactionLifecycleEffect::ReleaseOwner { .. })
    ));
}

#[test]
fn fatal_end_fences_the_epoch_and_public_operation() {
    let owner = TransactionalOwnerId::from_raw(6);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let epoch = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error}"))
        .into_effect()
        .and_then(|effect| match effect {
            TransactionLifecycleEffect::Began { epoch, .. } => Some(epoch),
            _ => None,
        })
        .unwrap_or_else(|| panic!("epoch"));
    let operation_id = OperationId::from_raw(3);
    machine
        .apply(
            owner,
            TransactionLifecycleInput::Abort {
                epoch,
                operation_id,
            },
        )
        .unwrap_or_else(|error| panic!("abort: {error}"));
    let fatal = machine
        .apply(
            owner,
            TransactionLifecycleInput::EndSettled {
                epoch,
                outcome: TransactionEndOutcome::Fatal,
            },
        )
        .unwrap_or_else(|error| panic!("fatal: {error}"));
    assert_eq!(machine.state(), TransactionLifecycleState::Fatal);
    assert!(matches!(
        fatal.into_effect(),
        Some(TransactionLifecycleEffect::EnterFatal {
            operation_id: Some(actual),
            owner_lost: false,
            ..
        }) if actual == operation_id
    ));
}
