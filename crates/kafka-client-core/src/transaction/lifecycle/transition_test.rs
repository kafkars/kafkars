//! Begin, commit, epoch, and owner-fence lifecycle scenarios.

use super::{
    TransactionEndMode, TransactionEndObservation, TransactionLifecycleEffect,
    TransactionLifecycleInput, TransactionLifecycleMachine, TransactionLifecycleMachineError,
    TransactionLifecycleState,
};
use crate::{OperationId, TransactionalOwnerId};

#[test]
fn begin_assigns_one_nonreused_epoch_and_commit_retains_exact_operation() {
    let owner = TransactionalOwnerId::from_raw(9);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let began = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error}"));
    let Some(TransactionLifecycleEffect::Began { epoch, .. }) = began.into_effect() else {
        panic!("begin effect");
    };
    let operation_id = OperationId::from_raw(17);
    let end = machine
        .apply(
            owner,
            TransactionLifecycleInput::Commit {
                epoch,
                operation_id,
            },
        )
        .unwrap_or_else(|error| panic!("commit: {error}"));
    assert_eq!(machine.state(), TransactionLifecycleState::EndingCommit);
    assert!(matches!(
        end.into_effect(),
        Some(TransactionLifecycleEffect::EndTransaction {
            epoch: actual_epoch,
            mode: TransactionEndMode::Commit,
            observation: TransactionEndObservation::Observed,
            operation_id: Some(actual_operation),
            ..
        }) if actual_epoch == epoch && actual_operation == operation_id
    ));
}

#[test]
fn stale_owner_and_epoch_are_rejected_without_mutation() {
    let owner = TransactionalOwnerId::from_raw(2);
    let foreign = TransactionalOwnerId::from_raw(3);
    let mut machine = TransactionLifecycleMachine::new(owner);
    assert!(matches!(
        machine.apply(foreign, TransactionLifecycleInput::Begin),
        Err(TransactionLifecycleMachineError::OwnerMismatch { .. })
    ));
    assert_eq!(machine.state(), TransactionLifecycleState::Idle);
}
