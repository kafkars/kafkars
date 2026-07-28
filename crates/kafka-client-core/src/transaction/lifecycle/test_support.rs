//! Shared deterministic constructors and transition assertions for lifecycle tests.

use crate::{OperationId, TransactionalOwnerId};

use super::{
    TransactionEndMode, TransactionEndObservation, TransactionEpoch, TransactionLifecycleEffect,
    TransactionLifecycleInput, TransactionLifecycleMachine, TransactionSendId,
    TransactionSendOutcome,
};

pub(super) fn owner(value: u64) -> TransactionalOwnerId {
    TransactionalOwnerId::from_raw(value)
}

pub(super) fn operation(value: u64) -> OperationId {
    OperationId::from_raw(value)
}

pub(super) fn send(value: u64) -> TransactionSendId {
    TransactionSendId::from_raw(value)
}

pub(super) fn begin(
    machine: &mut TransactionLifecycleMachine,
    owner_id: TransactionalOwnerId,
) -> TransactionEpoch {
    match effect(machine, owner_id, TransactionLifecycleInput::Begin) {
        TransactionLifecycleEffect::Began {
            owner_id: actual_owner,
            epoch,
        } => {
            assert_eq!(actual_owner, owner_id);
            epoch
        }
        other => panic!("expected begin effect, got {other:?}"),
    }
}

pub(super) fn accept(
    machine: &mut TransactionLifecycleMachine,
    owner_id: TransactionalOwnerId,
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
) {
    let transition = machine
        .apply(
            owner_id,
            TransactionLifecycleInput::SendAccepted { epoch, send_id },
        )
        .unwrap_or_else(|error| panic!("send acceptance: {error}"));
    assert_eq!(transition.into_effect(), None);
}

pub(super) fn settle(
    machine: &mut TransactionLifecycleMachine,
    owner_id: TransactionalOwnerId,
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    outcome: TransactionSendOutcome,
) {
    let transition = machine
        .apply(
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id,
                outcome,
            },
        )
        .unwrap_or_else(|error| panic!("send settlement: {error}"));
    assert_eq!(transition.into_effect(), None);
}

pub(super) fn effect(
    machine: &mut TransactionLifecycleMachine,
    owner_id: TransactionalOwnerId,
    input: TransactionLifecycleInput,
) -> TransactionLifecycleEffect {
    machine
        .apply(owner_id, input)
        .unwrap_or_else(|error| panic!("lifecycle transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("transition should emit one effect"))
}

pub(super) fn assert_end(
    effect: TransactionLifecycleEffect,
    epoch: TransactionEpoch,
    mode: TransactionEndMode,
    observation: TransactionEndObservation,
    operation_id: Option<OperationId>,
) {
    assert!(matches!(
        effect,
        TransactionLifecycleEffect::EndTransaction {
            epoch: actual_epoch,
            mode: actual_mode,
            observation: actual_observation,
            operation_id: actual_operation,
            ..
        } if actual_epoch == epoch
            && actual_mode == mode
            && actual_observation == observation
            && actual_operation == operation_id
    ));
}
