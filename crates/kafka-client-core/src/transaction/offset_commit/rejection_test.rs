//! Definitely-unsent rollback and retry identity scenarios.

use super::test_support::{InputKind, admitted, deadline, effect, group_fence, input};
use super::{
    TransactionOffsetCommitEffect, TransactionOffsetCommitMachineError,
    TransactionOffsetCommitStage, TransactionOffsetCommitState, TransactionOffsetCommitTerminal,
};

#[test]
fn add_offsets_rejection_rolls_back_healthy_and_retry_gets_new_identity() {
    let (mut machine, epoch, first) = admitted();
    let Ok(rejected) = machine.apply(input(
        InputKind::Rejected,
        epoch,
        first,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets rejection must complete");
    };
    assert_eq!(
        effect(rejected),
        complete(epoch, first, TransactionOffsetCommitStage::AddOffsets)
    );
    assert_eq!(machine.state(), TransactionOffsetCommitState::Idle);

    if let Err(error) = machine.admit(epoch, deadline(), group_fence()) {
        panic!("retry admission: {error}");
    }
    let Some(second) = machine.operation_id() else {
        panic!("retry admission must retain its operation");
    };
    assert_eq!(second.get(), first.get() + 1);
    let before = machine.state();
    assert_eq!(
        machine.apply(input(
            InputKind::Accepted,
            epoch,
            first,
            TransactionOffsetCommitStage::AddOffsets,
        )),
        Err(TransactionOffsetCommitMachineError::OperationMismatch {
            expected: second,
            supplied: first,
        })
    );
    assert_eq!(machine.state(), before);
}

#[test]
fn txn_offset_commit_rejection_rolls_back_after_add_offsets_success() {
    let (mut machine, epoch, operation_id) = admitted();
    let Ok(_transition) = machine.apply(input(
        InputKind::Accepted,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets acceptance must advance");
    };
    let Ok(_transition) = machine.apply(input(
        InputKind::Succeeded,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets success must advance");
    };
    assert_eq!(
        machine.state(),
        TransactionOffsetCommitState::TxnOffsetCommitAdmitted
    );

    let Ok(rejected) = machine.apply(input(
        InputKind::Rejected,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::TxnOffsetCommit,
    )) else {
        panic!("TxnOffsetCommit rejection must complete");
    };
    assert_eq!(
        effect(rejected),
        complete(
            epoch,
            operation_id,
            TransactionOffsetCommitStage::TxnOffsetCommit,
        )
    );
    assert_eq!(machine.state(), TransactionOffsetCommitState::Idle);
}

fn complete(
    epoch: crate::TransactionEpoch,
    operation_id: super::TransactionOffsetCommitId,
    stage: TransactionOffsetCommitStage,
) -> TransactionOffsetCommitEffect {
    TransactionOffsetCommitEffect::Complete {
        epoch,
        operation_id,
        deadline: deadline(),
        group_fence: group_fence(),
        terminal: TransactionOffsetCommitTerminal::RejectedNotSent { stage },
    }
}
