//! Stale correlation and accepted-failure consequence scenarios.

use super::test_support::{InputKind, admitted, deadline, effect, epoch, group_fence, input};
use super::{
    TransactionOffsetCommitConsequence, TransactionOffsetCommitEffect, TransactionOffsetCommitId,
    TransactionOffsetCommitInput, TransactionOffsetCommitMachine,
    TransactionOffsetCommitMachineError, TransactionOffsetCommitStage,
    TransactionOffsetCommitState, TransactionOffsetCommitTerminal,
};

#[test]
fn stale_epoch_and_operation_are_rejected_before_state_mutation() {
    let (mut machine, active, operation_id) = admitted();
    let stale_epoch = epoch(2);
    let stale_id = TransactionOffsetCommitId::from_raw_for_test(operation_id.get() + 1);
    let before = snapshot(&machine);

    assert_eq!(
        machine.apply(input(
            InputKind::Accepted,
            stale_epoch,
            operation_id,
            TransactionOffsetCommitStage::AddOffsets,
        )),
        Err(TransactionOffsetCommitMachineError::EpochMismatch {
            expected: active,
            supplied: stale_epoch,
        })
    );
    assert_eq!(snapshot(&machine), before);
    assert_eq!(
        machine.apply(input(
            InputKind::Accepted,
            active,
            stale_id,
            TransactionOffsetCommitStage::AddOffsets,
        )),
        Err(TransactionOffsetCommitMachineError::OperationMismatch {
            expected: operation_id,
            supplied: stale_id,
        })
    );
    assert_eq!(snapshot(&machine), before);
}

#[test]
fn accepted_failures_preserve_stage_and_abort_or_fatal_consequence() {
    for stage in [
        TransactionOffsetCommitStage::AddOffsets,
        TransactionOffsetCommitStage::TxnOffsetCommit,
    ] {
        for consequence in [
            TransactionOffsetCommitConsequence::AbortRequired,
            TransactionOffsetCommitConsequence::Fatal,
        ] {
            let (mut machine, active, operation_id) = awaiting(stage);
            let Ok(completed) = machine.apply(TransactionOffsetCommitInput::AcceptedFailed {
                epoch: active,
                operation_id,
                stage,
                consequence,
            }) else {
                panic!("accepted failure must complete");
            };
            assert_eq!(
                effect(completed),
                TransactionOffsetCommitEffect::Complete {
                    epoch: active,
                    operation_id,
                    deadline: deadline(),
                    group_fence: group_fence(),
                    terminal: TransactionOffsetCommitTerminal::Failed { stage, consequence },
                }
            );
            assert_eq!(machine.state(), TransactionOffsetCommitState::Idle);
        }
    }
}

fn awaiting(
    stage: TransactionOffsetCommitStage,
) -> (
    TransactionOffsetCommitMachine,
    crate::TransactionEpoch,
    TransactionOffsetCommitId,
) {
    let (mut machine, active, operation_id) = admitted();
    let Ok(_transition) = machine.apply(input(
        InputKind::Accepted,
        active,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets acceptance must advance");
    };
    if stage == TransactionOffsetCommitStage::TxnOffsetCommit {
        let Ok(_transition) = machine.apply(input(
            InputKind::Succeeded,
            active,
            operation_id,
            TransactionOffsetCommitStage::AddOffsets,
        )) else {
            panic!("AddOffsets success must advance");
        };
        let Ok(_transition) = machine.apply(input(
            InputKind::Accepted,
            active,
            operation_id,
            TransactionOffsetCommitStage::TxnOffsetCommit,
        )) else {
            panic!("TxnOffsetCommit acceptance must advance");
        };
    }
    (machine, active, operation_id)
}

fn snapshot(
    machine: &TransactionOffsetCommitMachine,
) -> (
    TransactionOffsetCommitState,
    Option<TransactionOffsetCommitId>,
    Option<crate::Deadline>,
    Option<crate::GroupPositionFence>,
) {
    (
        machine.state(),
        machine.operation_id(),
        machine.deadline(),
        machine.group_fence(),
    )
}
