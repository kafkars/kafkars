//! Legal progression, exact effect retention, and illegal transition scenarios.

use super::test_support::{InputKind, admitted, deadline, effect, group_fence, input};
use super::{
    TransactionOffsetCommitConsequence, TransactionOffsetCommitEffect, TransactionOffsetCommitId,
    TransactionOffsetCommitInput, TransactionOffsetCommitMachine,
    TransactionOffsetCommitMachineError, TransactionOffsetCommitStage,
    TransactionOffsetCommitState, TransactionOffsetCommitTerminal,
};

#[test]
fn legal_two_step_progression_retains_exact_deadline_and_fences() {
    let (mut machine, epoch, operation_id) = admitted();
    assert_eq!(operation_id.get(), 1);
    assert_eq!(machine.deadline(), Some(deadline()));
    assert_eq!(machine.group_fence(), Some(group_fence()));

    let Ok(accepted) = machine.apply(input(
        InputKind::Accepted,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets acceptance must advance");
    };
    assert_eq!(accepted.into_effect(), None);
    let Ok(second_submit) = machine.apply(input(
        InputKind::Succeeded,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets success must submit TxnOffsetCommit");
    };
    assert_eq!(
        effect(second_submit),
        TransactionOffsetCommitEffect::SubmitTxnOffsetCommit {
            epoch,
            operation_id,
            deadline: deadline(),
            group_fence: group_fence(),
        }
    );
    let Ok(accepted) = machine.apply(input(
        InputKind::Accepted,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::TxnOffsetCommit,
    )) else {
        panic!("TxnOffsetCommit acceptance must advance");
    };
    assert_eq!(accepted.into_effect(), None);
    let Ok(completed) = machine.apply(input(
        InputKind::Succeeded,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::TxnOffsetCommit,
    )) else {
        panic!("TxnOffsetCommit success must complete");
    };
    assert_eq!(
        effect(completed),
        TransactionOffsetCommitEffect::Complete {
            epoch,
            operation_id,
            deadline: deadline(),
            group_fence: group_fence(),
            terminal: TransactionOffsetCommitTerminal::Succeeded,
        }
    );
}

#[test]
fn every_same_stage_terminal_requires_its_legal_admitted_or_awaiting_state() {
    let (mut machine, epoch, operation_id) = admitted();
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::Succeeded {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::AddOffsets,
        },
    );
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::AcceptedFailed {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::AddOffsets,
            consequence: TransactionOffsetCommitConsequence::AbortRequired,
        },
    );
    let Ok(_transition) = machine.apply(input(
        InputKind::Accepted,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets acceptance must advance");
    };
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::DriverAccepted {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::AddOffsets,
        },
    );
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::DriverRejected {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::AddOffsets,
        },
    );
    let Ok(_transition) = machine.apply(input(
        InputKind::Succeeded,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets success must advance");
    };
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::Succeeded {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::TxnOffsetCommit,
        },
    );
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::AcceptedFailed {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::TxnOffsetCommit,
            consequence: TransactionOffsetCommitConsequence::Fatal,
        },
    );
    let Ok(_transition) = machine.apply(input(
        InputKind::Accepted,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::TxnOffsetCommit,
    )) else {
        panic!("TxnOffsetCommit acceptance must advance");
    };
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::DriverAccepted {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::TxnOffsetCommit,
        },
    );
    assert_invalid_without_mutation(
        &mut machine,
        TransactionOffsetCommitInput::DriverRejected {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::TxnOffsetCommit,
        },
    );
}

#[test]
fn every_wrong_stage_fact_is_rejected_without_mutation() {
    let (mut machine, epoch, operation_id) = admitted();
    for input in facts(
        epoch,
        operation_id,
        TransactionOffsetCommitStage::TxnOffsetCommit,
    ) {
        let before = snapshot(&machine);
        assert_eq!(
            machine.apply(input),
            Err(TransactionOffsetCommitMachineError::StageMismatch {
                expected: TransactionOffsetCommitStage::AddOffsets,
                supplied: TransactionOffsetCommitStage::TxnOffsetCommit,
            })
        );
        assert_eq!(snapshot(&machine), before);
    }
}

#[test]
fn terminal_fact_after_completion_has_no_owner_to_mutate() {
    let (mut machine, epoch, operation_id) = admitted();
    let Ok(_transition) = machine.apply(input(
        InputKind::Rejected,
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("AddOffsets rejection must complete");
    };
    let before = snapshot(&machine);
    for input in facts(
        epoch,
        operation_id,
        TransactionOffsetCommitStage::AddOffsets,
    ) {
        assert_eq!(
            machine.apply(input),
            Err(TransactionOffsetCommitMachineError::NoOperation)
        );
        assert_eq!(snapshot(&machine), before);
    }
}

fn facts(
    epoch: crate::TransactionEpoch,
    operation_id: TransactionOffsetCommitId,
    stage: TransactionOffsetCommitStage,
) -> [TransactionOffsetCommitInput; 5] {
    [
        input(InputKind::Accepted, epoch, operation_id, stage),
        input(InputKind::Rejected, epoch, operation_id, stage),
        input(InputKind::Succeeded, epoch, operation_id, stage),
        TransactionOffsetCommitInput::RetryableFailed {
            epoch,
            operation_id,
            stage,
        },
        TransactionOffsetCommitInput::AcceptedFailed {
            epoch,
            operation_id,
            stage,
            consequence: TransactionOffsetCommitConsequence::AbortRequired,
        },
    ]
}

fn assert_invalid_without_mutation(
    machine: &mut TransactionOffsetCommitMachine,
    input: TransactionOffsetCommitInput,
) {
    let before = snapshot(machine);
    assert_eq!(
        machine.apply(input),
        Err(TransactionOffsetCommitMachineError::InvalidState { state: before.0 })
    );
    assert_eq!(snapshot(machine), before);
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
