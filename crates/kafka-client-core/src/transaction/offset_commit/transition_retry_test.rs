//! Exact two-stage offset-transfer replacement authorization.

use super::test_support::{InputKind, admitted, deadline, effect, group_fence, input};
use super::{
    TransactionOffsetCommitEffect, TransactionOffsetCommitInput, TransactionOffsetCommitStage,
    TransactionOffsetCommitState,
};

#[test]
fn retryable_add_offsets_reuses_exact_identity_deadline_and_fence() {
    let (mut machine, epoch, operation_id) = admitted();
    machine
        .apply(input(
            InputKind::Accepted,
            epoch,
            operation_id,
            TransactionOffsetCommitStage::AddOffsets,
        ))
        .unwrap_or_else(|error| panic!("first AddOffsets acceptance: {error}"));

    let retry = machine
        .apply(TransactionOffsetCommitInput::RetryableFailed {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::AddOffsets,
        })
        .unwrap_or_else(|error| panic!("refreshed AddOffsets replacement: {error}"));
    assert_eq!(
        effect(retry),
        TransactionOffsetCommitEffect::SubmitAddOffsets {
            epoch,
            operation_id,
            deadline: deadline(),
            group_fence: group_fence(),
        }
    );
    assert_eq!(
        machine.state(),
        TransactionOffsetCommitState::AddOffsetsAdmitted
    );
    assert_eq!(machine.operation_id(), Some(operation_id));
}

#[test]
fn retryable_txn_offset_commit_reuses_exact_identity_deadline_and_fence() {
    let (mut machine, epoch, operation_id) = admitted();
    for (kind, stage) in [
        (
            InputKind::Accepted,
            TransactionOffsetCommitStage::AddOffsets,
        ),
        (
            InputKind::Succeeded,
            TransactionOffsetCommitStage::AddOffsets,
        ),
        (
            InputKind::Accepted,
            TransactionOffsetCommitStage::TxnOffsetCommit,
        ),
    ] {
        machine
            .apply(input(kind, epoch, operation_id, stage))
            .unwrap_or_else(|error| panic!("advance to TxnOffsetCommit awaiting: {error}"));
    }

    let retry = machine
        .apply(TransactionOffsetCommitInput::RetryableFailed {
            epoch,
            operation_id,
            stage: TransactionOffsetCommitStage::TxnOffsetCommit,
        })
        .unwrap_or_else(|error| panic!("refreshed TxnOffsetCommit replacement: {error}"));
    assert_eq!(
        effect(retry),
        TransactionOffsetCommitEffect::SubmitTxnOffsetCommit {
            epoch,
            operation_id,
            deadline: deadline(),
            group_fence: group_fence(),
        }
    );
    assert_eq!(
        machine.state(),
        TransactionOffsetCommitState::TxnOffsetCommitAdmitted
    );
    assert_eq!(machine.operation_id(), Some(operation_id));
}
