//! Identity exhaustion, retained facts, and transaction-end barrier scenarios.

use super::test_support::{InputKind, admitted, deadline, effect, epoch, group_fence, input};
use super::{
    TransactionOffsetCommitEffect, TransactionOffsetCommitEndBarrier, TransactionOffsetCommitId,
    TransactionOffsetCommitMachine, TransactionOffsetCommitMachineError,
    TransactionOffsetCommitStage, TransactionOffsetCommitState, TransactionOffsetCommitTerminal,
};

#[test]
fn idle_owner_retains_no_operation_and_allows_transaction_end() {
    let machine = TransactionOffsetCommitMachine::new();

    assert_eq!(machine.state(), TransactionOffsetCommitState::Idle);
    assert_eq!(machine.operation_id(), None);
    assert_eq!(machine.deadline(), None);
    assert_eq!(machine.group_fence(), None);
    assert_eq!(
        machine.preflight_end(epoch(1)),
        Ok(TransactionOffsetCommitEndBarrier::Ready)
    );
}

#[test]
fn every_unsettled_stage_blocks_end_until_exact_terminal_drain() {
    let (mut machine, active, operation_id) = admitted();
    for expected in [
        TransactionOffsetCommitState::AddOffsetsAdmitted,
        TransactionOffsetCommitState::AddOffsetsAwaiting,
        TransactionOffsetCommitState::TxnOffsetCommitAdmitted,
        TransactionOffsetCommitState::TxnOffsetCommitAwaiting,
    ] {
        assert_eq!(machine.state(), expected);
        assert_eq!(
            machine.preflight_end(active),
            Ok(TransactionOffsetCommitEndBarrier::Unsettled {
                operation_id,
                state: expected,
            })
        );
        match expected {
            TransactionOffsetCommitState::AddOffsetsAdmitted => {
                let Ok(_transition) = machine.apply(input(
                    InputKind::Accepted,
                    active,
                    operation_id,
                    TransactionOffsetCommitStage::AddOffsets,
                )) else {
                    panic!("AddOffsets acceptance must advance");
                };
            }
            TransactionOffsetCommitState::AddOffsetsAwaiting => {
                let Ok(_transition) = machine.apply(input(
                    InputKind::Succeeded,
                    active,
                    operation_id,
                    TransactionOffsetCommitStage::AddOffsets,
                )) else {
                    panic!("AddOffsets success must advance");
                };
            }
            TransactionOffsetCommitState::TxnOffsetCommitAdmitted => {
                let Ok(_transition) = machine.apply(input(
                    InputKind::Accepted,
                    active,
                    operation_id,
                    TransactionOffsetCommitStage::TxnOffsetCommit,
                )) else {
                    panic!("TxnOffsetCommit acceptance must advance");
                };
            }
            TransactionOffsetCommitState::TxnOffsetCommitAwaiting => {
                let Ok(_transition) = machine.apply(input(
                    InputKind::Succeeded,
                    active,
                    operation_id,
                    TransactionOffsetCommitStage::TxnOffsetCommit,
                )) else {
                    panic!("TxnOffsetCommit success must complete");
                };
            }
            TransactionOffsetCommitState::Idle => unreachable!(),
        }
    }
    assert_eq!(machine.state(), TransactionOffsetCommitState::Idle);
    assert_eq!(
        machine.preflight_end(active),
        Ok(TransactionOffsetCommitEndBarrier::Ready)
    );
}

#[test]
fn stale_epoch_cannot_probe_around_an_unsettled_end_barrier() {
    let (machine, active, _operation_id) = admitted();
    let before = machine.state();

    assert_eq!(
        machine.preflight_end(epoch(2)),
        Err(TransactionOffsetCommitMachineError::EpochMismatch {
            expected: active,
            supplied: epoch(2),
        })
    );
    assert_eq!(machine.state(), before);
}

#[test]
fn final_representable_identity_is_consumed_and_never_reused() {
    let active = epoch(1);
    let mut machine = TransactionOffsetCommitMachine::new();
    machine.next_operation_id = Some(TransactionOffsetCommitId::from_raw_for_test(u64::MAX));
    if let Err(error) = machine.admit(active, deadline(), group_fence()) {
        panic!("final identity admission: {error}");
    }
    let Some(final_id) = machine.operation_id() else {
        panic!("final identity admission must retain its operation");
    };
    assert_eq!(final_id.get(), u64::MAX);

    let Ok(completed) = machine.apply(input(
        InputKind::Rejected,
        active,
        final_id,
        TransactionOffsetCommitStage::AddOffsets,
    )) else {
        panic!("final identity rejection must complete");
    };
    assert_eq!(
        effect(completed),
        TransactionOffsetCommitEffect::Complete {
            epoch: active,
            operation_id: final_id,
            deadline: deadline(),
            group_fence: group_fence(),
            terminal: TransactionOffsetCommitTerminal::RejectedNotSent {
                stage: TransactionOffsetCommitStage::AddOffsets,
            },
        }
    );
    assert_eq!(
        machine.admit(active, deadline(), group_fence()),
        Err(TransactionOffsetCommitMachineError::IdentityExhausted)
    );
    assert_eq!(machine.state(), TransactionOffsetCommitState::Idle);
}
