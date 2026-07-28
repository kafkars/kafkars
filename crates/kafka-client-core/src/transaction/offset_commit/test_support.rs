//! Deterministic scalar fixtures for transactional offset-transfer tests.

use crate::{
    AssignmentGeneration, Deadline, GroupId, GroupPositionFence, MemberId, MembershipCycle,
    OperationId, TransactionEndOutcome, TransactionEpoch, TransactionLifecycleEffect,
    TransactionLifecycleInput, TransactionLifecycleMachine, TransactionalOwnerId,
};

use super::{
    TransactionOffsetCommitEffect, TransactionOffsetCommitId, TransactionOffsetCommitInput,
    TransactionOffsetCommitMachine, TransactionOffsetCommitStage,
};

pub(super) fn epoch(index: u64) -> TransactionEpoch {
    assert!(index != 0);
    let owner = TransactionalOwnerId::from_raw(31);
    let mut lifecycle = TransactionLifecycleMachine::new(owner);
    for current in 1..=index {
        let began = lifecycle
            .apply(owner, TransactionLifecycleInput::Begin)
            .unwrap_or_else(|error| panic!("begin epoch {current}: {error}"))
            .into_effect()
            .unwrap_or_else(|| panic!("begin effect"));
        let TransactionLifecycleEffect::Began { epoch, .. } = began else {
            panic!("begin must expose epoch");
        };
        if current == index {
            return epoch;
        }
        lifecycle
            .apply(
                owner,
                TransactionLifecycleInput::Commit {
                    epoch,
                    operation_id: OperationId::from_raw(current),
                },
            )
            .unwrap_or_else(|error| panic!("commit epoch {current}: {error}"));
        lifecycle
            .apply(
                owner,
                TransactionLifecycleInput::EndSettled {
                    epoch,
                    outcome: TransactionEndOutcome::Succeeded,
                },
            )
            .unwrap_or_else(|error| panic!("settle epoch {current}: {error}"));
    }
    unreachable!("positive epoch index returns from loop")
}

pub(super) const fn deadline() -> Deadline {
    Deadline::from_tick(900)
}

pub(super) fn group_fence() -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(7).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("cycle")),
        MemberId::try_from_raw(13).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(17).unwrap_or_else(|| panic!("assignment")),
    )
}

pub(super) fn admitted() -> (
    TransactionOffsetCommitMachine,
    TransactionEpoch,
    TransactionOffsetCommitId,
) {
    let epoch = epoch(1);
    let mut machine = TransactionOffsetCommitMachine::new();
    machine
        .admit(epoch, deadline(), group_fence())
        .unwrap_or_else(|error| panic!("admit: {error}"));
    let operation_id = machine
        .operation_id()
        .unwrap_or_else(|| panic!("admitted operation"));
    (machine, epoch, operation_id)
}

pub(super) fn input(
    kind: InputKind,
    epoch: TransactionEpoch,
    operation_id: TransactionOffsetCommitId,
    stage: TransactionOffsetCommitStage,
) -> TransactionOffsetCommitInput {
    match kind {
        InputKind::Accepted => TransactionOffsetCommitInput::DriverAccepted {
            epoch,
            operation_id,
            stage,
        },
        InputKind::Rejected => TransactionOffsetCommitInput::DriverRejected {
            epoch,
            operation_id,
            stage,
        },
        InputKind::Succeeded => TransactionOffsetCommitInput::Succeeded {
            epoch,
            operation_id,
            stage,
        },
    }
}

pub(super) fn effect(
    transition: super::TransactionOffsetCommitTransition,
) -> TransactionOffsetCommitEffect {
    transition
        .into_effect()
        .unwrap_or_else(|| panic!("transition effect"))
}

#[derive(Clone, Copy)]
pub(super) enum InputKind {
    Accepted,
    Rejected,
    Succeeded,
}
