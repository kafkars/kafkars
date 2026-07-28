//! Offset-transfer health consequence integration scenarios.

use super::{
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionLifecycleState,
};
use crate::{TransactionOffsetCommitConsequence, TransactionalOwnerId};

#[test]
fn offset_commit_abort_required_and_fatal_are_lifecycle_authoritative() {
    let owner = TransactionalOwnerId::from_raw(7);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let epoch = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error}"))
        .into_effect()
        .and_then(|effect| match effect {
            TransactionLifecycleEffect::Began { epoch, .. } => Some(epoch),
            _ => None,
        })
        .unwrap_or_else(|| panic!("begin epoch"));

    let transition = machine
        .apply(
            owner,
            TransactionLifecycleInput::OffsetCommitSettled {
                epoch,
                consequence: TransactionOffsetCommitConsequence::AbortRequired,
            },
        )
        .unwrap_or_else(|error| panic!("abort-required: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(TransactionLifecycleEffect::AbortRequired { .. })
    ));
    assert_eq!(machine.state(), TransactionLifecycleState::AbortRequired);

    let transition = machine
        .apply(
            owner,
            TransactionLifecycleInput::OffsetCommitSettled {
                epoch,
                consequence: TransactionOffsetCommitConsequence::Fatal,
            },
        )
        .unwrap_or_else(|error| panic!("fatal: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(TransactionLifecycleEffect::EnterFatal { .. })
    ));
    assert_eq!(machine.state(), TransactionLifecycleState::Fatal);
}
