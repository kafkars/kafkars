//! Single-effect transition ownership evidence.

use crate::{
    DeliveryStatus, GroupOffsetCommitEffect, GroupOffsetCommitFailure,
    GroupOffsetCommitFailureKind, GroupOffsetCommitTerminal, GroupOffsetCommitTransition,
    OperationId,
};

#[test]
fn transition_owns_zero_or_one_effect_without_an_effect_list() {
    assert_eq!(GroupOffsetCommitTransition::none().into_effect(), None);

    let effect = GroupOffsetCommitEffect::Complete {
        operation_id: OperationId::from_raw(9),
        terminal: GroupOffsetCommitTerminal::Failed(GroupOffsetCommitFailure::new(
            GroupOffsetCommitFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        )),
    };
    let Some(GroupOffsetCommitEffect::Complete {
        operation_id,
        terminal: GroupOffsetCommitTerminal::Failed(failure),
    }) = GroupOffsetCommitTransition::one(effect).into_effect()
    else {
        panic!("one complete effect");
    };
    assert_eq!(operation_id, OperationId::from_raw(9));
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
