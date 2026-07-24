//! Completion identity fencing and abnormal publication selection.

use crate::{
    completion::CompletionId,
    consumer::{
        assigned_close_error::{AssignedCloseSlotError, AssignedCloseSlotPhase},
        assigned_host::AssignedConsumerCloseTerminal,
    },
};

use super::AssignedCloseSlot;

#[test]
fn reserved_close_recovers_as_execution_unavailable_with_exact_identity() {
    let completion_id = CompletionId::from_parts_for_test(0, 4);
    let foreign = CompletionId::from_parts_for_test(0, 5);
    let mut close = AssignedCloseSlot::create_for_assigned_owner();
    close
        .reserve(completion_id)
        .unwrap_or_else(|error| panic!("reserve: {error:?}"));

    assert_eq!(
        close.recovery_terminal(),
        Some((
            completion_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable
        ))
    );
    assert_eq!(
        close.mark_published(foreign),
        Err(AssignedCloseSlotError::MismatchedCompletionId {
            active: completion_id,
            supplied: foreign,
        })
    );
    close
        .mark_published(completion_id)
        .unwrap_or_else(|error| panic!("publish exact completion: {error:?}"));
    assert_eq!(close.phase(), AssignedCloseSlotPhase::Published);
}
