//! Completion capacity precedes deterministic close admission.

use crate::consumer::{assigned_close_error::AssignedCloseSlotPhase, assigned_owner_test::owner};

#[test]
fn accepted_close_binds_one_completion_before_effect_interpretation() {
    let mut owner = owner(1);

    let observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));

    assert_eq!(owner.close.phase(), AssignedCloseSlotPhase::Reserved);
    assert_eq!(owner.close_completions.unsettled_len(), 1);
    drop(observer);
    assert_eq!(owner.close_completions.unsettled_len(), 1);
}
