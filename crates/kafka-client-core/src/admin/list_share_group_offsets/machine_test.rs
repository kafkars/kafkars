//! Construction scenarios for the API-90 deterministic owner.

use crate::{Deadline, OperationId};

use super::{ListShareGroupOffsetsMachine, ListShareGroupOffsetsPlan, ListShareGroupOffsetsState};

#[test]
fn construction_retains_original_identity_deadline_and_ready_state() {
    let plan = ListShareGroupOffsetsPlan::all("share-workers".to_owned())
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine =
        ListShareGroupOffsetsMachine::new(OperationId::from_raw(90), Deadline::from_tick(73), plan);

    assert_eq!(machine.state(), ListShareGroupOffsetsState::Ready);
    assert_eq!(machine.operation_id, OperationId::from_raw(90));
    assert_eq!(machine.deadline, Deadline::from_tick(73));
    assert_eq!(machine.plan.group_id(), "share-workers");
}
