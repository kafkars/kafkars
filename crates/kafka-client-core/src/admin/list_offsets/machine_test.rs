//! Construction scenarios for the Admin `ListOffsets` owner.

use crate::{Deadline, OperationId};

use super::{
    AdminListOffsetSpec, AdminListOffsetTarget, AdminListOffsetsMachine, AdminListOffsetsPlan,
    AdminListOffsetsState,
};

#[test]
fn accepted_machine_begins_ready_without_starting_hidden_work() {
    let machine =
        AdminListOffsetsMachine::new(OperationId::from_raw(17), Deadline::from_tick(20), plan());

    assert_eq!(machine.state(), AdminListOffsetsState::Ready);
}

fn plan() -> AdminListOffsetsPlan {
    AdminListOffsetsPlan::new(vec![AdminListOffsetTarget::new(
        "orders".to_owned(),
        0,
        AdminListOffsetSpec::Latest,
    )])
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
