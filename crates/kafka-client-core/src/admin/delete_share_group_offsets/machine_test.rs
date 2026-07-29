//! Construction scenarios for the deterministic API-92 owner.

use crate::{Deadline, OperationId};

use super::{
    DeleteShareGroupOffsetsMachine, DeleteShareGroupOffsetsPlan, DeleteShareGroupOffsetsState,
};

#[test]
fn accepted_machine_begins_ready_without_starting_hidden_work() {
    let plan = DeleteShareGroupOffsetsPlan::new("share".to_owned(), vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = DeleteShareGroupOffsetsMachine::new(
        OperationId::from_raw(92),
        Deadline::from_tick(20),
        plan,
    );

    assert_eq!(machine.state(), DeleteShareGroupOffsetsState::Ready);
}
