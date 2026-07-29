//! Scenarios for singular and batched group-offset lifecycle construction.

use crate::{Deadline, OperationId};

use super::{
    ListConsumerGroupOffsetsMachine, ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsState,
};

#[test]
fn accepted_machine_begins_ready_with_reserved_identity() {
    let plan = ListConsumerGroupOffsetsPlan::new("payments".to_owned(), false)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = ListConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(19),
        Deadline::from_tick(44),
        plan,
    );

    assert_eq!(machine.state(), ListConsumerGroupOffsetsState::Ready);
}
