//! Construction scenarios for the consumer-group offset deletion owner.

use crate::{Deadline, OperationId};

use super::{
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsMachine,
    DeleteConsumerGroupOffsetsPlan, DeleteConsumerGroupOffsetsState,
};

#[test]
fn accepted_machine_begins_ready_without_starting_hidden_work() {
    let plan = DeleteConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![DeleteConsumerGroupOffsetTarget::new("orders".to_owned(), 0)],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = DeleteConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(17),
        Deadline::from_tick(20),
        plan,
    );

    assert_eq!(machine.state(), DeleteConsumerGroupOffsetsState::Ready);
}
