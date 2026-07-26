//! Construction scenarios for the consumer-group offset alteration owner.

use crate::{Deadline, OperationId};

use super::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsMachine,
    AlterConsumerGroupOffsetsPlan, AlterConsumerGroupOffsetsState,
};

#[test]
fn accepted_machine_begins_ready_without_starting_hidden_work() {
    let plan = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            0,
            17,
            None,
            None,
        )],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = AlterConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(17),
        Deadline::from_tick(20),
        plan,
    );

    assert_eq!(machine.state(), AlterConsumerGroupOffsetsState::Ready);
}
