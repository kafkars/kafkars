//! Construction scenarios for the consumer-group member-removal owner.

use crate::{Deadline, OperationId};

use super::{
    ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersMachine, RemoveConsumerGroupMembersPlan,
    RemoveConsumerGroupMembersState,
};

#[test]
fn accepted_machine_begins_ready_without_starting_hidden_work() {
    let plan = RemoveConsumerGroupMembersPlan::new(
        "payments".to_owned(),
        vec![ConsumerGroupMemberRemoval::new("instance-a".to_owned())],
        None,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = RemoveConsumerGroupMembersMachine::new(
        OperationId::from_raw(17),
        Deadline::from_tick(20),
        plan,
    );

    assert_eq!(machine.state(), RemoveConsumerGroupMembersState::Ready);
}
