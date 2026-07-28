//! Lifecycle scenarios for deterministic Admin `DeleteConsumerGroups` ownership.

use crate::{Deadline, OperationId};

use super::{
    DeleteConsumerGroupsMachine, DeleteConsumerGroupsPlan, DeleteConsumerGroupsState,
    DeleteConsumerGroupsTarget,
};

#[test]
fn accepted_machine_begins_ready_with_first_group_visible() {
    let machine = DeleteConsumerGroupsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(99),
        DeleteConsumerGroupsPlan::new(vec![DeleteConsumerGroupsTarget::new(
            "orders-workers".to_owned(),
        )])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), DeleteConsumerGroupsState::Ready);
    assert_eq!(
        machine
            .current_target()
            .map(DeleteConsumerGroupsTarget::group_id),
        Some("orders-workers")
    );
}
