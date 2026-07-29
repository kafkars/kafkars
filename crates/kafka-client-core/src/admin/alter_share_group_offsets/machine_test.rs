//! Scenarios for API-91 machine construction and inert ownership.

use crate::{Deadline, OperationId};

use super::{
    AlterShareGroupOffset, AlterShareGroupOffsetsMachine, AlterShareGroupOffsetsPlan,
    AlterShareGroupOffsetsState,
};

#[test]
fn construction_retains_reserved_identity_deadline_and_ready_state() {
    let machine = AlterShareGroupOffsetsMachine::new(
        OperationId::from_raw(91),
        Deadline::from_tick(100),
        AlterShareGroupOffsetsPlan::new(
            "share-workers".to_owned(),
            vec![AlterShareGroupOffset::new("orders".to_owned(), 0, 42)],
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.operation_id, OperationId::from_raw(91));
    assert_eq!(machine.deadline, Deadline::from_tick(100));
    assert_eq!(machine.plan.group_id(), "share-workers");
    assert_eq!(machine.state(), AlterShareGroupOffsetsState::Ready);
}
