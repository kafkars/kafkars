//! Accepted voter-removal lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{RemoveRaftVoterMachine, RemoveRaftVoterPlan, RemoveRaftVoterState};

#[test]
fn accepted_machine_begins_ready_with_validated_plan() {
    let machine = RemoveRaftVoterMachine::new(
        OperationId::from_raw(81),
        Deadline::from_tick(100),
        RemoveRaftVoterPlan::new(None, 7, [9; 16]).unwrap_or_else(|error| panic!("plan: {error}")),
    );

    assert_eq!(machine.state(), RemoveRaftVoterState::Ready);
}
