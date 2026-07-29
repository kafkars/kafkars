//! Accepted voter-addition lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{AddRaftVoterEndpoint, AddRaftVoterMachine, AddRaftVoterPlan, AddRaftVoterState};

#[test]
fn accepted_machine_begins_ready_with_validated_plan() {
    let machine = AddRaftVoterMachine::new(
        OperationId::from_raw(80),
        Deadline::from_tick(100),
        AddRaftVoterPlan::new(
            None,
            7,
            [9; 16],
            vec![AddRaftVoterEndpoint::new(
                "CONTROLLER".to_owned(),
                "node-a".to_owned(),
                9093,
            )],
        )
        .unwrap_or_else(|error| panic!("plan: {error}")),
    );

    assert_eq!(machine.state(), AddRaftVoterState::Ready);
}
