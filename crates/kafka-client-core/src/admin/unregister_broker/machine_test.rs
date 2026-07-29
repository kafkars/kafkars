//! Accepted broker-unregistration lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{UnregisterBrokerMachine, UnregisterBrokerPlan, UnregisterBrokerState};

#[test]
fn accepted_machine_begins_ready_with_validated_plan() {
    let plan = UnregisterBrokerPlan::new(7).unwrap_or_else(|error| panic!("plan: {error}"));
    let machine =
        UnregisterBrokerMachine::new(OperationId::from_raw(64), Deadline::from_tick(100), plan);

    assert_eq!(machine.state(), UnregisterBrokerState::Ready);
}
