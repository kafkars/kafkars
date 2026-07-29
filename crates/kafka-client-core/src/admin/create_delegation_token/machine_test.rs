//! Accepted token-creation ownership scenarios.

use crate::{Deadline, OperationId};

use super::{CreateDelegationTokenMachine, CreateDelegationTokenPlan, CreateDelegationTokenState};

#[test]
fn capacity_reserved_machine_begins_ready() {
    let plan = CreateDelegationTokenPlan::new(None, Vec::new(), None)
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let machine = CreateDelegationTokenMachine::new(
        OperationId::from_raw(38),
        Deadline::from_tick(100),
        plan,
    );

    assert_eq!(machine.state(), CreateDelegationTokenState::Ready);
}
