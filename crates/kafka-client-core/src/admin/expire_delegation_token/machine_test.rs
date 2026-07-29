//! Accepted token-expiration ownership scenarios.

use crate::{Deadline, OperationId};

use super::{
    ExpireDelegationTokenHmac, ExpireDelegationTokenMachine, ExpireDelegationTokenPlan,
    ExpireDelegationTokenState,
};

#[test]
fn capacity_reserved_machine_begins_ready_with_unique_secret_ownership() {
    let machine = ExpireDelegationTokenMachine::new(
        OperationId::from_raw(40),
        Deadline::from_tick(100),
        plan(),
    );

    assert_eq!(machine.state(), ExpireDelegationTokenState::Ready);
    let debug = format!("{machine:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("[1, 2, 3, 4]"));
}

fn plan() -> ExpireDelegationTokenPlan {
    ExpireDelegationTokenPlan::new(
        ExpireDelegationTokenHmac::new(vec![1, 2, 3, 4])
            .unwrap_or_else(|error| panic!("hmac: {error}")),
        None,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
