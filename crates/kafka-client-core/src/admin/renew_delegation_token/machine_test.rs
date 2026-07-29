//! Accepted token-renewal ownership scenarios.

use crate::{Deadline, OperationId};

use super::{
    RenewDelegationTokenHmac, RenewDelegationTokenMachine, RenewDelegationTokenPlan,
    RenewDelegationTokenState,
};

#[test]
fn capacity_reserved_machine_begins_ready_with_unique_secret_ownership() {
    let machine = RenewDelegationTokenMachine::new(
        OperationId::from_raw(39),
        Deadline::from_tick(100),
        plan(),
    );

    assert_eq!(machine.state(), RenewDelegationTokenState::Ready);
    let debug = format!("{machine:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("[1, 2, 3, 4]"));
}

fn plan() -> RenewDelegationTokenPlan {
    RenewDelegationTokenPlan::new(
        RenewDelegationTokenHmac::new(vec![1, 2, 3, 4])
            .unwrap_or_else(|error| panic!("hmac: {error}")),
        None,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
