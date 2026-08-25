//! Owner loss performs one best-effort abort before releasing the identity.

use kafka_client_core::{TransactionEndMode, TransactionLifecycleState};

use super::{
    host::TransactionLifecycleTurn,
    host_support_test::{FakePort, assert_released, deadline, host},
};

#[test]
fn active_owner_loss_aborts_and_never_publishes_a_success() {
    let (mut lifecycle, active, release, _completion) = host();
    lifecycle
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    lifecycle
        .owner_lost(deadline(40))
        .unwrap_or_else(|error| panic!("owner loss admits cleanup: {error:?}"));
    let mut port = FakePort::succeeding();

    assert_eq!(
        lifecycle.turn_with(&mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    assert_eq!(port.first_mode(), TransactionEndMode::Abort);
    assert_eq!(
        lifecycle.turn_with(&mut port),
        Ok(TransactionLifecycleTurn::Progress)
    );
    assert_eq!(lifecycle.machine.state(), TransactionLifecycleState::Closed);
    assert_released(&active, &release);
}
