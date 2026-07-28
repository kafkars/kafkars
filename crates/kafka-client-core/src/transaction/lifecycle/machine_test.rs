//! Linear transaction lifecycle owner shape evidence.

use super::{TransactionLifecycleMachine, TransactionLifecycleState};
use crate::TransactionalOwnerId;

#[test]
fn new_machine_retains_one_idle_engine_owner() {
    let owner = TransactionalOwnerId::from_raw(7);
    let machine = TransactionLifecycleMachine::new(owner);

    assert_eq!(machine.owner_id(), owner);
    assert_eq!(machine.state(), TransactionLifecycleState::Idle);
    assert_eq!(machine.active_epoch(), None);
    assert_eq!(machine.outstanding_send_count(), 0);
}
