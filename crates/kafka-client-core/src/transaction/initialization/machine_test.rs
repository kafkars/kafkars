//! Accepted transaction-initialization owner construction.

use crate::{Deadline, OperationId};

use super::{
    TransactionInitializationMachine, TransactionInitializationPlan,
    TransactionInitializationState, TransactionalOwnerId,
};

#[test]
fn accepted_machine_retains_exact_owner_operation_deadline_and_plan() {
    let owner_id = TransactionalOwnerId::from_raw(7);
    let plan = TransactionInitializationPlan::new(60_000)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = TransactionInitializationMachine::new(
        owner_id,
        OperationId::from_raw(11),
        Deadline::from_tick(19),
        plan,
    );

    assert_eq!(machine.owner_id(), owner_id);
    assert_eq!(machine.operation_id, OperationId::from_raw(11));
    assert_eq!(machine.deadline, Deadline::from_tick(19));
    assert_eq!(machine.plan, plan);
    assert_eq!(machine.state(), TransactionInitializationState::Ready);
}
