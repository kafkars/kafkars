//! Accepted partition-transaction abort lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{
    AbortPartitionTransactionMachine, AbortPartitionTransactionPlan, AbortPartitionTransactionState,
};

#[test]
fn accepted_machine_begins_ready_with_validated_plan() {
    let machine = AbortPartitionTransactionMachine::new(
        OperationId::from_raw(27),
        Deadline::from_tick(100),
        AbortPartitionTransactionPlan::new("orders".to_owned(), 2, 91, 7, 11)
            .unwrap_or_else(|error| panic!("plan: {error}")),
    );

    assert_eq!(machine.state(), AbortPartitionTransactionState::Ready);
}
