//! Construction scenarios for the transaction-description deterministic owner.

use crate::{Deadline, OperationId};

use super::{
    AdminDescribeTransactionsMachine, AdminDescribeTransactionsPlan, AdminDescribeTransactionsState,
};

#[test]
fn accepted_owner_starts_ready_on_the_first_caller_identity() {
    let machine = AdminDescribeTransactionsMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(20),
        AdminDescribeTransactionsPlan::new(vec![
            "invoice-worker".to_owned(),
            "audit-writer".to_owned(),
        ])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), AdminDescribeTransactionsState::Ready);
    assert_eq!(
        machine
            .current_transactional_id()
            .unwrap_or_else(|| panic!("first ID")),
        "invoice-worker"
    );
}
