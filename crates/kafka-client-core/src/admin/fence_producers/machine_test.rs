//! Construction scenarios for the producer-fencing deterministic owner.

use crate::{Deadline, OperationId};

use super::{AdminFenceProducersMachine, AdminFenceProducersPlan, AdminFenceProducersState};

#[test]
fn accepted_owner_starts_ready_on_the_first_caller_identity() {
    let machine = AdminFenceProducersMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(20),
        AdminFenceProducersPlan::new(vec!["invoice-worker".to_owned(), "audit-writer".to_owned()])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), AdminFenceProducersState::Ready);
    assert_eq!(
        machine
            .current_transactional_id()
            .unwrap_or_else(|| panic!("first ID")),
        "invoice-worker"
    );
}
