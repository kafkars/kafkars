//! Construction scenarios for the active-producer deterministic owner.

use crate::{Deadline, OperationId};

use super::{
    AdminDescribeProducerTarget, AdminDescribeProducersMachine, AdminDescribeProducersPlan,
    AdminDescribeProducersState,
};

#[test]
fn accepted_owner_starts_ready_on_the_first_caller_target() {
    let machine = AdminDescribeProducersMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(20),
        AdminDescribeProducersPlan::new(
            vec![
                AdminDescribeProducerTarget::new("orders".to_owned(), 2),
                AdminDescribeProducerTarget::new("audit".to_owned(), 0),
            ],
            None,
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), AdminDescribeProducersState::Ready);
    let current = machine
        .current_target()
        .unwrap_or_else(|| panic!("first target"));
    assert_eq!(current.topic(), "orders");
    assert_eq!(current.partition(), 2);
}
