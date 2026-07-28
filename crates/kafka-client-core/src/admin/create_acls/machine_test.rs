//! Accepted ACL-creation lifecycle ownership tests.

use crate::{Deadline, OperationId};

use super::{CreateAclBinding, CreateAclsMachine, CreateAclsPlan, CreateAclsState};

#[test]
fn accepted_machine_begins_ready_with_reserved_plan_visible() {
    let machine = CreateAclsMachine::new(
        OperationId::from_raw(41),
        Deadline::from_tick(100),
        CreateAclsPlan::new(vec![binding("orders")])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), CreateAclsState::Ready);
    assert_eq!(
        machine
            .plan()
            .and_then(|plan| plan.bindings().first())
            .map(CreateAclBinding::resource_name),
        Some("orders")
    );
}

fn binding(name: &str) -> CreateAclBinding {
    CreateAclBinding::new(
        2,
        name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
    )
}
