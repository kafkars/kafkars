//! Lifecycle construction and accepted-plan ownership scenarios.

use crate::{Deadline, OperationId};

use super::{
    ConfigResourceType, ListConfigResourcesMachine, ListConfigResourcesPlan,
    ListConfigResourcesState,
};

#[test]
fn accepted_machine_starts_ready_and_owns_exact_plan() {
    let plan =
        ListConfigResourcesPlan::new(vec![ConfigResourceType::GROUP, ConfigResourceType::TOPIC])
            .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let machine = ListConfigResourcesMachine::new(
        OperationId::from_raw(74),
        Deadline::from_tick(900),
        plan.clone(),
    );
    assert_eq!(machine.state(), ListConfigResourcesState::Ready);
    assert_eq!(machine.plan(), &plan);
}
