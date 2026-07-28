//! Accepted ACL-deletion lifecycle ownership tests.

use crate::{Deadline, OperationId};

use super::{DeleteAclsFilter, DeleteAclsMachine, DeleteAclsPlan, DeleteAclsState};

#[test]
fn accepted_machine_begins_ready_with_duplicate_positions_visible() {
    let filter = DeleteAclsFilter::new(1, None, 1, None, None, 1, 1);
    let machine = DeleteAclsMachine::new(
        OperationId::from_raw(43),
        Deadline::from_tick(100),
        DeleteAclsPlan::new(vec![filter.clone(), filter])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), DeleteAclsState::Ready);
    assert_eq!(
        machine
            .plan()
            .map(DeleteAclsPlan::required_filter_result_capacity),
        Some(2)
    );
}
