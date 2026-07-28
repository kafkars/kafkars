//! Lifecycle scenarios for accepted `DescribeLogDirs` ownership.

use crate::{Deadline, OperationId};

use super::{AdminDescribeLogDirsMachine, AdminDescribeLogDirsPlan, AdminDescribeLogDirsState};

#[test]
fn accepted_machine_begins_ready_with_first_broker_visible() {
    let machine = AdminDescribeLogDirsMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(99),
        AdminDescribeLogDirsPlan::new(vec![7, 2])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), AdminDescribeLogDirsState::Ready);
    assert_eq!(machine.current_broker(), Some(7));
}
