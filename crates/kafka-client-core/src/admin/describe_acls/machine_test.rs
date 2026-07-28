//! Accepted ACL-description lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{DescribeAclsFilter, DescribeAclsMachine, DescribeAclsPlan, DescribeAclsState};

#[test]
fn accepted_machine_begins_ready() {
    let machine = DescribeAclsMachine::new(
        OperationId::from_raw(37),
        Deadline::from_tick(100),
        DescribeAclsPlan::new(DescribeAclsFilter::new(1, None, 1, None, None, 1, 1))
            .unwrap_or_else(|error| panic!("valid filter: {error}")),
    );

    assert_eq!(machine.state(), DescribeAclsState::Ready);
}
