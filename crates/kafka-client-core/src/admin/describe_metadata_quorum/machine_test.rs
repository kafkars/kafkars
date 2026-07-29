//! Accepted metadata-quorum description lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{DescribeMetadataQuorumMachine, DescribeMetadataQuorumState};

#[test]
fn accepted_machine_begins_ready() {
    let machine =
        DescribeMetadataQuorumMachine::new(OperationId::from_raw(55), Deadline::from_tick(100));

    assert_eq!(machine.state(), DescribeMetadataQuorumState::Ready);
}
