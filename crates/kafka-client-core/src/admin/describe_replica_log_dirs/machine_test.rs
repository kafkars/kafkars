//! Lifecycle scenarios for accepted replica placement ownership.

use crate::{Deadline, OperationId};

use super::{
    DescribeReplicaLogDirsMachine, DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsReplica,
    DescribeReplicaLogDirsState,
};

#[test]
fn accepted_machine_begins_ready_with_first_occurrence_broker_visible() {
    let machine = DescribeReplicaLogDirsMachine::new(
        OperationId::from_raw(35),
        Deadline::from_tick(99),
        DescribeReplicaLogDirsPlan::new(vec![
            replica("orders", 0, 8),
            replica("audit", 0, 3),
            replica("orders", 1, 8),
        ])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), DescribeReplicaLogDirsState::Ready);
    assert_eq!(machine.current_broker(), Some(8));
}

fn replica(topic: &str, partition: i32, broker_id: i32) -> DescribeReplicaLogDirsReplica {
    DescribeReplicaLogDirsReplica::new(topic.to_owned(), partition, broker_id)
}
