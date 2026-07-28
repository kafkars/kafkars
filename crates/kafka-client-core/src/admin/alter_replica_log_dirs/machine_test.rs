//! Lifecycle scenarios for accepted alteration ownership.

use crate::{Deadline, OperationId};

use super::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirsMachine, AlterReplicaLogDirsPlan,
    AlterReplicaLogDirsState,
};

#[test]
fn accepted_machine_begins_ready_with_first_broker_visible() {
    let machine = AlterReplicaLogDirsMachine::new(
        OperationId::from_raw(29),
        Deadline::from_tick(100),
        AlterReplicaLogDirsPlan::new(vec![AlterReplicaLogDirAssignment::new(
            7,
            "orders".to_owned(),
            2,
            "/data".to_owned(),
        )])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), AlterReplicaLogDirsState::Ready);
    assert_eq!(machine.current_broker(), Some(7));
}
