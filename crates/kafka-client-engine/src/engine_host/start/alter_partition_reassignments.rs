//! Focused construction of the synchronized partition-reassignment alteration owner.

use std::sync::Arc;

use crate::{
    admin::{
        AlterPartitionReassignmentsAdmissionPort, AlterPartitionReassignmentsHost,
        AlterPartitionReassignmentsPublisher, AlterPartitionReassignmentsShardOwner,
    },
    driver::ReactorWake,
};

pub(super) struct StartedAlterPartitionReassignments {
    pub(super) owner: AlterPartitionReassignmentsShardOwner,
    pub(super) admission: AlterPartitionReassignmentsAdmissionPort,
}

pub(super) fn start(
    publisher: AlterPartitionReassignmentsPublisher,
    wake: ReactorWake,
) -> StartedAlterPartitionReassignments {
    let owner = AlterPartitionReassignmentsShardOwner::new(
        AlterPartitionReassignmentsHost::new(publisher),
        Arc::new(wake),
    );
    let admission = owner.admission_port();
    StartedAlterPartitionReassignments { owner, admission }
}
