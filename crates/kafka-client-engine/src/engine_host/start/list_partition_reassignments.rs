//! Focused construction of the synchronized partition-reassignment listing owner.

use std::sync::Arc;

use crate::{
    admin::{
        ListPartitionReassignmentsAdmissionPort, ListPartitionReassignmentsHost,
        ListPartitionReassignmentsPublisher, ListPartitionReassignmentsShardOwner,
    },
    driver::ReactorWake,
};

pub(super) struct StartedListPartitionReassignments {
    pub(super) owner: ListPartitionReassignmentsShardOwner,
    pub(super) admission: ListPartitionReassignmentsAdmissionPort,
}

pub(super) fn start(
    publisher: ListPartitionReassignmentsPublisher,
    wake: ReactorWake,
) -> StartedListPartitionReassignments {
    let owner = ListPartitionReassignmentsShardOwner::new(
        ListPartitionReassignmentsHost::new(publisher),
        Arc::new(wake),
    );
    let admission = owner.admission_port();
    StartedListPartitionReassignments { owner, admission }
}
