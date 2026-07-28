//! Focused construction of the synchronized Admin `ListOffsets` owner.

use std::sync::Arc;

use crate::{
    admin::{
        AdminListOffsetsAdmissionPort, AdminListOffsetsHost, AdminListOffsetsPublisher,
        AdminListOffsetsShardOwner,
    },
    driver::ReactorWake,
};

pub(super) struct StartedAdminListOffsets {
    pub(super) owner: AdminListOffsetsShardOwner,
    pub(super) admission: AdminListOffsetsAdmissionPort,
}

pub(super) fn start(
    publisher: AdminListOffsetsPublisher,
    wake: ReactorWake,
) -> StartedAdminListOffsets {
    let owner =
        AdminListOffsetsShardOwner::new(AdminListOffsetsHost::new(publisher), Arc::new(wake));
    let admission = owner.admission_port();
    StartedAdminListOffsets { owner, admission }
}
