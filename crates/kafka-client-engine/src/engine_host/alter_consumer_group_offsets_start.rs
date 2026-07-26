//! Concrete startup wiring for consumer-group offset alteration ownership.

use std::sync::Arc;

use crate::{
    admin::{
        AlterConsumerGroupOffsetsAdmissionPort, AlterConsumerGroupOffsetsHost,
        AlterConsumerGroupOffsetsPublisher, AlterConsumerGroupOffsetsShardOwner,
    },
    driver::ReactorWake,
};

pub(super) struct StartedAlterConsumerGroupOffsets {
    pub(super) owner: AlterConsumerGroupOffsetsShardOwner,
    pub(super) admission: AlterConsumerGroupOffsetsAdmissionPort,
}

pub(super) fn start(
    publisher: AlterConsumerGroupOffsetsPublisher,
    wake: ReactorWake,
) -> StartedAlterConsumerGroupOffsets {
    let host = AlterConsumerGroupOffsetsHost::new(publisher);
    let owner = AlterConsumerGroupOffsetsShardOwner::new(host, Arc::new(wake));
    let admission = owner.admission_port();
    StartedAlterConsumerGroupOffsets { owner, admission }
}
