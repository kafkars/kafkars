//! Fair host turns for leader-routed Admin `ListOffsets` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{AdminListOffsetsShardLockError, AdminListOffsetsTurn},
    driver::AdminListOffsetsCall,
    protocol::admin::list_offsets::remaining_timeout_ms,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AdminListOffsetsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminListOffsetsProgress, EngineHostError> {
    let mut host = match resources.list_offsets.try_host() {
        Ok(host) => host,
        Err(AdminListOffsetsShardLockError::Contended) => {
            return Ok(AdminListOffsetsProgress::contended());
        }
        Err(AdminListOffsetsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminListOffsetsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.list_offsets.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::AdminListOffsets)?;
    let driver_progress = match turn {
        AdminListOffsetsTurn::Idle => false,
        AdminListOffsetsTurn::Progress => true,
        AdminListOffsetsTurn::Submit(submission) => {
            let (operation_id, deadline, target, read_isolation) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match remaining_timeout_ms(now, deadline.core()) {
                Ok(timeout_ms) => match AdminListOffsetsCall::submit(
                    driver,
                    target,
                    read_isolation,
                    timeout_ms,
                    deadline.transport(),
                ) {
                    Ok(call) => host
                        .accept_call(operation_id, call)
                        .map_err(EngineHostError::AdminListOffsets)?,
                    Err(rejection) => {
                        let (target, read_isolation) = rejection.into_correlation();
                        host.reject_handoff(operation_id, target, read_isolation)
                            .map_err(EngineHostError::AdminListOffsets)?;
                    }
                },
                Err(_elapsed) => host
                    .reject_handoff(operation_id, target, read_isolation)
                    .map_err(EngineHostError::AdminListOffsets)?,
            }
            true
        }
    };
    Ok(AdminListOffsetsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AdminListOffsetsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
