//! Fair host turns for leader-routed Admin `DeleteRecords` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{DeleteRecordsShardLockError, DeleteRecordsTurn},
    driver::DeleteRecordsCall,
    protocol::admin::delete_records::remaining_timeout_ms,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DeleteRecordsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DeleteRecordsProgress, EngineHostError> {
    let mut host = match resources.delete_records.try_host() {
        Ok(host) => host,
        Err(DeleteRecordsShardLockError::Contended) => {
            return Ok(DeleteRecordsProgress::contended());
        }
        Err(DeleteRecordsShardLockError::Poisoned) => {
            return Err(EngineHostError::DeleteRecordsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.delete_records.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DeleteRecords)?;
    let driver_progress = match turn {
        DeleteRecordsTurn::Idle => false,
        DeleteRecordsTurn::Progress => true,
        DeleteRecordsTurn::Submit(submission) => {
            let (operation_id, deadline, target) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            let timeout_ms = remaining_timeout_ms(now, deadline.core()).ok();
            match timeout_ms.and_then(|timeout_ms| {
                DeleteRecordsCall::submit(driver, &target, timeout_ms, deadline.transport()).ok()
            }) {
                Some(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DeleteRecords)?,
                None => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DeleteRecords)?,
            }
            true
        }
    };
    Ok(DeleteRecordsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DeleteRecordsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
