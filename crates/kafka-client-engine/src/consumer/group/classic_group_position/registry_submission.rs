//! One Sync-gated position request handoff into bounded RPC ownership.

use kafka_client_core::Moment;

use crate::driver::{
    DriverOwner, GroupPositionOffsetFetchAdmission, GroupPositionOffsetFetchReturnReason,
};

use super::super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

/// Outcome of attempting at most one prepared position request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn submit_one_classic_group_position(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> Result<ClassicGroupPositionSubmissionTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(position_is_ready) else {
            return Ok(ClassicGroupPositionSubmissionTurn::Idle);
        };
        if expire_ready_position(&mut self.entries[index], now)? {
            return Ok(ClassicGroupPositionSubmissionTurn::Progress);
        }
        let calls = self
            .position_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::PositionCallsUnavailable)?;
        let entry = &mut self.entries[index];
        let (key, request) = entry
            .position
            .begin_handoff()
            .map_err(ClassicGroupExecutionError::Position)?;
        let group = entry
            .position
            .handoff_group()
            .map_err(ClassicGroupExecutionError::Position)?;
        match calls.try_submit_group_position_offset_fetch(driver, key, group, request) {
            GroupPositionOffsetFetchAdmission::Returned(returned) => {
                let (key, request, reason) = returned.into_parts();
                let fence = key.fence();
                if let Err(error) = entry.position.restore_prepared(key, request) {
                    entry.fault = Some(ClassicGroupEntryFault::PositionSubmission { fence, error });
                    return Err(ClassicGroupExecutionError::Position(error));
                }
                match reason {
                    GroupPositionOffsetFetchReturnReason::Capacity { .. } => {
                        Ok(ClassicGroupPositionSubmissionTurn::Blocked)
                    }
                    GroupPositionOffsetFetchReturnReason::DuplicateFence => {
                        entry.fault = Some(ClassicGroupEntryFault::PositionDuplicateFence(fence));
                        Err(ClassicGroupExecutionError::PositionDuplicateFence(fence))
                    }
                }
            }
            GroupPositionOffsetFetchAdmission::Rejected(rejected) => {
                let (key, _source) = rejected.into_parts();
                match entry.position.finish_driver_rejected(key, now) {
                    Ok(()) => Ok(ClassicGroupPositionSubmissionTurn::Progress),
                    Err(failure) => {
                        let error = failure.error();
                        entry.fault = Some(ClassicGroupEntryFault::PositionRejection(failure));
                        Err(ClassicGroupExecutionError::Position(error))
                    }
                }
            }
            GroupPositionOffsetFetchAdmission::Accepted(accepted) => {
                match entry.position.confirm_driver_owned(accepted) {
                    Ok(()) => Ok(ClassicGroupPositionSubmissionTurn::Progress),
                    Err(failure) => {
                        let error = failure.error();
                        entry.fault = Some(ClassicGroupEntryFault::PositionAcceptance(failure));
                        Err(ClassicGroupExecutionError::Position(error))
                    }
                }
            }
        }
    }
}

fn position_is_ready(entry: &GroupConsumerEntry) -> bool {
    entry.is_active() && entry.execution.is_idle() && entry.position.is_prepared()
}

fn expire_ready_position(
    entry: &mut GroupConsumerEntry,
    now: Moment,
) -> Result<bool, ClassicGroupExecutionError> {
    match entry.position.expire_prepared_if_due(now) {
        Ok(expired) => Ok(expired),
        Err(failure) => {
            let error = failure.error();
            entry.fault = Some(ClassicGroupEntryFault::PositionRejection(failure));
            Err(ClassicGroupExecutionError::Position(error))
        }
    }
}
